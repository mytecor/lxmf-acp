package bridge

import (
	"encoding/hex"
	"errors"
	"log"
	"strings"
	"sync"

	"github.com/mytecor/lxmf-acp/internal/lxmf"
)

const emptyReplyText = "(Empty response from agent)"

// outPart is one LXMF message queued for sequential delivery.
type outPart struct {
	title   string
	content string
	final   bool
	status  lxmf.Status
	notice  bool
	fields  map[any]any
}

// replyStream turns a streamed ACP reply into a chain of LXMF messages.
//
// Text is segmented incrementally; finished parts are queued in RAM and
// delivered by a single sender goroutine, so there is at most one
// unconfirmed LXMF message per chat. Each part is sent only after the
// previous one reached DELIVERED, and its FIELD_CONTINUATION references the
// hash of that confirmed message. A delivery failure stops the chain.
//
// There is deliberately no backpressure towards ACP: Write never blocks.
type replyStream struct {
	client     LXMFClient
	localHash  []byte
	peerHash   []byte
	chatKey    string
	title      string
	replyTo    []byte
	replyQuote string
	seg        *Segmenter

	mu         sync.Mutex
	cond       *sync.Cond
	queue      []outPart
	finished   bool
	enqueued   int
	hasNotices bool
	err        error

	done chan struct{}
}

func newReplyStream(client LXMFClient, localHash, peerHash []byte, title string, replyTo []byte, replyQuote string, seg *Segmenter) *replyStream {
	chatKey := hex.EncodeToString(localHash) + ":" + hex.EncodeToString(peerHash)
	s := &replyStream{
		client:     client,
		localHash:  append([]byte(nil), localHash...),
		peerHash:   append([]byte(nil), peerHash...),
		chatKey:    chatKey,
		title:      title,
		replyTo:    append([]byte(nil), replyTo...),
		replyQuote: replyQuote,
		seg:        seg,
		done:       make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	go s.run()
	return s
}

// Write accepts an agent_message_chunk text delta.
func (s *replyStream) Write(chunk string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return
	}
	for _, p := range s.seg.Write(chunk) {
		s.enqueueLocked(outPart{title: s.title, content: p})
	}
}

// Notice queues an independent tool message on the same delivery worker.
// Flush preceding text so tool activity cannot overtake buffered commentary.
// Notices never participate in the answer's continuation chain.
func (s *replyStream) Notice(title, content string, fields map[any]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return
	}
	if rest := s.seg.Flush(); rest != "" {
		s.enqueueLocked(outPart{title: s.title, content: rest})
	}
	seg := NewSegmenter(s.seg.maxUnits, s.seg.maxBytes)
	parts := seg.Write(content)
	if rest := seg.Flush(); rest != "" {
		parts = append(parts, rest)
	}
	for _, part := range parts {
		s.queue = append(s.queue, outPart{title: title, content: part, notice: true, fields: fields})
	}
	s.hasNotices = true
	s.cond.Broadcast()
}

// Finish closes the stream with a terminal status. For StatusFailed,
// errText (if non-empty) becomes the content of the final message, and any
// still-buffered reply text is sent as the preceding part.
func (s *replyStream) Finish(status lxmf.Status, errTitle, errText string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return
	}

	rest := s.seg.Flush()
	switch {
	case status == lxmf.StatusFailed && errText != "":
		if rest != "" {
			s.enqueueLocked(outPart{title: s.title, content: rest})
		}
		title := errTitle
		if title == "" {
			title = s.title
		}
		s.enqueueLocked(outPart{title: title, content: limitText(errText), final: true, status: status})
	case rest != "" && (s.enqueued > 0 || strings.TrimSpace(rest) != ""):
		s.enqueueLocked(outPart{title: s.title, content: rest, final: true, status: status})
	case s.enqueued == 0 && !s.hasNotices && status == lxmf.StatusCompleted:
		s.enqueueLocked(outPart{title: s.title, content: emptyReplyText, final: true, status: status})
	case s.enqueued > 0:
		// A notice can flush the last text before the turn ends. Close
		// that answer chain even when no further text followed the notice.
		s.enqueueLocked(outPart{title: s.title, content: "", final: true, status: status})
	}
	// Otherwise (cancelled/failed without any text): nothing to send.

	s.finished = true
	s.cond.Broadcast()
}

// Wait blocks until every queued part was delivered or delivery failed.
func (s *replyStream) Wait() error {
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *replyStream) enqueueLocked(p outPart) {
	s.queue = append(s.queue, p)
	s.enqueued++
	s.cond.Broadcast()
}

func (s *replyStream) next() (outPart, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) == 0 && !s.finished {
		s.cond.Wait()
	}
	if len(s.queue) == 0 {
		return outPart{}, false
	}
	p := s.queue[0]
	s.queue[0] = outPart{}
	s.queue = s.queue[1:]
	return p, true
}

func (s *replyStream) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
	s.queue = nil
}

func (s *replyStream) failed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err != nil
}

func (s *replyStream) run() {
	defer close(s.done)

	var prev []byte
	first := true
	for {
		p, ok := s.next()
		if !ok {
			return
		}
		if s.failed() {
			continue // drain: never send parts after a delivery failure
		}

		single := first && p.final
		var status lxmf.Status
		if p.final {
			status = p.status
		}
		baseFields := map[any]any{lxmf.FieldRenderer: lxmf.RendererMarkdown}
		if first && len(s.replyTo) > 0 {
			baseFields = lxmf.WithReplyTo(baseFields, s.replyTo, s.replyQuote)
			log.Printf("[Bridge] Attaching reply metadata for %s: replyTo=%x, replyQuote=%q",
				hex.EncodeToString(s.peerHash), s.replyTo, s.replyQuote)
		}
		fields := lxmf.BuildMultipartFields(baseFields, prev, first && !p.final, status)
		if p.notice {
			fields = lxmf.WithRenderer(p.fields, lxmf.RendererPlain)
			if len(s.replyTo) > 0 {
				fields = lxmf.WithReplyTo(fields, s.replyTo, s.replyQuote)
			}
		}

		id, err := s.client.SendMessage(s.localHash, s.peerHash, lxmf.OutboundMessage{
			Title:   p.title,
			Content: p.content,
			Fields:  fields,
			Direct:  p.notice || !single, // multipart chains always use LXMF DIRECT
		})
		if err == nil && len(id) == 0 && !p.final && !p.notice {
			err = errors.New("delivery returned no message hash")
		}
		if err != nil {
			log.Printf("[Bridge] Delivery of reply part from %s to %s failed, aborting remaining parts: %v",
				hex.EncodeToString(s.localHash), hex.EncodeToString(s.peerHash), err)
			s.fail(err)
			continue
		}
		if !p.notice {
			prev = id
			first = false
		}
	}
}

// limitText truncates text to a single LXMF part (used for short notices).
func limitText(text string) string {
	seg := NewSegmenter(0, 0)
	parts := seg.Write(text)
	if len(parts) > 0 {
		return parts[0]
	}
	return seg.Flush()
}
