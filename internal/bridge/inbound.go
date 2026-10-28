package bridge

import (
	"bytes"
	"encoding/hex"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/mytecor/lxmf-acp/internal/lxmf"
)

const (
	// DefaultAssemblyTimeout drops incomplete inbound chains (and orphan
	// parts) that saw no new part for this long.
	DefaultAssemblyTimeout = time.Hour
	// maxAssemblyBytes bounds the RAM used by a single inbound chain.
	maxAssemblyBytes = 8 << 20
	// maxOrphansPerChat bounds parts buffered while their predecessor is
	// still missing.
	maxOrphansPerChat = 64
	// dedupRetention is how long multipart message hashes are remembered.
	dedupRetention = 2 * time.Hour
)

// assembly is one inbound multipart chain being collected.
type assembly struct {
	chatKey    string
	localHash  []byte
	peerHash   []byte
	title      string
	text       strings.Builder
	lastID     []byte
	renderer   int
	replyTo    []byte
	replyQuote string
	updated    time.Time
	parts      [][]byte
}

// orphan is a continuation part whose predecessor has not been seen yet
// (e.g. the inbound handlers ran out of order, or a part is missing).
type orphan struct {
	msg      *lxmf.InboundMessage
	info     lxmf.MultipartInfo
	received time.Time
}

// Assembler reassembles multipart LXMF chains into single prompts.
//
// Chains are linked by full LXMF message hashes and scoped by chat
// (local destination + peer source), so parts from different chats never
// mix. All state lives in RAM.
type Assembler struct {
	mu      sync.Mutex
	timeout time.Duration
	now     func() time.Time

	// byTail: chatKey|hex(last message hash) -> chain awaiting its next part.
	byTail map[string]*assembly
	// orphans: chatKey|hex(prev hash) -> part waiting for that predecessor.
	orphans map[string]*orphan
	// seen: chatKey|hex(message hash) -> first seen time (duplicate guard).
	seen map[string]time.Time
}

func NewAssembler(timeout time.Duration) *Assembler {
	if timeout <= 0 {
		timeout = DefaultAssemblyTimeout
	}
	return &Assembler{
		timeout: timeout,
		now:     time.Now,
		byTail:  make(map[string]*assembly),
		orphans: make(map[string]*orphan),
		seen:    make(map[string]time.Time),
	}
}

func chatKeyOf(msg *lxmf.InboundMessage) string {
	return hex.EncodeToString(msg.LocalHash) + ":" + hex.EncodeToString(msg.SenderHash)
}

func linkKey(chatKey string, hash []byte) string {
	return chatKey + "|" + hex.EncodeToString(hash)
}

// Process handles one inbound LXMF message. It returns the message that
// should be handed to the prompt handler, or nil if nothing is ready yet
// (intermediate part, duplicate, or a chain that ended unsuccessfully).
// Messages without multipart signalling are returned unchanged.
func (a *Assembler) Process(msg *lxmf.InboundMessage) *lxmf.InboundMessage {
	if len(msg.ReplyTo) == 0 && msg.ReplyQuote == "" && msg.Fields != nil {
		msg.ReplyTo, msg.ReplyQuote = lxmf.ParseReplyFields(msg.Fields)
	}

	info := lxmf.ParseMultipartFields(msg.Fields)
	if !info.IsMultipart() {
		return msg
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.now()
	a.sweepLocked(now)

	chatKey := chatKeyOf(msg)
	if len(msg.MessageID) > 0 {
		k := linkKey(chatKey, msg.MessageID)
		if _, dup := a.seen[k]; dup {
			log.Printf("[Multipart] Duplicate part %x in chat %s ignored", msg.MessageID, chatKey)
			return nil
		}
		a.seen[k] = now
	}

	// Standalone reply carrying only a status.
	if info.Prev == nil && info.Status != "" {
		if info.Status == lxmf.StatusCompleted {
			return msg
		}
		log.Printf("[Multipart] Single-part message in chat %s ended with status %q; not prompting", chatKey, info.Status)
		return nil
	}

	// First part of a chain.
	if info.Prev == nil {
		if len(msg.MessageID) == 0 {
			log.Printf("[Multipart] First part without message hash in chat %s dropped", chatKey)
			return nil
		}
		replyTo := msg.ReplyTo
		replyQuote := msg.ReplyQuote
		asm := &assembly{
			chatKey:    chatKey,
			localHash:  append([]byte(nil), msg.LocalHash...),
			peerHash:   append([]byte(nil), msg.SenderHash...),
			title:      msg.Title,
			lastID:     append([]byte(nil), msg.MessageID...),
			renderer:   msg.Renderer,
			replyTo:    append([]byte(nil), replyTo...),
			replyQuote: replyQuote,
			updated:    now,
			parts:      [][]byte{append([]byte(nil), msg.MessageID...)},
		}
		asm.text.WriteString(msg.Content)
		a.byTail[linkKey(chatKey, asm.lastID)] = asm
		return a.drainOrphansLocked(asm, now)
	}

	// Continuation part.
	asm, ok := a.byTail[linkKey(chatKey, info.Prev)]
	if !ok {
		a.stashOrphanLocked(chatKey, msg, info, now)
		return nil
	}
	return a.appendLocked(asm, msg, info, now)
}

// appendLocked attaches msg to asm and, if it was the final part, returns
// the assembled prompt. Afterwards it follows any buffered orphans.
func (a *Assembler) appendLocked(asm *assembly, msg *lxmf.InboundMessage, info lxmf.MultipartInfo, now time.Time) *lxmf.InboundMessage {
	delete(a.byTail, linkKey(asm.chatKey, asm.lastID))

	if !bytes.Equal(msg.LocalHash, asm.localHash) || !bytes.Equal(msg.SenderHash, asm.peerHash) {
		// Unreachable via chatKey scoping; kept as an explicit check.
		log.Printf("[Multipart] Source/destination mismatch for part %x; dropping chain", msg.MessageID)
		return nil
	}
	if asm.text.Len()+len(msg.Content) > maxAssemblyBytes {
		log.Printf("[Multipart] Chain in chat %s exceeds %d bytes; dropping", asm.chatKey, maxAssemblyBytes)
		return nil
	}

	asm.text.WriteString(msg.Content)
	asm.parts = append(asm.parts, append([]byte(nil), msg.MessageID...))
	asm.updated = now

	if info.Status != "" {
		if info.Status != lxmf.StatusCompleted {
			log.Printf("[Multipart] Chain in chat %s ended with status %q; not prompting", asm.chatKey, info.Status)
			return nil
		}
		var fields map[any]any
		if asm.renderer >= 0 {
			fields = map[any]any{lxmf.FieldRenderer: asm.renderer}
		}
		if len(asm.replyTo) > 0 {
			if fields == nil {
				fields = make(map[any]any)
			}
			fields[lxmf.FieldReplyTo] = asm.replyTo
		}
		if asm.replyQuote != "" {
			if fields == nil {
				fields = make(map[any]any)
			}
			fields[lxmf.FieldReplyQuote] = asm.replyQuote
		}
		return &lxmf.InboundMessage{
			LocalHash:  asm.localHash,
			SenderHash: asm.peerHash,
			MessageID:  append([]byte(nil), msg.MessageID...),
			Title:      asm.title,
			Content:    asm.text.String(),
			Multipart:  true,
			Parts:      asm.parts,
			Renderer:   asm.renderer,
			Fields:     fields,
			ReplyTo:    asm.replyTo,
			ReplyQuote: asm.replyQuote,
		}
	}

	if len(msg.MessageID) == 0 {
		log.Printf("[Multipart] Intermediate part without message hash in chat %s; dropping chain", asm.chatKey)
		return nil
	}
	asm.lastID = append([]byte(nil), msg.MessageID...)
	a.byTail[linkKey(asm.chatKey, asm.lastID)] = asm
	return a.drainOrphansLocked(asm, now)
}

// drainOrphansLocked continues asm with a buffered part that references its
// current tail, if any.
func (a *Assembler) drainOrphansLocked(asm *assembly, now time.Time) *lxmf.InboundMessage {
	k := linkKey(asm.chatKey, asm.lastID)
	o, ok := a.orphans[k]
	if !ok {
		return nil
	}
	delete(a.orphans, k)
	return a.appendLocked(asm, o.msg, o.info, now)
}

func (a *Assembler) stashOrphanLocked(chatKey string, msg *lxmf.InboundMessage, info lxmf.MultipartInfo, now time.Time) {
	k := linkKey(chatKey, info.Prev)
	if _, exists := a.orphans[k]; exists {
		log.Printf("[Multipart] Conflicting continuation of %x in chat %s dropped", info.Prev, chatKey)
		return
	}
	count := 0
	prefix := chatKey + "|"
	for key := range a.orphans {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	if count >= maxOrphansPerChat {
		log.Printf("[Multipart] Too many parts awaiting predecessors in chat %s; dropping %x", chatKey, msg.MessageID)
		return
	}
	log.Printf("[Multipart] Part %x in chat %s waits for missing predecessor %x", msg.MessageID, chatKey, info.Prev)
	a.orphans[k] = &orphan{msg: msg, info: info, received: now}
}

// Sweep drops incomplete chains and orphans idle for longer than the
// timeout, and forgets old duplicate-guard entries.
func (a *Assembler) Sweep() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sweepLocked(a.now())
}

func (a *Assembler) sweepLocked(now time.Time) {
	for k, asm := range a.byTail {
		if now.Sub(asm.updated) > a.timeout {
			log.Printf("[Multipart] Incomplete chain in chat %s timed out; discarding %d bytes", asm.chatKey, asm.text.Len())
			delete(a.byTail, k)
		}
	}
	for k, o := range a.orphans {
		if now.Sub(o.received) > a.timeout {
			delete(a.orphans, k)
		}
	}
	for k, t := range a.seen {
		if now.Sub(t) > dedupRetention {
			delete(a.seen, k)
		}
	}
}

// pending reports the number of incomplete chains (for tests).
func (a *Assembler) pending() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.byTail)
}
