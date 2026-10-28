package bridge

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mytecor/lxmf-acp/internal/lxmf"
	"github.com/mytecor/lxmf-acp/internal/storage"
	reticulumLxmf "github.com/thatSFguy/reticulum-go/lxmf"
	"github.com/thatSFguy/reticulum-go/rns"
)

// ---------- helpers ----------

func infoOf(m sentMsg) lxmf.MultipartInfo { return lxmf.ParseMultipartFields(m.Fields) }

// asInbound converts a message sent by one bridge into what the peer
// bridge receives.
func asInbound(m sentMsg) *lxmf.InboundMessage {
	return &lxmf.InboundMessage{
		LocalHash:  m.To,
		SenderHash: m.From,
		MessageID:  m.ID,
		Title:      m.Title,
		Content:    m.Content,
		Fields:     m.Fields,
	}
}

func joinContent(msgs []sentMsg) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.Content)
	}
	return b.String()
}

// assertChain checks that msgs form one well-formed multipart reply.
func assertChain(t *testing.T, msgs []sentMsg, status lxmf.Status) {
	t.Helper()
	if len(msgs) == 0 {
		t.Fatalf("empty chain")
	}
	if len(msgs) == 1 {
		info := infoOf(msgs[0])
		if info.Prev != nil || info.More || info.Status != status {
			t.Fatalf("single-part message has wrong metadata: %v", info)
		}
		return
	}
	for i, m := range msgs {
		info := infoOf(m)
		if !m.Direct {
			t.Errorf("part %d not sent with LXMF DIRECT", i)
		}
		switch {
		case i == 0:
			if !info.More || info.Prev != nil || info.Status != "" {
				t.Fatalf("first part metadata wrong: %v", info)
			}
		case i == len(msgs)-1:
			if info.More || info.Status != status || !bytes.Equal(info.Prev, msgs[i-1].ID) {
				t.Fatalf("last part metadata wrong: %v", info)
			}
		default:
			if info.More || info.Status != "" || !bytes.Equal(info.Prev, msgs[i-1].ID) {
				t.Fatalf("middle part %d metadata wrong: %v", i, info)
			}
			if _, ok := m.Fields[lxmf.FieldCustomMeta]; ok {
				t.Fatalf("middle part %d must not carry FIELD_CUSTOM_META", i)
			}
		}
		if n := utf16Len(m.Content); n > DefaultMaxPartUnits {
			t.Fatalf("part %d has %d UTF-16 units", i, n)
		}
	}
}

func sendUserMessage(mockL *mockLXMF, local, peer []byte, id, text string) {
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  local,
		SenderHash: peer,
		MessageID:  []byte(id),
		Title:      "Q",
		Content:    text,
	})
}

func streamText(text string, chunk int) func(context.Context, string, string, func(string)) (string, error) {
	return func(_ context.Context, _, _ string, onChunk func(string)) (string, error) {
		r := []rune(text)
		for i := 0; i < len(r); i += chunk {
			end := min(i+chunk, len(r))
			onChunk(string(r[i:end]))
		}
		return "end_turn", nil
	}
}

func setStream(m *mockACP, fn func(context.Context, string, string, func(string)) (string, error)) {
	m.mu.Lock()
	m.streamFn = fn
	m.mu.Unlock()
}

// ---------- 1-3: segmentation into messages ----------

func TestReplyShorterThanLimitIsSingleMessage(t *testing.T) {
	b, mockL, mockA, _, initialID, user := setupTestBridge(t)
	defer b.Close()
	local := initialID.DestinationHashFor(reticulumLxmf.FullName())

	setStream(mockA, streamText("  short reply\n with spaces  ", 3))
	sendUserMessage(mockL, local, user, "m1", "hi")
	waitFor(t, 2*time.Second, func() bool { return len(mockL.getSent()) == 1 })

	sent := mockL.getSent()
	assertChain(t, sent, lxmf.StatusCompleted)
	if sent[0].Content != "  short reply\n with spaces  " {
		t.Fatalf("content altered: %q", sent[0].Content)
	}
	if sent[0].Direct {
		t.Fatalf("single-part reply should use regular delivery")
	}
}

func TestReplyExactlyAtLimitIsSingleMessage(t *testing.T) {
	for name, text := range map[string]string{
		"ascii":     strings.Repeat("a", DefaultMaxPartUnits),
		"surrogate": strings.Repeat("😀", DefaultMaxPartUnits/2),
	} {
		t.Run(name, func(t *testing.T) {
			b, mockL, mockA, _, initialID, user := setupTestBridge(t)
			defer b.Close()
			local := initialID.DestinationHashFor(reticulumLxmf.FullName())

			setStream(mockA, streamText(text, 97))
			sendUserMessage(mockL, local, user, "m1", "go")
			waitFor(t, 2*time.Second, func() bool { return len(mockL.getSent()) >= 1 })
			time.Sleep(50 * time.Millisecond)

			sent := mockL.getSent()
			if len(sent) != 1 {
				t.Fatalf("expected exactly one message, got %d", len(sent))
			}
			assertChain(t, sent, lxmf.StatusCompleted)
			if sent[0].Content != text {
				t.Fatalf("content mismatch")
			}
		})
	}
}

func TestReplyLongerThanLimitIsLinkedChain(t *testing.T) {
	b, mockL, mockA, _, initialID, user := setupTestBridge(t)
	defer b.Close()
	local := initialID.DestinationHashFor(reticulumLxmf.FullName())

	text := strings.Repeat("x", 25000)
	setStream(mockA, streamText(text, 333))
	sendUserMessage(mockL, local, user, "m1", "go")
	waitFor(t, 2*time.Second, func() bool {
		s := mockL.getSent()
		return len(s) > 0 && infoOf(s[len(s)-1]).Status != ""
	})

	sent := mockL.getSent()
	if len(sent) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(sent))
	}
	assertChain(t, sent, lxmf.StatusCompleted)
	if joinContent(sent) != text {
		t.Fatalf("joined content differs from original")
	}
}

// ---------- 4-5: streaming and sequential delivery ----------

func TestFirstPartIsSentBeforeGenerationEnds(t *testing.T) {
	b, mockL, mockA, _, initialID, user := setupTestBridge(t)
	defer b.Close()
	local := initialID.DestinationHashFor(reticulumLxmf.FullName())

	release := make(chan struct{})
	var finished atomic.Bool
	setStream(mockA, func(_ context.Context, _, _ string, onChunk func(string)) (string, error) {
		onChunk(strings.Repeat("a", 15000))
		<-release // agent still generating
		onChunk("tail")
		finished.Store(true)
		return "end_turn", nil
	})
	sendUserMessage(mockL, local, user, "m1", "go")

	waitFor(t, 2*time.Second, func() bool { return len(mockL.getSent()) >= 1 })
	if finished.Load() {
		t.Fatalf("generation finished before first part was observed")
	}
	if !infoOf(mockL.getSent()[0]).More {
		t.Fatalf("first part must announce more:true")
	}
	close(release)

	waitFor(t, 2*time.Second, func() bool { return len(mockL.getSent()) == 2 })
	sent := mockL.getSent()
	assertChain(t, sent, lxmf.StatusCompleted)
	if sent[1].Content != strings.Repeat("a", 5000)+"tail" {
		t.Fatalf("unexpected tail content length %d", len(sent[1].Content))
	}
}

func TestNextPartWaitsForDelivery(t *testing.T) {
	b, mockL, mockA, _, initialID, user := setupTestBridge(t)
	defer b.Close()
	local := initialID.DestinationHashFor(reticulumLxmf.FullName())

	deliver := make(chan struct{})
	var attempts atomic.Int32
	mockL.mu.Lock()
	mockL.sendHook = func(m sentMsg) error {
		attempts.Add(1)
		<-deliver // stays unconfirmed until the test delivers it
		return nil
	}
	mockL.mu.Unlock()

	var genDone atomic.Bool
	setStream(mockA, func(_ context.Context, _, _ string, onChunk func(string)) (string, error) {
		onChunk(strings.Repeat("b", 35000))
		genDone.Store(true)
		return "end_turn", nil
	})
	sendUserMessage(mockL, local, user, "m1", "go")

	waitFor(t, 2*time.Second, func() bool { return genDone.Load() && attempts.Load() == 1 })
	time.Sleep(100 * time.Millisecond)
	if n := attempts.Load(); n != 1 {
		t.Fatalf("expected 1 in-flight send before DELIVERED, got %d", n)
	}

	for i := 2; i <= 4; i++ {
		deliver <- struct{}{}
		waitFor(t, 2*time.Second, func() bool { return attempts.Load() == int32(i) })
		time.Sleep(30 * time.Millisecond)
		if n := attempts.Load(); n != int32(i) {
			t.Fatalf("more than one unconfirmed send in flight: %d", n)
		}
	}
	deliver <- struct{}{}
	waitFor(t, 2*time.Second, func() bool { return len(mockL.getSent()) == 4 })
	assertChain(t, mockL.getSent(), lxmf.StatusCompleted)
}

func TestDeliveryFailureStopsChain(t *testing.T) {
	b, mockL, mockA, _, initialID, user := setupTestBridge(t)
	defer b.Close()
	local := initialID.DestinationHashFor(reticulumLxmf.FullName())

	var attempts atomic.Int32
	mockL.mu.Lock()
	mockL.sendHook = func(m sentMsg) error {
		if attempts.Add(1) == 2 {
			return errors.New("no delivery proof")
		}
		return nil
	}
	mockL.mu.Unlock()

	setStream(mockA, streamText(strings.Repeat("c", 45000), 1000))
	sendUserMessage(mockL, local, user, "m1", "go")
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() >= 2 })
	time.Sleep(150 * time.Millisecond)

	if n := attempts.Load(); n != 2 {
		t.Fatalf("parts were sent after a delivery failure: %d attempts", n)
	}
	if len(mockL.getSent()) != 1 {
		t.Fatalf("expected only the first part to be delivered")
	}
}

// ---------- 6: Unicode / whitespace preservation ----------

func TestSegmenterPreservesUnicodeAndWhitespace(t *testing.T) {
	pieces := []string{"Привет, мир! ", "😀👍🏽", "e\u0301", "👨\u200D👩\u200D👧", "漢字かな", "\n\n", "\t  ", "🇷🇺", "a"}
	var src strings.Builder
	for i := 0; src.Len() < 20000; i++ {
		src.WriteString(pieces[i%len(pieces)])
	}
	text := src.String()

	for _, limit := range []int{7, 13, 50, 101} {
		seg := NewSegmenter(limit, 0)
		var parts []string
		r := []rune(text)
		for i := 0; i < len(r); i += 5 {
			parts = append(parts, seg.Write(string(r[i:min(i+5, len(r))]))...)
		}
		parts = append(parts, seg.Flush())

		if strings.Join(parts, "") != text {
			t.Fatalf("limit %d: reassembled text differs", limit)
		}
		for i, p := range parts {
			if !utf8.ValidString(p) {
				t.Fatalf("limit %d: part %d is not valid UTF-8", limit, i)
			}
			if p == "" {
				t.Fatalf("limit %d: empty part %d", limit, i)
			}
			if utf16Len(p) > limit {
				t.Fatalf("limit %d: part %d has %d units", limit, i, utf16Len(p))
			}
		}
	}
}

func TestSegmenterByteLimitAndSoftBoundaries(t *testing.T) {
	seg := NewSegmenter(1000, 100)
	parts := seg.Write(strings.Repeat("ж", 120)) // 2 bytes each
	parts = append(parts, seg.Flush())
	for _, p := range parts {
		if len(p) > 100 {
			t.Fatalf("part exceeds byte limit: %d", len(p))
		}
	}
	if strings.Join(parts, "") != strings.Repeat("ж", 120) {
		t.Fatalf("content lost")
	}

	seg = NewSegmenter(20, 0)
	parts = seg.Write("hello world, this is a test\nmore")
	if len(parts) != 1 || parts[0] != "hello world, this " {
		t.Fatalf("expected cut after whitespace, got %q", parts)
	}
}

func TestUnicodeRoundTripThroughPeerBridge(t *testing.T) {
	bA, mockLA, mockAA, _, idA, user := setupTestBridge(t)
	defer bA.Close()
	bA.maxPartUnits = 64
	localA := idA.DestinationHashFor(reticulumLxmf.FullName())

	text := strings.Repeat("  Строка с пробелами 😀👍🏽 e\u0301\n\t", 40)
	setStream(mockAA, streamText(text, 11))
	sendUserMessage(mockLA, localA, user, "m1", "go")
	waitFor(t, 2*time.Second, func() bool {
		s := mockLA.getSent()
		return len(s) > 0 && infoOf(s[len(s)-1]).Status != ""
	})
	sent := mockLA.getSent()
	if len(sent) < 10 {
		t.Fatalf("expected many parts, got %d", len(sent))
	}

	// The peer lxmf-acp receives the chain and must prompt exactly once.
	bB, mockLB, mockAB, _, _, _ := setupTestBridge(t)
	defer bB.Close()
	for _, m := range sent {
		mockLB.handler(asInbound(m))
	}
	waitFor(t, 2*time.Second, func() bool { return len(mockAB.getPrompts()) == 1 })
	time.Sleep(50 * time.Millisecond)
	prompts := mockAB.getPrompts()
	if len(prompts) != 1 || prompts[0] != text {
		t.Fatalf("peer got %d prompts; text equal=%v", len(prompts), len(prompts) == 1 && prompts[0] == text)
	}
}

// ---------- 7-9: assembler behaviour ----------

type chainBuilder struct {
	local, peer []byte
	n           int
}

func (c *chainBuilder) msg(content string, prev []byte, more bool, status lxmf.Status) *lxmf.InboundMessage {
	c.n++
	id := bytes.Repeat([]byte{byte(c.n)}, 32)
	copy(id, c.local[:4])
	return &lxmf.InboundMessage{
		LocalHash:  c.local,
		SenderHash: c.peer,
		MessageID:  id,
		Content:    content,
		Fields:     lxmf.BuildMultipartFields(nil, prev, more, status),
	}
}

func newChain(local, peer string) *chainBuilder {
	return &chainBuilder{local: []byte(local + "________________")[:16], peer: []byte(peer + "________________")[:16]}
}

func TestAssemblerDuplicateDoesNotDuplicateText(t *testing.T) {
	a := NewAssembler(0)
	c := newChain("L", "P")
	p1 := c.msg("one ", nil, true, "")
	p2 := c.msg("two ", p1.MessageID, false, "")
	p3 := c.msg("three", p2.MessageID, false, lxmf.StatusCompleted)

	for _, m := range []*lxmf.InboundMessage{p1, p1, p2, p1, p2} {
		if out := a.Process(m); out != nil {
			t.Fatalf("premature output")
		}
	}
	out := a.Process(p3)
	if out == nil || out.Content != "one two three" || !out.Multipart {
		t.Fatalf("unexpected result: %+v", out)
	}
	if a.Process(p3) != nil {
		t.Fatalf("duplicate final part produced a second prompt")
	}

	single := c.msg("solo", nil, false, lxmf.StatusCompleted)
	if a.Process(single) == nil || a.Process(single) != nil {
		t.Fatalf("single-part duplicate handling wrong")
	}
}

func TestAssemblerMissingPartBlocksCompletion(t *testing.T) {
	a := NewAssembler(time.Minute)
	clock := time.Now()
	a.now = func() time.Time { return clock }

	c := newChain("L", "P")
	p1 := c.msg("A", nil, true, "")
	p2 := c.msg("B", p1.MessageID, false, "")
	p3 := c.msg("C", p2.MessageID, false, lxmf.StatusCompleted)

	if a.Process(p1) != nil || a.Process(p3) != nil {
		t.Fatalf("chain completed despite missing part")
	}
	if a.pending() != 1 {
		t.Fatalf("expected chain to stay pending")
	}

	// A late predecessor (handlers ran out of order) completes the chain.
	out := a.Process(p2)
	if out == nil || out.Content != "ABC" {
		t.Fatalf("late part did not complete chain: %+v", out)
	}

	// A chain whose part never arrives is dropped by the timeout.
	q1 := c.msg("X", nil, true, "")
	q3 := c.msg("Z", []byte("missing-hash-missing-hash-123456"), false, lxmf.StatusCompleted)
	a.Process(q1)
	a.Process(q3)
	clock = clock.Add(2 * time.Minute)
	a.Sweep()
	if a.pending() != 0 || len(a.orphans) != 0 {
		t.Fatalf("stale chain/orphan not cleaned up")
	}
}

func TestAssemblerOrphanWithoutChainIsNotDelivered(t *testing.T) {
	a := NewAssembler(0)
	c := newChain("L", "P")
	mid := c.msg("middle", []byte("unknown-previous-hash-0123456789"), false, "")
	if a.Process(mid) != nil {
		t.Fatalf("intermediate part delivered to agent")
	}
}

func TestFailedAndCancelledDoNotPrompt(t *testing.T) {
	for _, st := range []lxmf.Status{lxmf.StatusFailed, lxmf.StatusCancelled} {
		t.Run(string(st), func(t *testing.T) {
			b, mockL, mockA, _, initialID, _ := setupTestBridge(t)
			defer b.Close()
			local := initialID.DestinationHashFor(reticulumLxmf.FullName())
			c := newChain("", "")
			c.local, c.peer = local, []byte("peer_lxmf_acp_instance__________")

			p1 := c.msg("partial ", nil, true, "")
			p2 := c.msg("text", p1.MessageID, false, st)
			mockL.handler(p1)
			mockL.handler(p2)
			mockL.handler(c.msg("single failed", nil, false, st))
			time.Sleep(150 * time.Millisecond)

			if n := len(mockA.getPrompts()); n != 0 {
				t.Fatalf("%s chain produced %d prompts", st, n)
			}
			if b.assembler.pending() != 0 {
				t.Fatalf("terminated chain left pending")
			}
		})
	}
}

func TestOutboundStatusOnAgentErrorAndCancel(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		b, mockL, mockA, _, initialID, user := setupTestBridge(t)
		defer b.Close()
		local := initialID.DestinationHashFor(reticulumLxmf.FullName())
		setStream(mockA, func(_ context.Context, _, _ string, onChunk func(string)) (string, error) {
			onChunk(strings.Repeat("p", 12000))
			return "", errors.New("boom")
		})
		sendUserMessage(mockL, local, user, "m1", "go")
		waitFor(t, 2*time.Second, func() bool {
			s := mockL.getSent()
			return len(s) > 0 && infoOf(s[len(s)-1]).Status != ""
		})
		sent := mockL.getSent()
		assertChain(t, sent, lxmf.StatusFailed)
		if !strings.Contains(sent[len(sent)-1].Content, "boom") {
			t.Fatalf("final failed message should describe the error")
		}
		if joinContent(sent[:len(sent)-1]) != strings.Repeat("p", 12000) {
			t.Fatalf("partial reply text lost")
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		b, mockL, mockA, _, initialID, user := setupTestBridge(t)
		defer b.Close()
		local := initialID.DestinationHashFor(reticulumLxmf.FullName())
		setStream(mockA, func(_ context.Context, _, _ string, onChunk func(string)) (string, error) {
			onChunk(strings.Repeat("q", 10500))
			return "cancelled", nil
		})
		sendUserMessage(mockL, local, user, "m1", "go")
		waitFor(t, 2*time.Second, func() bool {
			s := mockL.getSent()
			return len(s) > 0 && infoOf(s[len(s)-1]).Status != ""
		})
		sent := mockL.getSent()
		assertChain(t, sent, lxmf.StatusCancelled)
		if joinContent(sent) != strings.Repeat("q", 10500) {
			t.Fatalf("content mismatch")
		}
	})
}

// ---------- 10: plain messages unchanged ----------

func TestPlainMessageProcessedImmediately(t *testing.T) {
	a := NewAssembler(0)
	m := &lxmf.InboundMessage{LocalHash: []byte("l"), SenderHash: []byte("p"), Content: "hi"}
	if a.Process(m) != m {
		t.Fatalf("plain message was not passed through unchanged")
	}

	b, mockL, mockA, _, initialID, user := setupTestBridge(t)
	defer b.Close()
	local := initialID.DestinationHashFor(reticulumLxmf.FullName())
	sendUserMessage(mockL, local, user, "m1", "Plain question")
	waitFor(t, 2*time.Second, func() bool { return len(mockL.getSent()) == 1 })
	if p := mockA.getPrompts(); len(p) != 1 || p[0] != "Plain question" {
		t.Fatalf("plain message not prompted as before: %v", p)
	}
	if got := mockL.getSent()[0].Content; got != "Agent reply: Plain question" {
		t.Fatalf("unexpected reply %q", got)
	}
}

// ---------- 11: concurrent chats ----------

func TestConcurrentChatsDoNotMix(t *testing.T) {
	// Inbound: two interleaved chains in different chats.
	a := NewAssembler(0)
	c1 := newChain("L1", "P1")
	c2 := newChain("L2", "P2")
	a1 := c1.msg("1a", nil, true, "")
	b1 := c2.msg("2a", nil, true, "")
	a2 := c1.msg("1b", a1.MessageID, false, "")
	b2 := c2.msg("2b", b1.MessageID, false, "")
	a3 := c1.msg("1c", a2.MessageID, false, lxmf.StatusCompleted)
	b3 := c2.msg("2c", b2.MessageID, false, lxmf.StatusCompleted)
	var outs []*lxmf.InboundMessage
	for _, m := range []*lxmf.InboundMessage{a1, b1, a2, b2, b3, a3} {
		if o := a.Process(m); o != nil {
			outs = append(outs, o)
		}
	}
	if len(outs) != 2 || outs[0].Content != "2a2b2c" || outs[1].Content != "1a1b1c" {
		t.Fatalf("chains mixed: %+v", outs)
	}

	// A continuation claiming another chat's hash must not attach to it.
	evil := newChain("L1", "PX").msg("evil", a1.MessageID, false, lxmf.StatusCompleted)
	if a.Process(evil) != nil {
		t.Fatalf("part from a different chat joined a chain")
	}

	// Outbound: two chats stream in parallel with slow deliveries.
	b, mockL, mockA, store, initialID, user := setupTestBridge(t)
	defer b.Close()
	local := initialID.DestinationHashFor(reticulumLxmf.FullName())
	user2 := []byte("user2_destination_hash1234567890")

	// Create dedicated chat for user2 (since every chat has its own author)
	user2ID, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("failed to create identity: %v", err)
	}
	local2 := user2ID.DestinationHashFor(reticulumLxmf.FullName())
	_ = mockL.RegisterIdentity(user2ID)
	_ = store.SaveChat(storage.ChatRecord{
		Hash:       hex.EncodeToString(local2),
		PrivateKey: hex.EncodeToString(user2ID.PrivateKey()),
		Owner:      hex.EncodeToString(user2),
		CreatedAt:  time.Now(),
	})

	var inflight sync.Map
	var overlap atomic.Bool
	mockL.mu.Lock()
	mockL.sendHook = func(m sentMsg) error {
		key := string(m.To)
		if _, busy := inflight.LoadOrStore(key, true); busy {
			overlap.Store(true)
		}
		time.Sleep(5 * time.Millisecond)
		inflight.Delete(key)
		return nil
	}
	mockL.mu.Unlock()

	setStream(mockA, func(_ context.Context, _, prompt string, onChunk func(string)) (string, error) {
		r := strings.Repeat(prompt, 30000/len(prompt))
		for i := 0; i < len(r); i += 700 {
			onChunk(r[i:min(i+700, len(r))])
		}
		return "end_turn", nil
	})
	sendUserMessage(mockL, local, user, "u1", "AAAA")
	sendUserMessage(mockL, local2, user2, "u2", "BBBB")

	byPeer := func(peer []byte) []sentMsg {
		var out []sentMsg
		for _, m := range mockL.getSent() {
			if bytes.Equal(m.To, peer) {
				out = append(out, m)
			}
		}
		return out
	}
	done := func(peer []byte) bool {
		s := byPeer(peer)
		return len(s) > 0 && infoOf(s[len(s)-1]).Status != ""
	}
	waitFor(t, 3*time.Second, func() bool { return done(user) && done(user2) })

	if overlap.Load() {
		t.Fatalf("more than one unconfirmed send in a single chat")
	}
	for peer, prompt := range map[string]string{string(user): "AAAA", string(user2): "BBBB"} {
		s := byPeer([]byte(peer))
		assertChain(t, s, lxmf.StatusCompleted)
		if joinContent(s) != strings.Repeat(prompt, 30000/len(prompt)) {
			t.Fatalf("chat content mixed for prompt %s", prompt)
		}
	}
}

// ---------- 12: markdown renderer support ----------

func TestOutboundStreamEmitsMarkdownRenderer(t *testing.T) {
	b, mockL, mockA, _, initialID, user := setupTestBridge(t)
	defer b.Close()
	local := initialID.DestinationHashFor(reticulumLxmf.FullName())

	// Multi-part response
	setStream(mockA, streamText(strings.Repeat("Long markdown response line with **bold** text.\n", 300), 200))
	sendUserMessage(mockL, local, user, "md-1", "Generate markdown")

	waitFor(t, 2*time.Second, func() bool {
		sent := mockL.getSent()
		return len(sent) > 1 && infoOf(sent[len(sent)-1]).Status == lxmf.StatusCompleted
	})

	sent := mockL.getSent()
	if len(sent) < 2 {
		t.Fatalf("expected multi-part stream, got %d parts", len(sent))
	}

	for i, part := range sent {
		renderer, ok := lxmf.ParseRenderer(part.Fields)
		if !ok || renderer != lxmf.RendererMarkdown {
			t.Errorf("part %d missing or wrong renderer: got (%d, %v), want (%d, true)", i, renderer, ok, lxmf.RendererMarkdown)
		}
	}
}

func TestHelpCommandMarkdownFormatting(t *testing.T) {
	b, mockL, _, _, initialID, user := setupTestBridge(t)
	defer b.Close()
	local := initialID.DestinationHashFor(reticulumLxmf.FullName())

	sendUserMessage(mockL, local, user, "cmd-help", "/bridge help")

	waitFor(t, 2*time.Second, func() bool {
		return len(mockL.getSent()) >= 1
	})

	sent := mockL.getSent()
	last := sent[len(sent)-1]
	if last.Title != "Help" {
		t.Fatalf("expected Help title, got %q", last.Title)
	}
	if !strings.Contains(last.Content, "**LXMF-ACP Agent Bridge commands:**") {
		t.Errorf("expected bold title in markdown help, got: %q", last.Content)
	}
	if !strings.Contains(last.Content, "`/bridge new`") || !strings.Contains(last.Content, "`/bridge cancel`") {
		t.Errorf("expected backticks in markdown help, got: %q", last.Content)
	}
}

func TestInboundAssemblerPreservesRenderer(t *testing.T) {
	a := NewAssembler(0)
	c := newChain("L", "P")
	p1 := c.msg("part1 ", nil, true, "")
	p1.Renderer = lxmf.RendererMarkdown
	p1.Fields = lxmf.WithRenderer(p1.Fields, lxmf.RendererMarkdown)

	p2 := c.msg("part2", p1.MessageID, false, lxmf.StatusCompleted)

	if out := a.Process(p1); out != nil {
		t.Fatalf("premature output on first part")
	}
	out := a.Process(p2)
	if out == nil {
		t.Fatalf("expected assembled output on final part")
	}
	if out.Renderer != lxmf.RendererMarkdown {
		t.Fatalf("assembled Renderer = %d, want %d", out.Renderer, lxmf.RendererMarkdown)
	}
	if !out.IsMarkdown() {
		t.Fatalf("expected assembled message IsMarkdown() == true")
	}
}

func TestInboundAssemblerPreservesReplyFields(t *testing.T) {
	a := NewAssembler(0)
	c := newChain("L", "P")
	replyID := []byte{0x12, 0x34, 0x56, 0x78}
	quote := "quoted previous text"

	p1 := c.msg("part1 ", nil, true, "")
	p1.ReplyTo = replyID
	p1.ReplyQuote = quote
	p1.Fields = lxmf.WithReplyTo(p1.Fields, replyID, quote)

	p2 := c.msg("part2", p1.MessageID, false, lxmf.StatusCompleted)

	if out := a.Process(p1); out != nil {
		t.Fatalf("premature output on first part")
	}
	out := a.Process(p2)
	if out == nil {
		t.Fatalf("expected assembled output on final part")
	}
	if !bytes.Equal(out.ReplyTo, replyID) {
		t.Fatalf("assembled ReplyTo = %x, want %x", out.ReplyTo, replyID)
	}
	if out.ReplyQuote != quote {
		t.Fatalf("assembled ReplyQuote = %q, want %q", out.ReplyQuote, quote)
	}
	if !out.IsReply() {
		t.Fatalf("expected assembled message IsReply() == true")
	}
}
