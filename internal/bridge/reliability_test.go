package bridge

import (
	"bytes"
	"context"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/lxmf-acp/internal/config"
	"github.com/mytecor/lxmf-acp/internal/lxmf"
	"github.com/mytecor/lxmf-acp/internal/storage"
	wirelxmf "github.com/thatSFguy/reticulum-go/lxmf"
	"github.com/thatSFguy/reticulum-go/rns"
	"github.com/vmihailenco/msgpack/v5"
)

func durableBridge(t *testing.T) (*Bridge, *lxmf.Client, *mockACP, *storage.DeliveryStore, []byte) {
	t.Helper()
	dir := t.TempDir()
	state, err := storage.NewStore(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	spool, err := storage.OpenDeliveryStore(filepath.Join(dir, "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spool.Close() })
	id, _ := rns.NewIdentity()
	peerID, _ := rns.NewIdentity()
	peer := peerID.DestinationHashFor(wirelxmf.FullName())
	client, err := lxmf.NewClient("", "test", id)
	if err != nil {
		t.Fatal(err)
	}
	client.ConfigureDelivery(spool, lxmf.DeliveryOptions{})
	if err := state.SaveChat(storage.ChatRecord{Hash: client.HexHash(), Owner: hex.EncodeToString(peer), CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	acp := newMockACP(true)
	b := NewBridge(&config.Config{DisplayName: "test"}, client, acp, state)
	b.ConfigureReliability(spool)
	t.Cleanup(func() { b.Close() })
	return b, client, acp, spool, peer
}
func TestDurableStreamingQueuesBeforeNetworkAndDeduplicatesPrompt(t *testing.T) {
	b, c, agent, spool, peer := durableBridge(t)
	answer := strings.Repeat("hello 😀\n", 6000)
	setStream(agent, streamText(answer, 71))
	msg := &lxmf.InboundMessage{LocalHash: c.Hash(), SenderHash: peer, MessageID: []byte("request"), Content: "go"}
	b.handleInbound(msg)
	key := chatKeyOf(msg)
	waitFor(t, 3*time.Second, func() bool { state, _ := spool.IncomingState(key, msg.MessageID); return state == "done" })
	b.handleInbound(msg)
	if len(agent.getPrompts()) != 1 {
		t.Fatal("duplicate request executed again")
	}
	// No transport exists: generation still completes and every part is durable.
	var contents strings.Builder
	var prev []byte
	var count int
	for {
		due, err := spool.Due(time.Now(), 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(due) == 0 {
			break
		}
		m := due[0]
		parsed, err := wirelxmf.ParseDirectBody(m.Wire)
		if err != nil {
			t.Fatal(err)
		}
		info := lxmf.ParseMultipartFields(parsed.Fields)
		if count > 0 && !bytes.Equal(info.Prev, prev) {
			t.Fatal("continuation references an unpersisted/new ID")
		}
		contents.Write(parsed.Content)
		prev = m.ID
		count++
		if err := spool.Delivered(m.Seq); err != nil {
			t.Fatal(err)
		}
	}
	if count < 2 || contents.String() != answer {
		t.Fatal("generated reply lost or reordered", count)
	}
}
func TestInboundFragmentsSurviveBridgeRestartAndOutOfOrder(t *testing.T) {
	b, c, agent, spool, peer := durableBridge(t)
	firstID, lastID := []byte("first"), []byte("last")
	first := &lxmf.InboundMessage{LocalHash: c.Hash(), SenderHash: peer, MessageID: firstID, Content: "hello ", Fields: lxmf.BuildMultipartFields(map[any]any{lxmf.FieldRenderer: lxmf.RendererMarkdown}, nil, true, "")}
	last := &lxmf.InboundMessage{LocalHash: c.Hash(), SenderHash: peer, MessageID: lastID, Content: "world", Fields: lxmf.BuildMultipartFields(nil, firstID, false, lxmf.StatusCompleted)}
	// Last arrives before first, followed by loss of the entire in-memory assembler.
	b.handleInbound(last)
	if len(agent.getPrompts()) != 0 {
		t.Fatal("orphan executed")
	}
	b.assembler = NewAssembler(DefaultAssemblyTimeout)
	if err := b.recoverIncoming(); err != nil {
		t.Fatal(err)
	}
	b.handleInbound(first)
	waitFor(t, 2*time.Second, func() bool { state, _ := spool.IncomingState(chatKeyOf(last), lastID); return state == "done" })
	prompts := agent.getPrompts()
	if len(prompts) != 1 || prompts[0] != "hello world" {
		t.Fatal("recovered chain incorrect", prompts)
	}
	b.handleInbound(last)
	b.handleInbound(first)
	if len(agent.getPrompts()) != 1 {
		t.Fatal("fragments triggered duplicate execution")
	}
}
func TestSavedPromptWithNumericMetadataDecodes(t *testing.T) {
	original := lxmf.InboundMessage{Fields: map[any]any{lxmf.FieldCustomMeta: map[any]any{lxmf.MetaNamespace: map[any]any{"version": 1, "calls": []any{map[any]any{3: []byte("numeric")}}}}, lxmf.FieldRenderer: lxmf.RendererMarkdown}}
	raw, err := msgpack.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded lxmf.InboundMessage
	if err := decodeIncoming(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Fields) != 2 {
		t.Fatal("fields lost")
	}
}
func TestShutdownKeepsGeneratedTextWithoutReplayingACP(t *testing.T) {
	b, c, agent, spool, peer := durableBridge(t)
	setStream(agent, func(ctx context.Context, _, _ string, onChunk func(string)) (string, error) {
		onChunk("unfinished but saved")
		<-ctx.Done()
		return "", ctx.Err()
	})
	msg := &lxmf.InboundMessage{LocalHash: c.Hash(), SenderHash: peer, MessageID: []byte("interrupted"), Content: "execute"}
	b.handleInbound(msg)
	waitFor(t, time.Second, func() bool { return len(agent.getPrompts()) == 1 })
	// Wait for journal write, not merely entry into the ACP stub.
	waitFor(t, time.Second, func() bool { state, _ := spool.IncomingState(chatKeyOf(msg), msg.MessageID); return state == "running" })
	b.Close()
	state, err := spool.IncomingState(chatKeyOf(msg), msg.MessageID)
	if err != nil || state != "uncertain" {
		t.Fatal("interrupted request not marked uncertain", state, err)
	}
	text, err := spool.RecoverOutput(chatKeyOf(msg))
	if err != nil || text != "unfinished but saved" {
		t.Fatal("buffered text lost", text, err)
	}
}

func TestDurableAssemblerHandlesMoreThan64Orphans(t *testing.T) {
	b, c, agent, spool, peer := durableBridge(t)
	ids := make([][]byte, 70)
	for i := range ids {
		ids[i] = []byte{byte(i + 1)}
	}
	for i := 69; i >= 1; i-- {
		status := lxmf.Status("")
		if i == 69 {
			status = lxmf.StatusCompleted
		}
		b.handleInbound(&lxmf.InboundMessage{LocalHash: c.Hash(), SenderHash: peer, MessageID: ids[i], Content: "x", Fields: lxmf.BuildMultipartFields(nil, ids[i-1], false, status)})
	}
	if len(agent.getPrompts()) != 0 {
		t.Fatal("incomplete chain executed")
	}
	if err := b.recoverIncoming(); err != nil {
		t.Fatal(err)
	}
	first := &lxmf.InboundMessage{LocalHash: c.Hash(), SenderHash: peer, MessageID: ids[0], Content: "x", Fields: lxmf.BuildMultipartFields(nil, nil, true, "")}
	b.handleInbound(first)
	waitFor(t, 3*time.Second, func() bool { state, _ := spool.IncomingState(chatKeyOf(first), ids[69]); return state == "done" })
	prompts := agent.getPrompts()
	if len(prompts) != 1 || prompts[0] != strings.Repeat("x", 70) {
		t.Fatal("out-of-order parts lost", prompts)
	}
}
func TestQueueBudgetPreservesGeneratedTextForRecovery(t *testing.T) {
	b, c, agent, spool, peer := durableBridge(t)
	spool.MaxPendingBytes = 1
	setStream(agent, streamText("answer retained despite full outgoing queue", 10))
	msg := &lxmf.InboundMessage{LocalHash: c.Hash(), SenderHash: peer, MessageID: []byte("full-queue"), Content: "go"}
	b.handleInbound(msg)
	waitFor(t, time.Second, func() bool {
		state, _ := spool.IncomingState(chatKeyOf(msg), msg.MessageID)
		return state == "uncertain"
	})
	output, err := spool.RecoverOutput(chatKeyOf(msg))
	if err != nil || output != "answer retained despite full outgoing queue" {
		t.Fatal("full queue lost generated text", output, err)
	}
}
