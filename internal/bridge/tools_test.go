package bridge

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/lxmf-acp/internal/acp"
	"github.com/mytecor/lxmf-acp/internal/lxmf"
	"github.com/mytecor/lxmf-acp/internal/storage"
	reticulumLxmf "github.com/thatSFguy/reticulum-go/lxmf"
)

func toolString(s string) *string { return &s }

func reporterFixture(t *testing.T, mode string, interval time.Duration) (*toolReporter, *replyStream, *mockLXMF, *storage.Store) {
	t.Helper()
	store, err := storage.NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	client := newMockLXMF(nil)
	stream := newReplyStream(client, []byte("local"), []byte("peer"), "", nil, "", NewSegmenter(0, 0))
	if err := store.SetToolMode(stream.chatKey, mode); err != nil {
		t.Fatal(err)
	}
	reporter := newToolReporter(stream, store, interval)
	t.Cleanup(func() { reporter.Finish(false); stream.Finish(lxmf.StatusCompleted, "", ""); _ = stream.Wait() })
	return reporter, stream, client, store
}

func TestToolBatchPartialMergeOrderingAndMetadata(t *testing.T) {
	reporter, stream, client, store := reporterFixture(t, "compact", time.Hour)
	reporter.Event(acp.Event{Text: "Looking at the files.\n"})
	reporter.Event(acp.Event{Tool: &acp.ToolUpdate{ID: "a", Title: toolString("Read config"), Input: toolString("secret-input"), Content: toolString("old result")}})
	reporter.Event(acp.Event{Tool: &acp.ToolUpdate{ID: "b", Title: toolString("Run tests")}})
	reporter.Event(acp.Event{Tool: &acp.ToolUpdate{ID: "a", Status: toolString("completed"), Content: toolString("new result")}})
	reporter.Event(acp.Event{Tool: &acp.ToolUpdate{ID: "b", Status: toolString("failed"), Output: toolString("exit 1")}})
	reporter.Event(acp.Event{Text: "Done."})
	reporter.Finish(false)
	stream.Finish(lxmf.StatusCompleted, "", "")
	if err := stream.Wait(); err != nil {
		t.Fatal(err)
	}
	sent := client.getSent()
	if len(sent) != 3 || sent[0].Content != "Looking at the files.\n" || sent[2].Content != "Done." {
		t.Fatalf("unexpected ordering: %+v", sent)
	}
	notice := sent[1]
	for _, want := range []string{"✓ #1 Read config [completed]", "new result", "✗ #2 Run tests [failed]", "exit 1"} {
		if !strings.Contains(notice.Content, want) {
			t.Fatalf("missing %q in %q", want, notice.Content)
		}
	}
	if strings.Contains(notice.Content, "secret-input") || strings.Contains(notice.Content, "old result") {
		t.Fatal("compact leaked input or retained replaced content")
	}
	if lxmf.ParseMultipartFields(notice.Fields).IsMultipart() {
		t.Fatal("tools entered multipart chain")
	}
	renderer, _ := lxmf.ParseRenderer(notice.Fields)
	if renderer != lxmf.RendererPlain {
		t.Fatal("tools must be readable as plain text")
	}
	continuation := lxmf.ParseMultipartFields(sent[2].Fields)
	if !bytes.Equal(continuation.Prev, sent[0].ID) || continuation.Status != lxmf.StatusCompleted {
		t.Fatal("tool message broke answer continuation")
	}
	meta := notice.Fields[lxmf.FieldCustomMeta].(map[any]any)[lxmf.MetaNamespace].(map[any]any)
	if meta["version"] != 1 || meta["sequence"] != 1 || meta["turn_id"] != reporter.turnID || len(meta["calls"].([]any)) != 2 {
		t.Fatalf("metadata: %+v", meta)
	}
	record, ok := store.GetTool(stream.chatKey, 1)
	if !ok || record.Title != "Read config" || record.Input != "secret-input" || record.Content != "new result" {
		t.Fatalf("partial merge failed: %+v", record)
	}

}

func TestLongToolTickAndCompletionDoNotRepeatOrReopen(t *testing.T) {
	reporter, stream, client, _ := reporterFixture(t, "compact", 10*time.Millisecond)
	reporter.Event(acp.Event{Tool: &acp.ToolUpdate{ID: "a", Title: toolString("Long task"), Status: toolString("in_progress")}})
	waitFor(t, time.Second, func() bool { return len(client.getSent()) == 1 })
	reporter.Event(acp.Event{Tool: &acp.ToolUpdate{ID: "a", Status: toolString("completed")}})
	reporter.Event(acp.Event{Tool: &acp.ToolUpdate{ID: "a", Status: toolString("pending")}})
	reporter.Finish(false)
	stream.Finish(lxmf.StatusCompleted, "", "")
	if err := stream.Wait(); err != nil {
		t.Fatal(err)
	}
	sent := client.getSent()
	if len(sent) != 2 || !strings.Contains(sent[0].Content, "in_progress") || !strings.Contains(sent[1].Content, "completed") {
		t.Fatalf("unexpected statuses: %+v", sent)
	}
}

func TestToolModesAndUnfinishedCalls(t *testing.T) {
	for _, mode := range []string{"off", "compact", "verbose"} {
		t.Run(mode, func(t *testing.T) {
			reporter, stream, client, store := reporterFixture(t, mode, time.Hour)
			reporter.Event(acp.Event{Tool: &acp.ToolUpdate{ID: "a", Title: toolString("Execute"), Input: toolString("args"), Content: toolString("partial output")}})
			reporter.Finish(true)
			stream.Write("Stopped.")
			stream.Finish(lxmf.StatusCancelled, "", "")
			if err := stream.Wait(); err != nil {
				t.Fatal(err)
			}
			record, _ := store.GetTool(stream.chatKey, 1)
			if record.Status != "cancelled" {
				t.Fatalf("unfinished call: %+v", record)
			}
			sent := client.getSent()
			if mode == "off" {
				if len(sent) != 1 || sent[0].Content != "Stopped." {
					t.Fatalf("off sent tool activity: %+v", sent)
				}
			} else {
				if len(sent) != 2 || !strings.Contains(sent[0].Content, "cancelled") {
					t.Fatalf("missing tool activity: %+v", sent)
				}
				if strings.Contains(sent[0].Content, "Input: args") != (mode == "verbose") {
					t.Fatal("wrong verbosity")
				}
			}
		})
	}
}

type eventACP struct {
	*mockACP
	fn func(func(acp.Event)) (string, error)
}

func (m *eventACP) PromptEvents(_ context.Context, _, _ string, emit func(acp.Event)) (string, error) {
	return m.fn(emit)
}

func TestToolCommandsAndPromptIntegration(t *testing.T) {
	b, client, mock, store, identity, peer := setupTestBridge(t)
	defer b.Close()
	local := identity.DestinationHashFor(reticulumLxmf.FullName())
	q := &peerQueue{localHex: hex.EncodeToString(local), peerHex: hex.EncodeToString(peer), localHash: local, peerHash: peer, bridge: b}
	b.acpClient = &eventACP{mockACP: mock, fn: func(emit func(acp.Event)) (string, error) {
		emit(acp.Event{Tool: &acp.ToolUpdate{ID: "a", Title: toolString("Read file"), Output: toolString("full output")}})
		emit(acp.Event{Tool: &acp.ToolUpdate{ID: "a", Status: toolString("completed")}})
		emit(acp.Event{Text: "Answer"})
		return "end_turn", nil
	}}
	q.processMessage(&lxmf.InboundMessage{LocalHash: local, SenderHash: peer, Content: "go"})
	q.processMessage(&lxmf.InboundMessage{Content: "/bridge tool 1"})
	q.processMessage(&lxmf.InboundMessage{Content: "/bridge tools off"})
	key := q.localHex + ":" + q.peerHex
	if store.ToolMode(key) != "off" {
		t.Fatal("preference not saved")
	}
	sent := client.getSent()
	if len(sent) != 4 || !strings.Contains(sent[2].Content, "Output:\nfull output") || sent[3].Content != "Tool display: off" {
		t.Fatalf("command integration: %+v", sent)
	}
	// A second chat cannot access the first chat's tool record.
	q.peerHex = "other"
	q.processMessage(&lxmf.InboundMessage{Content: "/bridge tool 1"})
	sent = client.getSent()
	if !strings.Contains(sent[len(sent)-1].Content, "Tool not found") {
		t.Fatal("details leaked across chats")
	}
}

func TestToolDeliveryFailureDoesNotStopFollowingAnswer(t *testing.T) {
	reporter, stream, client, _ := reporterFixture(t, "compact", time.Hour)
	client.sendHook = func(sent sentMsg) error {
		if sent.Title == "Tools" {
			return errors.New("no receipt")
		}
		return nil
	}
	reporter.Event(acp.Event{Tool: &acp.ToolUpdate{ID: "a", Status: toolString("completed")}})
	reporter.Event(acp.Event{Text: "Answer"})
	reporter.Finish(false)
	stream.Finish(lxmf.StatusCompleted, "", "")
	if err := stream.Wait(); err != nil {
		t.Fatal("optional notice blocked answer", err)
	}
	sent := client.getSent()
	if len(sent) != 1 || sent[0].Content != "Answer" {
		t.Fatal("answer lost", sent)
	}
}
