package bridge

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	acpwire "github.com/ironpark/acp-go"
	"github.com/mytecor/lxmf-acp/internal/config"
	"github.com/mytecor/lxmf-acp/internal/lxmf"
	"github.com/mytecor/lxmf-acp/internal/storage"
)

type lifecycleACP struct {
	*mockACP
	detach  func(context.Context, string) error
	loadErr error
}

func (m *lifecycleACP) DetachSession(ctx context.Context, id string) error {
	if m.detach != nil {
		return m.detach(ctx, id)
	}
	return nil
}

func (m *lifecycleACP) LoadSession(ctx context.Context, id string) error {
	if m.loadErr != nil {
		return m.loadErr
	}
	return m.mockACP.LoadSession(ctx, id)
}

func lifecycleBridge(t *testing.T, agent ACPClient, timeout time.Duration) *Bridge {
	t.Helper()
	store, err := storage.NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	b := NewBridge(&config.Config{QueueIdleTimeout: timeout}, newMockLXMF(nil), agent, store)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func workerCount(b *Bridge) int {
	b.queue.mu.Lock()
	defer b.queue.mu.Unlock()
	return len(b.queue.workers)
}

func TestIdleQueueDetachesAndResumesSameSession(t *testing.T) {
	detached := make(chan string, 10)
	agent := &lifecycleACP{mockACP: newMockACP(true), detach: func(_ context.Context, id string) error { detached <- id; return nil }}
	b := lifecycleBridge(t, agent, 30*time.Millisecond)
	msg := &lxmf.InboundMessage{LocalHash: []byte{1}, SenderHash: []byte{2}, Content: "first"}
	b.queue.Enqueue(msg)
	waitFor(t, time.Second, func() bool { return workerCount(b) == 0 && len(agent.getPrompts()) == 1 })
	select {
	case id := <-detached:
		if id != "session-1" {
			t.Fatal(id)
		}
	default:
		t.Fatal("idle session was not detached")
	}
	if id, ok := b.store.GetSession("01", "02"); !ok || id != "session-1" {
		t.Fatal("durable session mapping lost")
	}
	msg2 := *msg
	msg2.Content = "second"
	b.queue.Enqueue(&msg2)
	waitFor(t, time.Second, func() bool { return len(agent.getPrompts()) == 2 })
	if len(agent.getCreated()) != 1 || len(agent.getLoaded()) != 1 {
		t.Fatal("did not resume the original session")
	}
}

func TestMessageArrivingDuringDetachStaysOrdered(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	agent := &lifecycleACP{mockACP: newMockACP(true), detach: func(ctx context.Context, _ string) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	b := lifecycleBridge(t, agent, 20*time.Millisecond)
	b.queue.Enqueue(&lxmf.InboundMessage{LocalHash: []byte{1}, SenderHash: []byte{2}, Content: "first"})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("detach did not start")
	}
	b.queue.Enqueue(&lxmf.InboundMessage{LocalHash: []byte{1}, SenderHash: []byte{2}, Content: "second"})
	if workerCount(b) != 1 || len(agent.getPrompts()) != 1 {
		t.Fatal("concurrent worker escaped detach ordering")
	}
	close(release)
	waitFor(t, time.Second, func() bool { return len(agent.getPrompts()) == 2 })
	if len(agent.getCreated()) != 1 {
		t.Fatal("a new session replaced the old one")
	}
}

func TestActivePromptAndDeliveryDoNotExpire(t *testing.T) {
	for _, phase := range []string{"prompt", "delivery"} {
		t.Run(phase, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			agent := &lifecycleACP{mockACP: newMockACP(true)}
			b := lifecycleBridge(t, agent, 20*time.Millisecond)
			if phase == "prompt" {
				agent.streamFn = func(ctx context.Context, _, _ string, chunk func(string)) (string, error) {
					close(entered)
					select {
					case <-release:
						chunk("done")
						return "end_turn", nil
					case <-ctx.Done():
						return "", ctx.Err()
					}
				}
			} else {
				b.lxmfClient.(*mockLXMF).sendHook = func(sentMsg) error { close(entered); <-release; return nil }
			}
			b.queue.Enqueue(&lxmf.InboundMessage{LocalHash: []byte{1}, SenderHash: []byte{2}, Content: "work"})
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("work did not start")
			}
			time.Sleep(60 * time.Millisecond)
			if workerCount(b) != 1 {
				t.Fatal("active work expired")
			}
			close(release)
			waitFor(t, time.Second, func() bool { return workerCount(b) == 0 })
		})
	}
}

func TestQueueShutdownCancelsActivePromptAndRejectsNewWork(t *testing.T) {
	entered := make(chan struct{})
	agent := newMockACP(true)
	agent.streamFn = func(ctx context.Context, _, _ string, _ func(string)) (string, error) {
		close(entered)
		<-ctx.Done()
		return "", ctx.Err()
	}
	b := lifecycleBridge(t, agent, 0)
	msg := &lxmf.InboundMessage{LocalHash: []byte{1}, SenderHash: []byte{2}, Content: "work"}
	b.queue.Enqueue(msg)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("work did not start")
	}
	b.queue.Close()
	waitFor(t, time.Second, func() bool { return workerCount(b) == 0 })
	b.queue.Enqueue(msg)
	if workerCount(b) != 0 {
		t.Fatal("closed queue accepted new work")
	}
}

func TestResumeFailureDoesNotDiscardConversation(t *testing.T) {
	agent := &lifecycleACP{mockACP: newMockACP(true), loadErr: errors.New("agent temporarily unavailable")}
	b := lifecycleBridge(t, agent, 0)
	if err := b.store.SetSession("01", "02", "saved"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.sessions.GetOrCreateSession(t.Context(), "01", "02"); err == nil {
		t.Fatal("expected load error")
	}
	if len(agent.getCreated()) != 0 {
		t.Fatal("transient failure created a blank conversation")
	}
	agent.loadErr = &acpwire.RequestError{Code: -32001, Message: "session not found"}
	if _, err := b.sessions.GetOrCreateSession(t.Context(), "01", "02"); err != nil {
		t.Fatal(err)
	}
	if len(agent.getCreated()) != 1 {
		t.Fatal("missing session did not get recreated")
	}
}

func TestHydraCommandsPassThroughAndBridgeNamespaceIsLocal(t *testing.T) {
	agent := newMockACP(true)
	b := lifecycleBridge(t, agent, 0)
	q := &peerQueue{localHex: "01", peerHex: "02", localHash: []byte{1}, peerHash: []byte{2}, bridge: b}
	for _, tc := range []struct{ input, want string }{
		{"/hydra", "/hydra"},
		{"/hydra kill", "/hydra kill"},
		{"/hydra restart", "/hydra restart"},
		{"/hydra compact status", "/hydra compact status"},
		{"/new", "/new"},
		{"/help", "/help"},
		{"/cancel", "/cancel"},
		{"/tools off", "/tools off"},
		{"/tool 1", "/tool 1"},
		{"/allow deadbeef", "/allow deadbeef"},
		{"/revoke deadbeef", "/revoke deadbeef"},
		{"/whitelist", "/whitelist"},
		{"/allowlist", "/allowlist"},
		{"/bridge-tools", "/bridge-tools"},
		{"/allowance test", "/allowance test"},
		{"/revoke-token test", "/revoke-token test"},
		{"/bridge forward /new", "/new"},
		{"/bridge forward /tools off", "/tools off"},
		{"/bridge forward /bridge help", "/bridge help"},
	} {
		before := len(agent.getPrompts())
		q.processMessage(&lxmf.InboundMessage{Content: tc.input, ReplyQuote: "quoted earlier message"})
		prompts := agent.getPrompts()
		if len(prompts) != before+1 || prompts[len(prompts)-1] != tc.want {
			t.Fatalf("%q routed as %v", tc.input, prompts)
		}
	}
	if b.store.ToolMode("01:02") != "compact" {
		t.Fatal("forwarded tool commands changed local preferences")
	}
	count := len(agent.getPrompts())
	for _, command := range []string{"/bridge", "/bridge help", "/bridge tools off", "/bridge unknown", "/bridge forward", "/bridge help extra", "/bridge new extra", "/bridge cancel extra", "/bridge whitelist extra", "/bridge allow", "/bridge revoke", "/bridge cancel", "/bridge whitelist", "/bridge allowlist"} {
		q.processMessage(&lxmf.InboundMessage{Content: command})
	}
	if len(agent.getPrompts()) != count {
		t.Fatal("bridge commands leaked upstream")
	}
	if b.store.ToolMode("01:02") != "off" {
		t.Fatal("namespaced tool command did not run locally")
	}
}
