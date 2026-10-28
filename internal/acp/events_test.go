package acp

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ironpark/acp-go/acp1"
	schema "github.com/ironpark/acp-go/schema/v1"
)

func TestToolEventsPreservePartialFieldsAndContentReplacement(t *testing.T) {
	title := "Read config"
	event, ok := eventFromUpdate(schema.NewSessionUpdate(schema.SessionUpdateToolCall{
		ToolCallID: "call1", Title: title,
		Content: []schema.ToolCallContent{acp1.ToolText("first")},
	}))
	if !ok || event.Tool == nil || *event.Tool.Title != title || *event.Tool.Content != "first" {
		t.Fatalf("initial event: %+v", event)
	}
	status := schema.ToolCallStatusCompleted
	event, ok = eventFromUpdate(schema.NewSessionUpdate(schema.SessionUpdateToolCallUpdate{ToolCallID: "call1", Status: &status}))
	if !ok || event.Tool.Title != nil || event.Tool.Content != nil || *event.Tool.Status != "completed" {
		t.Fatalf("partial event: %+v", event.Tool)
	}
	event, _ = eventFromUpdate(schema.NewSessionUpdate(schema.SessionUpdateToolCallUpdate{ToolCallID: "call1", Content: []schema.ToolCallContent{}}))
	if event.Tool.Content == nil || *event.Tool.Content != "" {
		t.Fatal("empty content must replace earlier content")
	}
}

func TestToolContentHandlesDiffAndTerminal(t *testing.T) {
	old := "before"
	content := toolContent([]schema.ToolCallContent{
		schema.NewToolCallContent(schema.ToolCallContentDiff{Path: "/config", OldText: &old, NewText: "after"}),
		schema.NewToolCallContent(schema.ToolCallContentTerminal{TerminalID: "term1"}),
	})
	for _, want := range []string{"File: /config", "Before:\nbefore", "After:\nafter", "Terminal: term1"} {
		if !strings.Contains(*content, want) {
			t.Fatalf("missing %q in %q", want, *content)
		}
	}
}

func TestToolPayloadBoundIsUTF8Safe(t *testing.T) {
	value := BoundedToolText(strings.Repeat("界😀", 20000))
	if len(value) > MaxToolDetailBytes || !utf8.ValidString(value) || !strings.HasSuffix(value, "[truncated]") {
		t.Fatalf("invalid bounded payload: bytes=%d", len(value))
	}
}

func TestEventTextAndIgnoredUpdates(t *testing.T) {
	event, ok := eventFromUpdate(schema.NewSessionUpdate(schema.SessionUpdateAgentMessageChunk{Content: acp1.TextBlock("hello")}))
	if !ok || event.Text != "hello" || event.Tool != nil {
		t.Fatal("text event lost")
	}
	if _, ok := eventFromUpdate(schema.NewSessionUpdate(schema.SessionUpdateToolCall{Title: "missing ID"})); ok {
		t.Fatal("accepted a call without ID")
	}
}

// Exercise the actual ACP transport/session routing, including simultaneous
// sessions whose agents reuse the same tool ID.
type eventTestAgent struct {
	client *acp1.AgentSideConnection
}

func (*eventTestAgent) Initialize(context.Context, *acp1.InitializeRequest) (*acp1.InitializeResponse, error) {
	return &acp1.InitializeResponse{ProtocolVersion: acp1.ProtocolVersion}, nil
}
func (*eventTestAgent) NewSession(context.Context, *acp1.NewSessionRequest) (*acp1.NewSessionResponse, error) {
	return &acp1.NewSessionResponse{SessionID: acp1.GenerateSessionID()}, nil
}
func (*eventTestAgent) CancelSession(context.Context, *acp1.CancelNotification) error { return nil }

func (a *eventTestAgent) Prompt(ctx context.Context, params *acp1.PromptRequest) (*acp1.PromptResponse, error) {
	stream := acp1.NewSessionStream(a.client, params.SessionID)
	if err := stream.SendText(ctx, "before:"+string(params.SessionID)); err != nil {
		return nil, err
	}
	if err := stream.StartToolCall(ctx, "shared-id", string(params.SessionID), schema.ToolKindRead); err != nil {
		return nil, err
	}
	status := schema.ToolCallStatusCompleted
	if err := stream.Send(ctx, schema.SessionUpdateToolCallUpdate{ToolCallID: "shared-id", Status: &status}); err != nil {
		return nil, err
	}
	if err := stream.SendText(ctx, "after"); err != nil {
		return nil, err
	}
	return &acp1.PromptResponse{StopReason: schema.StopReasonEndTurn}, nil
}

func TestPromptEventsRoutesActualACPUpdatesPerSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	c := NewClient("")
	agent := &eventTestAgent{}
	_, remote := acp1.Pipe(ctx, func(conn *acp1.AgentSideConnection) acp1.Agent {
		agent.client = conn
		return agent
	}, func(*acp1.ClientSideConnection) acp1.Client { return &clientHandler{client: c} })
	c.agent = &acp1.RemoteAgent{ClientSideConnection: remote}
	defer remote.Close()
	var wg sync.WaitGroup
	for _, id := range []string{"session-a", "session-b"} {
		wg.Go(func() {
			var events []Event
			reason, err := c.PromptEvents(ctx, id, "go", func(e Event) { events = append(events, e) })
			if err != nil || reason != "end_turn" {
				t.Errorf("prompt: %s %v", reason, err)
				return
			}
			if len(events) != 4 || events[0].Text != "before:"+id || events[3].Text != "after" || events[1].Tool == nil || events[2].Tool == nil {
				t.Errorf("events for %s: %+v", id, events)
				return
			}
			if *events[1].Tool.Title != id || *events[2].Tool.Status != "completed" {
				t.Errorf("tool crossed sessions: %+v", events)
			}
		})
	}
	wg.Wait()
}
