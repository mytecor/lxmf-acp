package acp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	acpwire "github.com/ironpark/acp-go"
	"github.com/ironpark/acp-go/acp1"
	"github.com/ironpark/acp-go/acphttp"
	schema "github.com/ironpark/acp-go/schema/v1"
)

type Client struct {
	endpoint string
	cwd      string

	mu          sync.RWMutex
	agent       *acp1.RemoteAgent
	initialized *acp1.InitializeResponse

	// Active prompts for cancellation
	activeMu   sync.Mutex
	activeTurn map[string]context.CancelFunc
}

func NewClient(endpoint string) *Client {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "/"
	}
	return &Client{
		endpoint:   endpoint,
		cwd:        cwd,
		activeTurn: make(map[string]context.CancelFunc),
	}
}

// clientHandler implements acp1.Client interface
type clientHandler struct {
	acp1.UnimplementedClient
	client *Client
}

func (h *clientHandler) SessionUpdate(_ context.Context, params *acp1.SessionNotification) error {
	// Debug logging of session updates if necessary
	return nil
}

func (h *clientHandler) RequestPermission(_ context.Context, params *acp1.RequestPermissionRequest) (*acp1.RequestPermissionResponse, error) {
	log.Printf("[ACP] Permission requested by agent: %v (defaulting to safe cancellation)", params.ToolCall.Title)
	// Safe default: cancel or reject
	return acp1.PermissionCancelled(), nil
}

func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.agent != nil {
		select {
		case <-c.agent.Done():
		default:
			return nil
		}
	}

	log.Printf("[ACP] Dialing WebSocket to agent endpoint: %s", c.endpoint)
	wsTransport, err := acphttp.DialWebSocket(ctx, c.endpoint)
	if err != nil {
		return fmt.Errorf("dial acp websocket: %w", err)
	}

	handler := &clientHandler{client: c}
	agent := acp1.ConnectAgent(ctx, wsTransport, func(*acp1.ClientSideConnection) acp1.Client {
		return handler
	})

	initReq := &acp1.InitializeRequest{
		ProtocolVersion: acp1.ProtocolVersion,
		ClientInfo: &schema.Implementation{
			Name:    "lxmf-acp",
			Version: "0.1.0",
		},
	}

	initResp, err := agent.Initialize(ctx, initReq)
	if err != nil {
		_ = agent.Close()
		return fmt.Errorf("acp initialize: %w", err)
	}

	c.agent = agent
	c.initialized = initResp
	log.Printf("[ACP] Connected and initialized successfully (agent protocol v%d)", initResp.ProtocolVersion)
	return nil
}

func (c *Client) EnsureConnected(ctx context.Context) error {
	c.mu.RLock()
	if c.agent != nil {
		select {
		case <-c.agent.Done():
		default:
			c.mu.RUnlock()
			return nil
		}
	}
	c.mu.RUnlock()

	return c.Connect(ctx)
}

// DetachSession releases only our Hydra subscription. The daemon owns agent
// idle shutdown and persistent history; other ACP agents need no extension.
func (c *Client) DetachSession(ctx context.Context, sessionID string) error {
	c.mu.RLock()
	agent := c.agent
	info := c.initialized.GetAgentInfo()
	c.mu.RUnlock()
	if agent == nil || info == nil || (info.Name != "hydra" && info.Name != "hydra-acp") {
		return nil
	}
	select {
	case <-agent.Done():
		return nil
	default:
	}
	_, err := agent.ExtMethod(ctx, "session/detach", map[string]string{"sessionId": sessionID})
	var rpcErr *acpwire.RequestError
	// Hydra may already have closed this session on its own idle timer.
	if errors.As(err, &rpcErr) && (rpcErr.Code == -32001 || rpcErr.Code == acpwire.ErrorCodeMethodNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("detach session %s: %w", sessionID, err)
	}
	log.Printf("[ACP] Detached idle session %s", sessionID)
	return nil
}

func (c *Client) CreateSession(ctx context.Context) (string, error) {
	if err := c.EnsureConnected(ctx); err != nil {
		return "", err
	}

	c.mu.RLock()
	agent := c.agent
	c.mu.RUnlock()

	if agent == nil {
		return "", errors.New("agent not connected")
	}

	session, err := agent.StartSession(ctx, &acp1.NewSessionRequest{
		Cwd: c.cwd,
	})
	if err != nil {
		return "", fmt.Errorf("create session: %w", err)
	}

	log.Printf("[ACP] Created new session ID: %s", session.ID)
	return string(session.ID), nil
}

func (c *Client) CanLoadSession() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.initialized == nil {
		return false
	}
	caps := c.initialized.GetAgentCapabilities()
	if caps == nil {
		return false
	}
	return caps.GetLoadSession()
}

func (c *Client) LoadSession(ctx context.Context, sessionID string) error {
	if err := c.EnsureConnected(ctx); err != nil {
		return err
	}

	c.mu.RLock()
	agent := c.agent
	c.mu.RUnlock()

	if agent == nil {
		return errors.New("agent not connected")
	}

	if !c.CanLoadSession() {
		return errors.New("agent does not support session/load")
	}

	_, err := agent.LoadSession(ctx, &acp1.LoadSessionRequest{
		SessionID: schema.SessionID(sessionID),
		Cwd:       c.cwd,
	})
	if err != nil {
		return fmt.Errorf("load session %s: %w", sessionID, err)
	}

	log.Printf("[ACP] Successfully loaded session ID: %s", sessionID)
	return nil
}

func (c *Client) Prompt(ctx context.Context, sessionID, text string) (string, error) {
	var b strings.Builder
	_, err := c.PromptStream(ctx, sessionID, text, func(chunk string) {
		b.WriteString(chunk)
	})
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

// StopReasonCancelled is the ACP stop reason reported for a cancelled turn.
const StopReasonCancelled = string(schema.StopReasonCancelled)

// PromptStream sends a prompt and invokes onChunk with the text of every
// agent_message_chunk as it arrives. It returns the turn's stop reason once
// the turn has ended.
func (c *Client) PromptStream(ctx context.Context, sessionID, text string, onChunk func(string)) (string, error) {
	return c.PromptEvents(ctx, sessionID, text, func(event Event) {
		if event.Text != "" && onChunk != nil {
			onChunk(event.Text)
		}
	})
}

// PromptEvents emits ordered text and tool events from this prompt only.
func (c *Client) PromptEvents(ctx context.Context, sessionID, text string, onEvent func(Event)) (string, error) {
	if err := c.EnsureConnected(ctx); err != nil {
		return "", err
	}

	c.mu.RLock()
	agent := c.agent
	c.mu.RUnlock()

	if agent == nil {
		return "", errors.New("agent not connected")
	}

	session := agent.Session(schema.SessionID(sessionID))

	promptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.activeMu.Lock()
	c.activeTurn[sessionID] = cancel
	c.activeMu.Unlock()

	defer func() {
		c.activeMu.Lock()
		delete(c.activeTurn, sessionID)
		c.activeMu.Unlock()
	}()

	turn, err := session.Prompt(promptCtx, acp1.TextBlock(text))
	if err != nil {
		return "", fmt.Errorf("prompt session %s: %w", sessionID, err)
	}

	for update := range turn.Updates() {
		if event, ok := eventFromUpdate(update); ok && onEvent != nil {
			onEvent(event)
		}
	}

	result, err := turn.Wait()
	if err != nil {
		if errors.Is(promptCtx.Err(), context.Canceled) {
			return "", context.Canceled
		}
		return "", fmt.Errorf("turn text: %w", err)
	}

	stopReason := ""
	if result != nil {
		stopReason = string(result.StopReason)
	}
	if stopReason != "" {
		log.Printf("[ACP] Turn completed for session %s (stop reason: %s)", sessionID, stopReason)
	}

	return stopReason, nil
}

func (c *Client) CancelPrompt(sessionID string) bool {
	c.activeMu.Lock()
	cancel, ok := c.activeTurn[sessionID]
	if ok && cancel != nil {
		cancel()
	}
	c.activeMu.Unlock()

	// Also send ACP CancelSession notification if connected
	c.mu.RLock()
	agent := c.agent
	c.mu.RUnlock()

	if agent != nil {
		ctx, ctxCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ctxCancel()
		_ = agent.CancelSession(ctx, &acp1.CancelNotification{
			SessionID: schema.SessionID(sessionID),
		})
	}

	return ok
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.agent != nil {
		err := c.agent.Close()
		c.agent = nil
		c.initialized = nil
		return err
	}
	return nil
}
