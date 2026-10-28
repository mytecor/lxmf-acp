package acp

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"testing"

	acpwire "github.com/ironpark/acp-go"
	"github.com/ironpark/acp-go/acp1"
	schema "github.com/ironpark/acp-go/schema/v1"
)

type detachTestAgent struct {
	eventTestAgent
	calls chan string
	err   error
}

func (a *detachTestAgent) ServeExtMethod(_ context.Context, method string, params jsontext.Value) (any, error) {
	a.calls <- method + ":" + string(params)
	return map[string]any{}, a.err
}

func TestHydraIdleDetachUsesExtensionWithoutClosingConnection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rpcErr  error
		wantErr bool
	}{
		{"hydra", nil, false},
		{"hydra-acp", nil, false},
		{"other-agent", nil, false},
		{"hydra", &acpwire.RequestError{Code: -32001, Message: "already gone"}, false},
		{"hydra", acpwire.MethodNotFound("session/detach"), false},
		{"hydra", errors.New("temporary failure"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agent := &detachTestAgent{calls: make(chan string, 1), err: tc.rpcErr}
			c := NewClient("")
			_, remote := acp1.Pipe(t.Context(), func(*acp1.AgentSideConnection) acp1.Agent { return agent }, func(*acp1.ClientSideConnection) acp1.Client { return &clientHandler{client: c} })
			defer remote.Close()
			c.agent = &acp1.RemoteAgent{ClientSideConnection: remote}
			c.initialized = &acp1.InitializeResponse{AgentInfo: &schema.Implementation{Name: tc.name}}
			err := c.DetachSession(t.Context(), "saved-session")
			if (err != nil) != tc.wantErr {
				t.Fatal(err)
			}
			if tc.name == "other-agent" {
				if len(agent.calls) != 0 {
					t.Fatal("sent Hydra extension to another agent")
				}
			} else if call := <-agent.calls; call != `session/detach:{"sessionId":"saved-session"}` {
				t.Fatal(call)
			}
			select {
			case <-remote.Done():
				t.Fatal("detach closed the shared ACP connection")
			default:
			}
		})
	}
}
