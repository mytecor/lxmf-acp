package bridge

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/mytecor/lxmf-acp/internal/storage"
)

type SessionManager struct {
	store     *storage.Store
	acpClient ACPClient
	mu        sync.Mutex
}

func NewSessionManager(store *storage.Store, acpClient ACPClient) *SessionManager {
	return &SessionManager{
		store:     store,
		acpClient: acpClient,
	}
}

// GetOrCreateSession retrieves the active ACP session ID for (localDestination, peerDestination) or initiates a new one.
func (sm *SessionManager) GetOrCreateSession(ctx context.Context, localHex, peerHex string) (string, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sessionID, found := sm.store.GetSession(localHex, peerHex)
	if found {
		// If agent supports loading, attempt to verify/load
		if sm.acpClient.CanLoadSession() {
			if err := sm.acpClient.LoadSession(ctx, sessionID); err != nil {
				log.Printf("[Bridge] Session %s for (%s, %s) failed to load (%v), creating new session...", sessionID, localHex, peerHex, err)
				// Create new session below
			} else {
				return sessionID, nil
			}
		} else {
			// Agent doesn't require or support loadSession, use sessionID directly
			return sessionID, nil
		}
	}

	// Create new ACP session
	newID, err := sm.acpClient.CreateSession(ctx)
	if err != nil {
		return "", fmt.Errorf("create acp session: %w", err)
	}

	if err := sm.store.SetSession(localHex, peerHex, newID); err != nil {
		log.Printf("[Bridge] Warning: failed to save session to storage: %v", err)
	}

	return newID, nil
}

// ResetSession discards the existing session for (localDestination, peerDestination) and creates a fresh one.
func (sm *SessionManager) ResetSession(ctx context.Context, localHex, peerHex string) (string, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	newID, err := sm.acpClient.CreateSession(ctx)
	if err != nil {
		return "", fmt.Errorf("create acp session: %w", err)
	}

	if err := sm.store.SetSession(localHex, peerHex, newID); err != nil {
		return "", fmt.Errorf("save state: %w", err)
	}

	log.Printf("[Bridge] Reset session for (%s, %s) -> %s", localHex, peerHex, newID)
	return newID, nil
}
