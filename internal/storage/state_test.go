package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/thatSFguy/reticulum-go/rns"
)

func TestStoreOperations(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")

	store, err := NewStore(stateFile)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	local := "00112233445566778899aabbccddeeff"
	peer := "11223344556677889900aabbccddeeff"
	sessionID := "session-abc-123"

	// 1. Get non-existent
	if _, ok := store.GetSession(local, peer); ok {
		t.Errorf("expected session not to exist initially")
	}

	// 2. Set session
	if err := store.SetSession(local, peer, sessionID); err != nil {
		t.Fatalf("SetSession failed: %v", err)
	}

	// 3. Get session
	got, ok := store.GetSession(local, peer)
	if !ok || got != sessionID {
		t.Errorf("GetSession = %q, %v; want %q, true", got, ok, sessionID)
	}

	// 4. Persistence check: reload from disk
	store2, err := NewStore(stateFile)
	if err != nil {
		t.Fatalf("Reload NewStore failed: %v", err)
	}

	got2, ok2 := store2.GetSession(local, peer)
	if !ok2 || got2 != sessionID {
		t.Errorf("Reloaded GetSession = %q, %v; want %q, true", got2, ok2, sessionID)
	}

	// 5. Delete session
	if err := store.DeleteSession(local, peer); err != nil {
		t.Fatalf("DeleteSession failed: %v", err)
	}

	if _, ok := store.GetSession(local, peer); ok {
		t.Errorf("expected session to be deleted")
	}
}

func TestStoreIdentities(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")

	store, err := NewStore(stateFile)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	owner := "user1_aabbccddeeff0011223344556677"
	id1 := IdentityRecord{
		Hash:       "chat1_11223344556677889900aabbccd",
		PrivateKey: "privkey1_deadbeef001122334455667788",
		Owner:      owner,
		CreatedAt:  time.Now(),
	}
	id2 := IdentityRecord{
		Hash:       "chat2_99887766554433221100aabbccd",
		PrivateKey: "privkey2_beefdead001122334455667788",
		Owner:      owner,
		CreatedAt:  time.Now(),
	}

	if err := store.SaveIdentity(id1); err != nil {
		t.Fatalf("SaveIdentity id1 failed: %v", err)
	}
	if err := store.SaveIdentity(id2); err != nil {
		t.Fatalf("SaveIdentity id2 failed: %v", err)
	}

	if count := store.CountIdentitiesForOwner(owner); count != 2 {
		t.Errorf("CountIdentitiesForOwner = %d; want 2", count)
	}

	if count := store.CountIdentitiesForOwner("other_user"); count != 0 {
		t.Errorf("CountIdentitiesForOwner(other) = %d; want 0", count)
	}

	// Reload from disk
	store2, err := NewStore(stateFile)
	if err != nil {
		t.Fatalf("Reload NewStore failed: %v", err)
	}

	rec, ok := store2.GetIdentity(id1.Hash)
	if !ok || rec.PrivateKey != id1.PrivateKey {
		t.Errorf("GetIdentity failed or key mismatch: %+v", rec)
	}

	all := store2.GetAllIdentities()
	if len(all) != 2 {
		t.Errorf("GetAllIdentities len = %d; want 2", len(all))
	}
}

func TestStoreMessages(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")

	store, err := NewStore(stateFile)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	m1 := MessageEntry{
		ID:        "0123456789abcdef",
		ChatKey:   "local:peer",
		Role:      "assistant",
		Content:   "Hello from assistant",
		Timestamp: time.Now(),
	}

	if err := store.SaveMessage(m1); err != nil {
		t.Fatalf("SaveMessage failed: %v", err)
	}

	got, ok := store.GetMessage(m1.ID)
	if !ok || got.Content != m1.Content || got.Role != "assistant" {
		t.Fatalf("GetMessage = %+v, %v; want %+v, true", got, ok, m1)
	}

	// Reload from disk
	store2, err := NewStore(stateFile)
	if err != nil {
		t.Fatalf("Reload failed: %v", err)
	}

	got2, ok2 := store2.GetMessage(m1.ID)
	if !ok2 || got2.Content != m1.Content {
		t.Fatalf("Reload GetMessage = %+v, %v", got2, ok2)
	}

	recent := store2.GetRecentMessages()
	if len(recent) != 1 || recent[0].ID != m1.ID {
		t.Fatalf("GetRecentMessages = %+v; want 1 message", recent)
	}
}

func TestStoreKnownIdentities(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")

	store, err := NewStore(stateFile)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	destHash := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	pubKey := make([]byte, 64)
	pubKey[0] = 0x42
	k := &rns.KnownIdentity{
		DestHash:  destHash,
		PublicKey: pubKey,
		LastSeen:  time.Now(),
	}

	if err := store.SaveKnownIdentities([]*rns.KnownIdentity{k}); err != nil {
		t.Fatalf("SaveKnownIdentities failed: %v", err)
	}

	// Reload from disk
	store2, err := NewStore(stateFile)
	if err != nil {
		t.Fatalf("Reload failed: %v", err)
	}

	known := store2.GetKnownIdentities()
	if len(known) != 1 {
		t.Fatalf("GetKnownIdentities len = %d; want 1", len(known))
	}
	if string(known[0].DestHash) != string(destHash) {
		t.Fatalf("DestHash mismatch: got %x, want %x", known[0].DestHash, destHash)
	}
	if string(known[0].PublicKey) != string(pubKey) {
		t.Fatalf("PublicKey mismatch: got %x, want %x", known[0].PublicKey, pubKey)
	}
}
