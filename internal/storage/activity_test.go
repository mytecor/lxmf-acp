package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIdentityActivityPersistsAndMigratesOldState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-72 * time.Hour).Truncate(time.Second)
	recent := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := s.SaveIdentity(IdentityRecord{Hash: "chat", Owner: "owner", PrivateKey: "key", CreatedAt: old}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveIdentity(IdentityRecord{Hash: "abandoned", CreatedAt: old}); err != nil {
		t.Fatal(err)
	}
	// Simulate a legacy state file containing message history.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var legacy map[string]any
	if err := json.Unmarshal(raw, &legacy); err != nil {
		t.Fatal(err)
	}
	legacy["messages"] = map[string]any{
		"message": map[string]any{"chatKey": "chat:owner", "timestamp": recent, "content": "old private text"},
	}
	raw, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var migrated map[string]json.RawMessage
	if err := json.Unmarshal(raw, &migrated); err != nil {
		t.Fatal(err)
	}
	if _, ok := migrated["messages"]; ok {
		t.Fatal("legacy messages remain on disk")
	}
	if _, err := NewStore(path); err != nil {
		t.Fatalf("migrated state cannot be reloaded: %v", err)
	}
	rec, _ := s.GetIdentity("chat")
	if !rec.LastActivityAt.Equal(recent) {
		t.Fatal("legacy activity was not recovered from history")
	}
	abandoned, _ := s.GetIdentity("abandoned")
	if !abandoned.LastActivityAt.Equal(old) {
		t.Fatal("old unused chat should remain old")
	}
	now := time.Now().Truncate(time.Second)
	if err := s.TouchIdentity("chat", now); err != nil {
		t.Fatal(err)
	}
	if err := s.TouchIdentity("chat", old); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	rec, _ = s.GetIdentity("chat")
	if !rec.LastActivityAt.Equal(now) || rec.PrivateKey != "key" || rec.Owner != "owner" {
		t.Fatal("activity update changed durable identity or went backwards")
	}
}
