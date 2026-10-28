package storage

import (
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
	if err := s.SaveMessage(MessageEntry{ID: "message", ChatKey: "chat:owner", Timestamp: recent}); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path)
	if err != nil {
		t.Fatal(err)
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
