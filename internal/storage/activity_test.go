package storage

import (
	"path/filepath"
	"testing"
	"time"
)

func TestChatActivityPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-72 * time.Hour).Truncate(time.Second)
	if err := s.SaveChat(ChatRecord{Hash: "chat", Owner: "owner", PrivateKey: "key", CreatedAt: old}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveChat(ChatRecord{Hash: "abandoned", CreatedAt: old}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	if err := s.TouchChat("chat", now); err != nil {
		t.Fatal(err)
	}
	if err := s.TouchChat("chat", old); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := s.GetChat("chat")
	if !rec.LastActivityAt.Equal(now) || rec.PrivateKey != "key" || rec.Owner != "owner" {
		t.Fatal("activity update changed durable identity or went backwards")
	}
}
