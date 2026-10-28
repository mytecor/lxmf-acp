package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestChatPersistenceAndSessionReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	raw := []byte(`{
 "admin":"owner", "allowlist":["owner"],
 "chats": {
   "chat": {"hash":"chat","privateKey":"secret","owner":"owner","sessionId":"current","createdAt":"2026-01-01T00:00:00Z","lastActivityAt":"2026-02-01T00:00:00Z"},
   "unused": {"hash":"unused","privateKey":"unused-key","owner":"owner","createdAt":"2026-01-01T00:00:00Z"},
   "root": {"hash":"root","owner":"admin","sessionId":"root-session","createdAt":"2026-01-01T00:00:00Z"}
 },
 "knownIdentities":{"peer":{"DestHash":"AQ==","PublicKey":"Ag=="}},
 "toolChats":{"chat:owner":{"mode":"verbose","next":3}}
 }`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	chat, ok := store.GetChat("chat")
	if !ok || chat.PrivateKey != "secret" || chat.Owner != "owner" || chat.SessionID != "current" || chat.LastActivityAt.Format(time.RFC3339) != "2026-02-01T00:00:00Z" {
		t.Fatalf("incomplete chat: %+v", chat)
	}
	if _, ok := store.GetSession("unused", "owner"); ok {
		t.Fatal("unused chat acquired a session")
	}
	if got, ok := store.GetSession("root", "admin"); !ok || got != "root-session" {
		t.Fatal("root session lost")
	}
	if store.GetAdmin() != "owner" || !store.IsAllowed("owner") || len(store.GetKnownIdentities()) != 1 || store.ToolMode("chat:owner") != "verbose" {
		t.Fatal("unrelated state changed")
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]json.RawMessage
	if err := json.Unmarshal(saved, &schema); err != nil {
		t.Fatal(err)
	}
	if schema["chats"] == nil || schema["identities"] != nil || schema["sessions"] != nil {
		t.Fatal("unexpected state schema")
	}
	if err := store.DeleteSession("chat", "owner"); err != nil {
		t.Fatal(err)
	}
	store, err = NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	chat, ok = store.GetChat("chat")
	if !ok || chat.SessionID != "" || chat.PrivateKey != "secret" || chat.Owner != "owner" {
		t.Fatal("reset lost chat or restored session")
	}
	if err := store.SetSession("chat", "stranger", "bad"); err == nil {
		t.Fatal("another peer replaced the owner")
	}
	if err := store.SetSession("chat", "owner", "new"); err != nil {
		t.Fatal(err)
	}
	if got, ok := store.GetSession("chat", "owner"); !ok || got != "new" {
		t.Fatal("new session missing")
	}
}
