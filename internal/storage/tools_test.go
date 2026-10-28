package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestToolPreferencesHistoryAndChatIsolationSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.ToolMode("a:b") != "compact" {
		t.Fatal("wrong default")
	}
	if err := s.SetToolMode("a:b", "verbose"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetToolMode("a:b", "invalid"); err == nil {
		t.Fatal("invalid mode accepted")
	}
	for i := 1; i <= MaxRecentTools+2; i++ {
		rec, err := s.AddTool("a:b", ToolRecord{TurnID: fmt.Sprint(i), ID: "reused-id", Title: "read", Status: "pending"})
		if err != nil || rec.Number != i {
			t.Fatalf("allocation: %+v %v", rec, err)
		}
	}
	rec, _ := s.GetTool("a:b", 52)
	rec.Status = "completed"
	rec.Output = "done"
	if err := s.UpdateTools("a:b", []ToolRecord{rec, {Number: 1, TurnID: "1", Status: "completed"}}); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.ToolMode("a:b") != "verbose" || s.ToolMode("a:c") != "compact" {
		t.Fatal("mode leaked or lost")
	}
	if _, ok := s.GetTool("a:c", 52); ok {
		t.Fatal("cross-chat tool access")
	}
	if _, ok := s.GetTool("a:b", 1); ok {
		t.Fatal("evicted tool reinserted")
	}
	rec, ok := s.GetTool("a:b", 52)
	if !ok || rec.Status != "completed" || rec.Output != "done" {
		t.Fatalf("record lost: %+v", rec)
	}
	next, err := s.AddTool("a:b", ToolRecord{ID: "next"})
	if err != nil || next.Number != 53 {
		t.Fatal("number reused after restart")
	}
}
