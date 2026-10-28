package bridge

import (
	"testing"
	"time"
)

func TestMessageHistory(t *testing.T) {
	h := NewMessageHistory(3)

	id1 := []byte{0x01, 0x02, 0x03}
	id2 := []byte{0x04, 0x05, 0x06}
	id3 := []byte{0x07, 0x08, 0x09}
	id4 := []byte{0x0A, 0x0B, 0x0C}

	h.Record(id1, "chat1", "assistant", "msg 1")
	h.Record(id2, "chat1", "user", "msg 2")
	h.Record(id3, "chat1", "assistant", "msg 3")

	// Verify all 3 present
	if e, ok := h.Get(id1); !ok || e.Content != "msg 1" || e.Role != "assistant" {
		t.Fatalf("expected id1 found, got %+v, %v", e, ok)
	}
	if e, ok := h.Get(id2); !ok || e.Content != "msg 2" || e.Role != "user" {
		t.Fatalf("expected id2 found, got %+v, %v", e, ok)
	}
	if e, ok := h.Get(id3); !ok || e.Content != "msg 3" || e.Role != "assistant" {
		t.Fatalf("expected id3 found, got %+v, %v", e, ok)
	}

	// Record 4th -> id1 should be evicted (capacity 3)
	h.Record(id4, "chat1", "user", "msg 4")

	if _, ok := h.Get(id1); ok {
		t.Fatalf("expected id1 evicted")
	}
	if e, ok := h.Get(id4); !ok || e.Content != "msg 4" {
		t.Fatalf("expected id4 found")
	}

	// Updating existing ID
	h.RecordHex("040506", "chat1", "user", "updated msg 2", time.Now())
	if e, ok := h.Get(id2); !ok || e.Content != "updated msg 2" {
		t.Fatalf("expected id2 updated, got %+v", e)
	}
}
