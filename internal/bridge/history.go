package bridge

import (
	"encoding/hex"
	"sync"
	"time"
)

// HistoryEntry is a recorded inbound or outbound message in a conversation.
type HistoryEntry struct {
	ID        string    // hex string of MessageID
	ChatKey   string    // localHex:peerHex
	Role      string    // "user" or "assistant"
	Content   string    // message text
	Timestamp time.Time
}

// MessageHistory maintains an in-memory bounded ring buffer of recent messages
// indexed by their message ID (hex) for fast reply resolution.
type MessageHistory struct {
	mu       sync.RWMutex
	maxItems int
	order    []string                // message IDs in insertion order
	entries  map[string]HistoryEntry // idHex -> HistoryEntry
}

// NewMessageHistory creates a new MessageHistory cache with maxItems capacity.
func NewMessageHistory(maxItems int) *MessageHistory {
	if maxItems <= 0 {
		maxItems = 1000
	}
	return &MessageHistory{
		maxItems: maxItems,
		entries:  make(map[string]HistoryEntry),
		order:    make([]string, 0, maxItems),
	}
}

// Record saves a message using raw byte slice ID.
func (h *MessageHistory) Record(id []byte, chatKey, role, content string) {
	if len(id) == 0 || content == "" {
		return
	}
	h.RecordHex(hex.EncodeToString(id), chatKey, role, content, time.Now())
}

// RecordHex saves a message with hex ID and specific timestamp.
func (h *MessageHistory) RecordHex(idHex, chatKey, role, content string, ts time.Time) {
	if idHex == "" || content == "" {
		return
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.entries[idHex]; exists {
		h.entries[idHex] = HistoryEntry{
			ID:        idHex,
			ChatKey:   chatKey,
			Role:      role,
			Content:   content,
			Timestamp: ts,
		}
		return
	}

	if len(h.order) >= h.maxItems {
		oldest := h.order[0]
		h.order = h.order[1:]
		delete(h.entries, oldest)
	}

	h.entries[idHex] = HistoryEntry{
		ID:        idHex,
		ChatKey:   chatKey,
		Role:      role,
		Content:   content,
		Timestamp: ts,
	}
	h.order = append(h.order, idHex)
}

// Get looks up a message by its raw byte slice ID.
func (h *MessageHistory) Get(id []byte) (HistoryEntry, bool) {
	if len(id) == 0 {
		return HistoryEntry{}, false
	}
	return h.GetByHex(hex.EncodeToString(id))
}

// GetByHex looks up a message by its hex string ID.
func (h *MessageHistory) GetByHex(idHex string) (HistoryEntry, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	entry, ok := h.entries[idHex]
	return entry, ok
}
