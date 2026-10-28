package storage

import "fmt"

const MaxRecentTools = 50

// ToolRecord is a bounded snapshot, scoped to one local/remote conversation.
type ToolRecord struct {
	Number     int    `json:"number"`
	TurnID     string `json:"turnId"`
	ID         string `json:"id"`
	Title      string `json:"title"`
	Name       string `json:"name,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Status     string `json:"status"`
	Content    string `json:"content,omitempty"`
	Input      string `json:"input,omitempty"`
	Output     string `json:"output,omitempty"`
	DurationMS int64  `json:"durationMs,omitempty"`
}

type ToolChat struct {
	Mode   string       `json:"mode,omitempty"`
	Next   int          `json:"next"`
	Recent []ToolRecord `json:"recent,omitempty"`
}

func ValidToolMode(mode string) bool { return mode == "off" || mode == "compact" || mode == "verbose" }

func (s *Store) ToolMode(chatKey string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	mode := s.data.ToolChats[chatKey].Mode
	if !ValidToolMode(mode) {
		return "compact"
	}
	return mode
}

func (s *Store) SetToolMode(chatKey, mode string) error {
	if !ValidToolMode(mode) {
		return fmt.Errorf("invalid tool mode %q", mode)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.ToolChats == nil {
		s.data.ToolChats = make(map[string]ToolChat)
	}
	chat := s.data.ToolChats[chatKey]
	old := chat
	chat.Mode = mode
	s.data.ToolChats[chatKey] = chat
	if err := s.saveLocked(); err != nil {
		s.data.ToolChats[chatKey] = old
		return err
	}
	return nil
}

// AddTool reserves a stable, increasing display number; IDs may repeat in later turns.
func (s *Store) AddTool(chatKey string, record ToolRecord) (ToolRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.ToolChats == nil {
		s.data.ToolChats = make(map[string]ToolChat)
	}
	chat := s.data.ToolChats[chatKey]
	old := chat
	chat.Next++
	record.Number = chat.Next
	chat.Recent = append(append([]ToolRecord(nil), chat.Recent...), record)
	if len(chat.Recent) > MaxRecentTools {
		chat.Recent = chat.Recent[len(chat.Recent)-MaxRecentTools:]
	}
	s.data.ToolChats[chatKey] = chat
	if err := s.saveLocked(); err != nil {
		s.data.ToolChats[chatKey] = old
		return ToolRecord{}, err
	}
	return record, nil
}

// UpdateTools updates retained records only; evicted records are never reinserted.
func (s *Store) UpdateTools(chatKey string, records []ToolRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	chat := s.data.ToolChats[chatKey]
	old := chat
	chat.Recent = append([]ToolRecord(nil), chat.Recent...)
	for i, existing := range chat.Recent {
		for _, record := range records {
			if existing.Number == record.Number && existing.TurnID == record.TurnID {
				chat.Recent[i] = record
				break
			}
		}
	}
	if s.data.ToolChats == nil {
		s.data.ToolChats = make(map[string]ToolChat)
	}
	s.data.ToolChats[chatKey] = chat
	if err := s.saveLocked(); err != nil {
		s.data.ToolChats[chatKey] = old
		return err
	}
	return nil
}

func (s *Store) GetTool(chatKey string, number int) (ToolRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, record := range s.data.ToolChats[chatKey].Recent {
		if record.Number == number {
			return record, true
		}
	}
	return ToolRecord{}, false
}
