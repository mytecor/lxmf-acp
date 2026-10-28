package storage

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/thatSFguy/reticulum-go/rns"
)

// ChatRecord survives ACP session resets; Hash is the local LXMF destination.
type ChatRecord struct {
	Hash           string    `json:"hash"`
	PrivateKey     string    `json:"privateKey,omitempty"`
	Owner          string    `json:"owner"`
	SessionID      string    `json:"sessionId,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	LastActivityAt time.Time `json:"lastActivityAt,omitempty"`
	UpdatedAt      time.Time `json:"updatedAt,omitempty"`
}

type State struct {
	ToolChats       map[string]ToolChat           `json:"toolChats,omitempty"`
	Admin           string                        `json:"admin,omitempty"`
	Allowlist       []string                      `json:"allowlist,omitempty"`
	Chats           map[string]ChatRecord         `json:"chats"`
	KnownIdentities map[string]*rns.KnownIdentity `json:"knownIdentities,omitempty"`
}

type Store struct {
	path string
	mu   sync.RWMutex
	data State
}

func SessionKey(localHex, remoteHex string) string {
	if localHex == "" {
		return remoteHex
	}
	return localHex + ":" + remoteHex
}

func NewStore(path string) (*Store, error) {
	s := &Store{
		path: path,
		data: State{
			Chats:           make(map[string]ChatRecord),
			KnownIdentities: make(map[string]*rns.KnownIdentity),
		},
	}

	if err := s.load(); err != nil {
		return nil, fmt.Errorf("load state from %s: %w", path, err)
	}

	return s, nil
}

func (s *Store) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	raw, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		// New store, create empty file
		return s.saveLocked()
	}
	if err != nil {
		return err
	}

	if len(raw) == 0 {
		return nil
	}

	var loaded State
	if err := json.Unmarshal(raw, &loaded); err != nil {
		return fmt.Errorf("unmarshal json: %w", err)
	}
	if loaded.Chats == nil {
		loaded.Chats = make(map[string]ChatRecord)
	}
	if loaded.KnownIdentities == nil {
		loaded.KnownIdentities = make(map[string]*rns.KnownIdentity)
	}
	s.data = loaded
	return nil
}

func (s *Store) saveLocked() error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}

	tmpFile := s.path + ".tmp"
	if err := os.WriteFile(tmpFile, raw, 0600); err != nil {
		return err
	}

	return os.Rename(tmpFile, s.path)
}

func (s *Store) GetSession(localHex, remoteHex string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	chat, ok := s.data.Chats[localHex]
	return chat.SessionID, ok && strings.EqualFold(chat.Owner, remoteHex) && chat.SessionID != ""
}

func (s *Store) SetSession(localHex, remoteHex, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	chat := s.data.Chats[localHex]
	if chat.Owner != "" && !strings.EqualFold(chat.Owner, remoteHex) {
		return fmt.Errorf("chat %s belongs to another peer", localHex)
	}
	now := time.Now()
	if chat.CreatedAt.IsZero() {
		chat.CreatedAt = now
	}
	chat.Hash, chat.Owner = localHex, remoteHex
	chat.SessionID, chat.UpdatedAt = sessionID, now
	s.data.Chats[localHex] = chat
	return s.saveLocked()
}

func (s *Store) DeleteSession(localHex, remoteHex string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	chat, ok := s.data.Chats[localHex]
	if !ok || !strings.EqualFold(chat.Owner, remoteHex) {
		return nil
	}
	chat.SessionID = ""
	chat.UpdatedAt = time.Now()
	s.data.Chats[localHex] = chat
	return s.saveLocked()
}

func (s *Store) SaveChat(entry ChatRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data.Chats == nil {
		s.data.Chats = make(map[string]ChatRecord)
	}
	s.data.Chats[entry.Hash] = entry
	return s.saveLocked()
}

func (s *Store) GetChat(localHex string) (ChatRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data.Chats[localHex]
	return entry, ok
}

// TouchChat tracks real chat activity independently of ACP session creation.
func (s *Store) TouchChat(localHex string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.data.Chats[localHex]
	if !ok || !at.After(rec.LastActivityAt) {
		return nil
	}
	rec.LastActivityAt = at
	s.data.Chats[localHex] = rec
	return s.saveLocked()
}

func (s *Store) GetAllChats() []ChatRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]ChatRecord, 0, len(s.data.Chats))
	for _, entry := range s.data.Chats {
		entries = append(entries, entry)
	}
	return entries
}

func (s *Store) CountChatsForOwner(ownerHex string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, entry := range s.data.Chats {
		if entry.PrivateKey != "" && strings.EqualFold(entry.Owner, ownerHex) {
			count++
		}
	}
	return count
}

func (s *Store) GetChatsForOwner(ownerHex string) []ChatRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var res []ChatRecord
	for _, entry := range s.data.Chats {
		if strings.EqualFold(entry.Owner, ownerHex) {
			res = append(res, entry)
		}
	}
	return res
}

func (s *Store) GetAdmin() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Admin
}

func (s *Store) SetAdmin(adminHex string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Admin = strings.ToLower(adminHex)
	return s.saveLocked()
}

func (s *Store) GetAllowlist() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]string, len(s.data.Allowlist))
	copy(res, s.data.Allowlist)
	return res
}

func (s *Store) AddToAllowlist(peerHex string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	clean := strings.ToLower(peerHex)

	// If allowlist was empty and admin is set, ensure admin is also included in allowlist
	if len(s.data.Allowlist) == 0 && s.data.Admin != "" && !strings.EqualFold(s.data.Admin, clean) {
		s.data.Allowlist = append(s.data.Allowlist, strings.ToLower(s.data.Admin))
	}

	for _, a := range s.data.Allowlist {
		if strings.EqualFold(a, clean) {
			return false, nil
		}
	}
	s.data.Allowlist = append(s.data.Allowlist, clean)
	return true, s.saveLocked()
}

func (s *Store) RemoveFromAllowlist(peerHex string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Admin != "" && strings.EqualFold(peerHex, s.data.Admin) {
		return false, fmt.Errorf("cannot remove administrator from allowlist")
	}

	idx := -1
	for i, a := range s.data.Allowlist {
		if strings.EqualFold(a, peerHex) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return false, nil
	}
	s.data.Allowlist = append(s.data.Allowlist[:idx], s.data.Allowlist[idx+1:]...)
	return true, s.saveLocked()
}

func (s *Store) IsAllowed(peerHex string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.data.Allowlist) == 0 {
		return true
	}
	if s.data.Admin != "" && strings.EqualFold(s.data.Admin, peerHex) {
		return true
	}
	for _, a := range s.data.Allowlist {
		if strings.EqualFold(a, peerHex) {
			return true
		}
	}
	return false
}

func (s *Store) SaveKnownIdentities(known []*rns.KnownIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data.KnownIdentities == nil {
		s.data.KnownIdentities = make(map[string]*rns.KnownIdentity)
	}
	changed := false
	for _, k := range known {
		if k == nil || len(k.DestHash) == 0 {
			continue
		}
		key := hex.EncodeToString(k.DestHash)
		existing, ok := s.data.KnownIdentities[key]
		if !ok || k.LastSeen.After(existing.LastSeen) {
			s.data.KnownIdentities[key] = k
			changed = true
		}
	}
	if changed {
		return s.saveLocked()
	}
	return nil
}

func (s *Store) GetKnownIdentities() []*rns.KnownIdentity {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*rns.KnownIdentity, 0, len(s.data.KnownIdentities))
	for _, k := range s.data.KnownIdentities {
		out = append(out, k)
	}
	return out
}
