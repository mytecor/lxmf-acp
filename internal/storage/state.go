package storage

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/thatSFguy/reticulum-go/rns"
)

type IdentityRecord struct {
	Hash       string    `json:"hash"`
	PrivateKey string    `json:"privateKey"`
	Owner      string    `json:"owner"`
	SessionID  string    `json:"sessionId,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
}

type SessionEntry struct {
	LocalDestination  string    `json:"localDestination,omitempty"`
	RemoteDestination string    `json:"remoteDestination,omitempty"`
	SessionID         string    `json:"sessionId"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

type MessageEntry struct {
	ID        string    `json:"id"`
	ChatKey   string    `json:"chatKey"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

type State struct {
	Identities      map[string]IdentityRecord     `json:"identities"`
	Sessions        map[string]SessionEntry       `json:"sessions"`
	Messages        map[string]MessageEntry       `json:"messages,omitempty"`
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
			Identities:      make(map[string]IdentityRecord),
			Sessions:        make(map[string]SessionEntry),
			Messages:        make(map[string]MessageEntry),
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

	if loaded.Sessions == nil {
		loaded.Sessions = make(map[string]SessionEntry)
	}
	if loaded.Identities == nil {
		loaded.Identities = make(map[string]IdentityRecord)
	}
	if loaded.Messages == nil {
		loaded.Messages = make(map[string]MessageEntry)
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

	key := SessionKey(localHex, remoteHex)
	entry, ok := s.data.Sessions[key]
	if (!ok || entry.SessionID == "") && localHex != "" {
		// Fallback for legacy state files where key was just remoteHex
		entry, ok = s.data.Sessions[remoteHex]
	}
	if !ok || entry.SessionID == "" {
		return "", false
	}
	return entry.SessionID, true
}

func (s *Store) SetSession(localHex, remoteHex, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	key := SessionKey(localHex, remoteHex)
	entry, exists := s.data.Sessions[key]
	if !exists {
		entry.CreatedAt = now
	}
	entry.LocalDestination = localHex
	entry.RemoteDestination = remoteHex
	entry.SessionID = sessionID
	entry.UpdatedAt = now

	s.data.Sessions[key] = entry

	// Keep IdentityRecord.SessionID in sync if local destination is registered
	if localHex != "" {
		if idRec, exists := s.data.Identities[localHex]; exists {
			idRec.SessionID = sessionID
			s.data.Identities[localHex] = idRec
		}
	}

	return s.saveLocked()
}

func (s *Store) DeleteSession(localHex, remoteHex string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := SessionKey(localHex, remoteHex)
	_, exists := s.data.Sessions[key]
	if !exists && localHex != "" {
		key = remoteHex
		_, exists = s.data.Sessions[key]
	}
	if !exists {
		return nil
	}

	delete(s.data.Sessions, key)
	if localHex != "" {
		if idRec, ok := s.data.Identities[localHex]; ok {
			idRec.SessionID = ""
			s.data.Identities[localHex] = idRec
		}
	}
	return s.saveLocked()
}

func (s *Store) SaveIdentity(entry IdentityRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data.Identities == nil {
		s.data.Identities = make(map[string]IdentityRecord)
	}
	s.data.Identities[entry.Hash] = entry
	return s.saveLocked()
}

func (s *Store) GetIdentity(localHex string) (IdentityRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data.Identities[localHex]
	return entry, ok
}

func (s *Store) GetAllIdentities() []IdentityRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]IdentityRecord, 0, len(s.data.Identities))
	for _, entry := range s.data.Identities {
		entries = append(entries, entry)
	}
	return entries
}

func (s *Store) CountIdentitiesForOwner(ownerHex string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, entry := range s.data.Identities {
		if entry.Owner == ownerHex {
			count++
		}
	}
	return count
}

const maxStoredMessages = 200

func (s *Store) SaveMessage(entry MessageEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry.ID == "" {
		return nil
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	if s.data.Messages == nil {
		s.data.Messages = make(map[string]MessageEntry)
	}

	if len(s.data.Messages) >= maxStoredMessages {
		var oldestKey string
		var oldestTime time.Time
		first := true
		for k, m := range s.data.Messages {
			if first || m.Timestamp.Before(oldestTime) {
				oldestKey = k
				oldestTime = m.Timestamp
				first = false
			}
		}
		if oldestKey != "" {
			delete(s.data.Messages, oldestKey)
		}
	}

	s.data.Messages[entry.ID] = entry
	return s.saveLocked()
}

func (s *Store) GetMessage(idHex string) (MessageEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.data.Messages == nil {
		return MessageEntry{}, false
	}
	entry, ok := s.data.Messages[idHex]
	return entry, ok
}

func (s *Store) GetRecentMessages() []MessageEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data.Messages) == 0 {
		return nil
	}
	msgs := make([]MessageEntry, 0, len(s.data.Messages))
	for _, m := range s.data.Messages {
		msgs = append(msgs, m)
	}
	return msgs
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
