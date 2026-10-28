package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
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

type State struct {
	Identities map[string]IdentityRecord `json:"identities"`
	Sessions   map[string]SessionEntry   `json:"sessions"`
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
			Identities: make(map[string]IdentityRecord),
			Sessions:   make(map[string]SessionEntry),
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
