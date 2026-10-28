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

type IdentityRecord struct {
	Hash           string    `json:"hash"`
	PrivateKey     string    `json:"privateKey"`
	Owner          string    `json:"owner"`
	SessionID      string    `json:"sessionId,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	LastActivityAt time.Time `json:"lastActivityAt,omitempty"`
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
	ToolChats       map[string]ToolChat           `json:"toolChats,omitempty"`
	Admin           string                        `json:"admin,omitempty"`
	Allowlist       []string                      `json:"allowlist,omitempty"`
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
	// Older state files have no activity timestamp. Recover the best available
	// date instead of making every restored address look newly active.
	legacy := make(map[string]bool)
	for key, rec := range loaded.Identities {
		if rec.LastActivityAt.IsZero() {
			legacy[key] = true
			rec.LastActivityAt = rec.CreatedAt
			loaded.Identities[key] = rec
		}
	}
	recoverActivity := func(local string, at time.Time) {
		if !legacy[local] {
			return
		}
		rec := loaded.Identities[local]
		if at.After(rec.LastActivityAt) {
			rec.LastActivityAt = at
			loaded.Identities[local] = rec
		}
	}
	for _, entry := range loaded.Sessions {
		recoverActivity(entry.LocalDestination, entry.UpdatedAt)
	}
	for _, entry := range loaded.Messages {
		local, _, ok := strings.Cut(entry.ChatKey, ":")
		if ok {
			recoverActivity(local, entry.Timestamp)
		}
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

// TouchIdentity tracks real chat activity independently of ACP session creation.
func (s *Store) TouchIdentity(localHex string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.data.Identities[localHex]
	if !ok || !at.After(rec.LastActivityAt) {
		return nil
	}
	rec.LastActivityAt = at
	s.data.Identities[localHex] = rec
	return s.saveLocked()
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
		if entry.PrivateKey != "" && strings.EqualFold(entry.Owner, ownerHex) {
			count++
		}
	}
	return count
}

func (s *Store) GetIdentitiesForOwner(ownerHex string) []IdentityRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var res []IdentityRecord
	for _, entry := range s.data.Identities {
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
