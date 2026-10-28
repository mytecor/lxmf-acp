package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mytecor/lxmf-acp/internal/acp"
	"github.com/mytecor/lxmf-acp/internal/config"
	"github.com/mytecor/lxmf-acp/internal/lxmf"
	"github.com/mytecor/lxmf-acp/internal/storage"
	reticulumLxmf "github.com/thatSFguy/reticulum-go/lxmf"
	"github.com/thatSFguy/reticulum-go/rns"
)

type sentMsg struct {
	From    []byte
	To      []byte
	Title   string
	Content string
	Fields  map[any]any
	Direct  bool
	ID      []byte
}

type mockLXMF struct {
	mu           sync.Mutex
	initialID    *rns.Identity
	identities   map[string]*rns.Identity
	sent         []sentMsg
	handler      lxmf.MessageHandler
	failWelcome  bool
	failLocalHex string
	msgCounter   int
	// sendHook, if set, runs before SendMessage records a message; it can
	// block to simulate a pending delivery or return an error to fail it.
	sendHook func(m sentMsg) error
}

func newMockLXMF(initialID *rns.Identity) *mockLXMF {
	m := &mockLXMF{
		initialID:  initialID,
		identities: make(map[string]*rns.Identity),
	}
	if initialID != nil {
		h := initialID.DestinationHashFor(reticulumLxmf.FullName())
		m.identities[hex.EncodeToString(h)] = initialID
	}
	return m
}

func (m *mockLXMF) Start(ctx context.Context) error {
	return nil
}

func (m *mockLXMF) Close() error {
	return nil
}

func (m *mockLXMF) SendFrom(localDestHash, recipientHash []byte, title, content string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	locHex := hex.EncodeToString(localDestHash)
	if m.failWelcome {
		initHex := ""
		if m.initialID != nil {
			initHex = hex.EncodeToString(m.initialID.DestinationHashFor(reticulumLxmf.FullName()))
		}
		// If failWelcome is active, fail outbound message from the new identity, but allow report from initial identity
		if locHex != initHex {
			return errors.New("simulated delivery failure")
		}
	}

	m.sent = append(m.sent, sentMsg{
		From:    append([]byte(nil), localDestHash...),
		To:      append([]byte(nil), recipientHash...),
		Title:   title,
		Content: content,
	})
	return nil
}

func (m *mockLXMF) SendMessage(localDestHash, recipientHash []byte, out lxmf.OutboundMessage) ([]byte, error) {
	m.mu.Lock()
	locHex := hex.EncodeToString(localDestHash)
	if m.failWelcome {
		initHex := ""
		if m.initialID != nil {
			initHex = hex.EncodeToString(m.initialID.DestinationHashFor(reticulumLxmf.FullName()))
		}
		if locHex != initHex {
			m.mu.Unlock()
			return nil, errors.New("simulated delivery failure")
		}
	}
	m.msgCounter++
	id := sha256.Sum256([]byte(fmt.Sprintf("msg-%d-%x", m.msgCounter, localDestHash)))
	hook := m.sendHook
	m.mu.Unlock()

	msg := sentMsg{
		From:    append([]byte(nil), localDestHash...),
		To:      append([]byte(nil), recipientHash...),
		Title:   out.Title,
		Content: out.Content,
		Fields:  out.Fields,
		Direct:  out.Direct,
		ID:      id[:],
	}
	if hook != nil {
		if err := hook(msg); err != nil {
			return nil, err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, msg)
	return id[:], nil
}

func (m *mockLXMF) Send(recipientHash []byte, title, content string) error {
	return m.SendFrom(m.Hash(), recipientHash, title, content)
}

func (m *mockLXMF) RegisterIdentity(identity *rns.Identity) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := identity.DestinationHashFor(reticulumLxmf.FullName())
	m.identities[hex.EncodeToString(h)] = identity
	return nil
}

func (m *mockLXMF) Hash() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.initialID != nil {
		return m.initialID.DestinationHashFor(reticulumLxmf.FullName())
	}
	return nil
}

func (m *mockLXMF) HexHash() string {
	h := m.Hash()
	if h == nil {
		return ""
	}
	return hex.EncodeToString(h)
}

func (m *mockLXMF) SetMessageHandler(h lxmf.MessageHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = h
}

func (m *mockLXMF) RestoreKnownIdentities(identities []*rns.KnownIdentity) {}
func (m *mockLXMF) KnownSnapshot() []*rns.KnownIdentity                    { return nil }
func (m *mockLXMF) PinDestinations(destHashes [][]byte)                    {}
func (m *mockLXMF) RequestPath(destHash []byte) error                      { return nil }
func (m *mockLXMF) SetAnnounceHandler(h func(*rns.Announce))               {}
func (m *mockLXMF) SetIdentityLastActivity([]byte, time.Time)              {}

func (m *mockLXMF) getSent() []sentMsg {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]sentMsg, len(m.sent))
	copy(cp, m.sent)
	return cp
}

func (m *mockLXMF) identityCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.identities)
}

func (m *mockLXMF) hasIdentity(hexHash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.identities[hexHash]
	return ok
}

type mockACP struct {
	mu             sync.Mutex
	sessionCounter int
	created        []string
	loaded         []string
	prompts        map[string][]string
	supportLoad    bool
	// streamFn, if set, replaces the default streaming behaviour.
	streamFn func(ctx context.Context, sessionID, text string, onChunk func(string)) (string, error)
}

func newMockACP(supportLoad bool) *mockACP {
	return &mockACP{
		prompts:     make(map[string][]string),
		supportLoad: supportLoad,
	}
}

func (m *mockACP) Connect(ctx context.Context) error {
	return nil
}

func (m *mockACP) Close() error {
	return nil
}

func (m *mockACP) CanLoadSession() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.supportLoad
}

func (m *mockACP) LoadSession(ctx context.Context, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loaded = append(m.loaded, sessionID)
	return nil
}

func (m *mockACP) CreateSession(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessionCounter++
	id := fmt.Sprintf("session-%d", m.sessionCounter)
	m.created = append(m.created, id)
	return id, nil
}

func (m *mockACP) PromptStream(ctx context.Context, sessionID, text string, onChunk func(string)) (string, error) {
	m.mu.Lock()
	m.prompts[sessionID] = append(m.prompts[sessionID], text)
	fn := m.streamFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, sessionID, text, onChunk)
	}
	onChunk("Agent reply: " + text)
	return "end_turn", nil
}

func (m *mockACP) PromptEvents(ctx context.Context, sessionID, text string, onEvent func(acp.Event)) (string, error) {
	return m.PromptStream(ctx, sessionID, text, func(chunk string) { onEvent(acp.Event{Text: chunk}) })
}

func (m *mockACP) getPrompts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var all []string
	for _, p := range m.prompts {
		all = append(all, p...)
	}
	return all
}

func (m *mockACP) CancelPrompt(sessionID string) bool {
	return true
}

func (m *mockACP) getCreated() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.created))
	copy(cp, m.created)
	return cp
}

func (m *mockACP) getLoaded() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.loaded))
	copy(cp, m.loaded)
	return cp
}

// Helper to wait for asynchronous queue processing
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for condition after %v", timeout)
}

func setupTestBridge(t *testing.T) (*Bridge, *mockLXMF, *mockACP, *storage.Store, *rns.Identity, []byte) {
	t.Helper()

	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	store, err := storage.NewStore(stateFile)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	initialID, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}

	mockL := newMockLXMF(initialID)
	mockA := newMockACP(true)

	cfg := &config.Config{
		DisplayName:          "TestAgent",
		MaxIdentitiesPerUser: 10,
		NewChatCooldownSec:   0, // disable cooldown for most tests
	}

	b := NewBridge(cfg, mockL, mockA, store)
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Bridge.Start failed: %v", err)
	}

	userHash := []byte("user1_destination_hash1234567890")
	return b, mockL, mockA, store, initialID, userHash
}

// Scenario 1: /bridge new from initial chat creates a new identity and independent ACP session.
func TestScenario1_NewFromInitialChat(t *testing.T) {
	b, mockL, mockA, store, initialID, userHash := setupTestBridge(t)
	defer b.Close()

	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())
	initialHex := hex.EncodeToString(initialHash)
	userHex := hex.EncodeToString(userHash)

	// 1. Send normal message to initial chat
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("msg-1"),
		Title:      "Question",
		Content:    "Hello in initial chat",
	})

	waitFor(t, 2*time.Second, func() bool {
		return len(mockL.getSent()) >= 1
	})

	sess1, ok := store.GetSession(initialHex, userHex)
	if !ok || sess1 != "session-1" {
		t.Fatalf("expected session-1 in initial chat, got %q, %v", sess1, ok)
	}

	// 2. Send /bridge new from initial chat
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("cmd-new-1"),
		Title:      "Command",
		Content:    "/bridge new",
	})

	// Wait until new identity is created and welcome message is sent
	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2 && len(mockL.getSent()) >= 2
	})

	// Initial chat session should be unchanged!
	sess1After, ok := store.GetSession(initialHex, userHex)
	if !ok || sess1After != "session-1" {
		t.Fatalf("initial chat session was altered! got %q, want session-1", sess1After)
	}

	// Find the new identity
	var newChatHash []byte
	var newChatHex string
	for _, rec := range store.GetAllChats() {
		if rec.Hash != initialHex {
			newChatHex = rec.Hash
			newChatHash, _ = hex.DecodeString(rec.Hash)
			break
		}
	}
	if newChatHex == "" {
		t.Fatalf("new identity not found in store")
	}

	// Verify welcome message was sent from new identity to user
	sentMessages := mockL.getSent()
	welcome := sentMessages[len(sentMessages)-1]
	if hex.EncodeToString(welcome.From) != newChatHex {
		t.Fatalf("welcome message sent from %s, want %s", hex.EncodeToString(welcome.From), newChatHex)
	}
	if hex.EncodeToString(welcome.To) != userHex {
		t.Fatalf("welcome message recipient %s, want %s", hex.EncodeToString(welcome.To), userHex)
	}

	// Verify ACP agent was NOT called with /bridge new
	for sessID, prompts := range mockA.prompts {
		for _, p := range prompts {
			if p == "/bridge new" {
				t.Fatalf("/bridge new was sent to ACP agent session %s!", sessID)
			}
		}
	}

	// 3. Send first normal message in the new chat
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  newChatHash,
		SenderHash: userHash,
		MessageID:  []byte("msg-2"),
		Title:      "Question in new chat",
		Content:    "Hello in new chat",
	})

	waitFor(t, 2*time.Second, func() bool {
		return len(mockA.getCreated()) == 2
	})

	sess2, ok := store.GetSession(newChatHex, userHex)
	if !ok || sess2 != "session-2" {
		t.Fatalf("expected session-2 for new chat, got %q, %v", sess2, ok)
	}

	if sess2 == sess1 {
		t.Fatalf("new chat did not get independent ACP session! sess1=%q, sess2=%q", sess1, sess2)
	}
}

// Scenario 2: /bridge new from a newly created chat behaves identically, without inheriting context.
func TestScenario2_NewFromDynamicChat(t *testing.T) {
	b, mockL, mockA, store, initialID, userHash := setupTestBridge(t)
	defer b.Close()

	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())
	initialHex := hex.EncodeToString(initialHash)
	userHex := hex.EncodeToString(userHash)

	// Create first dynamic chat via /bridge new
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("cmd-new-1"),
		Title:      "Command",
		Content:    "/bridge new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2
	})

	var chat1Hash []byte
	var chat1Hex string
	for _, rec := range store.GetAllChats() {
		if rec.Hash != initialHex {
			chat1Hex = rec.Hash
			chat1Hash, _ = hex.DecodeString(rec.Hash)
			break
		}
	}

	// Message in chat1 -> creates session-1
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  chat1Hash,
		SenderHash: userHash,
		MessageID:  []byte("msg-c1"),
		Title:      "Question",
		Content:    "Question in chat 1",
	})

	waitFor(t, 2*time.Second, func() bool {
		return len(mockA.getCreated()) == 1
	})
	sessChat1, _ := store.GetSession(chat1Hex, userHex)

	// Now send /bridge new FROM chat1!
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  chat1Hash,
		SenderHash: userHash,
		MessageID:  []byte("cmd-new-2"),
		Title:      "Command",
		Content:    "/bridge new",
	})

	// Wait for 3rd identity to be created
	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 3
	})

	// chat1 session must remain unchanged
	sessChat1After, _ := store.GetSession(chat1Hex, userHex)
	if sessChat1After != sessChat1 {
		t.Fatalf("chat1 session was altered by /bridge new! got %s, want %s", sessChat1After, sessChat1)
	}

	// Find the 3rd identity
	var chat2Hash []byte
	var chat2Hex string
	for _, rec := range store.GetAllChats() {
		if rec.Hash != initialHex && rec.Hash != chat1Hex {
			chat2Hex = rec.Hash
			chat2Hash, _ = hex.DecodeString(rec.Hash)
			break
		}
	}
	if chat2Hex == "" {
		t.Fatalf("second dynamic chat was not created")
	}

	// Send normal message in chat2
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  chat2Hash,
		SenderHash: userHash,
		MessageID:  []byte("msg-c2"),
		Title:      "Question",
		Content:    "Question in chat 2",
	})

	waitFor(t, 2*time.Second, func() bool {
		return len(mockA.getCreated()) == 2
	})

	sessChat2, ok := store.GetSession(chat2Hex, userHex)
	if !ok {
		t.Fatalf("session not found for chat2")
	}
	if sessChat2 == sessChat1 {
		t.Fatalf("chat2 shared ACP session with chat1! (%s == %s)", sessChat2, sessChat1)
	}
}

// Scenario 3: After restart, all dynamically created addresses continue accepting messages and restoring ACP sessions.
func TestScenario3_RestartRestoresIdentitiesAndRecoversACPSessions(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	store, err := storage.NewStore(stateFile)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	initialID, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())
	initialHex := hex.EncodeToString(initialHash)

	userHash := []byte("user1_destination_hash1234567890")
	userHex := hex.EncodeToString(userHash)

	cfg := &config.Config{
		DisplayName:          "TestAgent",
		MaxIdentitiesPerUser: 10,
		NewChatCooldownSec:   0,
	}

	// 1. Run bridge before restart
	mockL1 := newMockLXMF(initialID)
	mockA1 := newMockACP(true)
	b1 := NewBridge(cfg, mockL1, mockA1, store)
	if err := b1.Start(context.Background()); err != nil {
		t.Fatalf("b1.Start failed: %v", err)
	}

	// Create a chat via /bridge new
	mockL1.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("cmd-new-1"),
		Title:      "Command",
		Content:    "/bridge new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL1.identityCount() == 2
	})

	var dynamicHash []byte
	var dynamicHex string
	for _, rec := range store.GetAllChats() {
		if rec.Hash != initialHex {
			dynamicHex = rec.Hash
			dynamicHash, _ = hex.DecodeString(rec.Hash)
			break
		}
	}

	// Message to initial chat -> session-1
	mockL1.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("msg-init-1"),
		Title:      "Init",
		Content:    "Hello init",
	})
	// Message to dynamic chat -> session-2
	mockL1.handler(&lxmf.InboundMessage{
		LocalHash:  dynamicHash,
		SenderHash: userHash,
		MessageID:  []byte("msg-dyn-1"),
		Title:      "Dyn",
		Content:    "Hello dyn",
	})

	waitFor(t, 2*time.Second, func() bool {
		return len(mockA1.getCreated()) == 2
	})

	// Close b1 (simulating shutdown)
	_ = b1.Close()

	// 2. Restart bridge with fresh client instances using the same store file
	storeReloaded, err := storage.NewStore(stateFile)
	if err != nil {
		t.Fatalf("Reload store failed: %v", err)
	}

	mockL2 := newMockLXMF(initialID) // only initialized with initialID
	mockA2 := newMockACP(true)
	b2 := NewBridge(cfg, mockL2, mockA2, storeReloaded)

	if err := b2.Start(context.Background()); err != nil {
		t.Fatalf("b2.Start failed: %v", err)
	}
	defer b2.Close()

	// Verify all identities are registered after restart
	if !mockL2.hasIdentity(initialHex) {
		t.Errorf("initial identity not registered in LXMF after restart")
	}
	if !mockL2.hasIdentity(dynamicHex) {
		t.Errorf("dynamic identity %s was not restored after restart", dynamicHex)
	}
	if mockL2.identityCount() != 2 {
		t.Errorf("expected 2 identities after restart, got %d", mockL2.identityCount())
	}

	// 3. Send message to initial chat after restart
	mockL2.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("msg-init-2"),
		Title:      "Init",
		Content:    "Message after restart to initial chat",
	})

	// 4. Send message to dynamic chat after restart
	mockL2.handler(&lxmf.InboundMessage{
		LocalHash:  dynamicHash,
		SenderHash: userHash,
		MessageID:  []byte("msg-dyn-2"),
		Title:      "Dyn",
		Content:    "Message after restart to dynamic chat",
	})

	// Verify both sessions were loaded (recovered) rather than newly created!
	waitFor(t, 2*time.Second, func() bool {
		return len(mockA2.getLoaded()) >= 2
	})

	loaded := mockA2.getLoaded()
	sessInit, _ := storeReloaded.GetSession(initialHex, userHex)
	sessDyn, _ := storeReloaded.GetSession(dynamicHex, userHex)

	hasInit := false
	hasDyn := false
	for _, l := range loaded {
		if l == sessInit {
			hasInit = true
		}
		if l == sessDyn {
			hasDyn = true
		}
	}
	if !hasInit || !hasDyn {
		t.Errorf("expected sessions %s and %s to be loaded, got %v", sessInit, sessDyn, loaded)
	}
}

// Scenario 4: Re-delivery of the same /bridge new command does not create an extra chat.
func TestScenario4_DuplicateNewCommandDoesNotCreateDuplicateChat(t *testing.T) {
	b, mockL, _, _, initialID, userHash := setupTestBridge(t)
	defer b.Close()

	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())

	duplicateMsgID := []byte("duplicate-message-id-1234567890123456")

	// First delivery of /bridge new
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  duplicateMsgID,
		Title:      "Command",
		Content:    "/bridge new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2
	})

	// Second delivery of identical /bridge new (same message ID)
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  duplicateMsgID,
		Title:      "Command",
		Content:    "/bridge new",
	})

	time.Sleep(150 * time.Millisecond)

	// Identity count must remain 2!
	if mockL.identityCount() != 2 {
		t.Fatalf("duplicate /bridge new created additional identity! got %d identities, want 2", mockL.identityCount())
	}
}

// Verify owner isolation: another user cannot write to a chat created by someone else
func TestOwnerIsolation(t *testing.T) {
	b, mockL, mockA, store, initialID, user1Hash := setupTestBridge(t)
	defer b.Close()

	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())
	user2Hash := []byte("user2_other_destination_hash1234")

	// User1 creates chat
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: user1Hash,
		MessageID:  []byte("new-1"),
		Title:      "Command",
		Content:    "/bridge new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2
	})

	var dynamicHash []byte
	for _, rec := range store.GetAllChats() {
		if rec.Hash != hex.EncodeToString(initialHash) {
			dynamicHash, _ = hex.DecodeString(rec.Hash)
			break
		}
	}

	// User2 tries to message dynamic chat created by User1
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  dynamicHash,
		SenderHash: user2Hash,
		MessageID:  []byte("unauth-msg"),
		Title:      "Attack",
		Content:    "Should be rejected",
	})

	time.Sleep(150 * time.Millisecond)

	// User2's message must NOT have triggered any ACP prompt or session creation
	if len(mockA.getCreated()) > 0 {
		t.Fatalf("unauthorized peer message was processed by ACP agent!")
	}
}

// Verify fallback notice in source chat if welcome message delivery fails
func TestWelcomeDeliveryFailureReportsDestinationInSourceChat(t *testing.T) {
	b, mockL, _, store, initialID, userHash := setupTestBridge(t)
	defer b.Close()

	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())

	// Configure mock to fail welcome message sends
	mockL.mu.Lock()
	mockL.failWelcome = true
	mockL.mu.Unlock()

	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("cmd-new-fail"),
		Title:      "Command",
		Content:    "/bridge new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2 && len(mockL.getSent()) >= 1
	})

	// Find the created identity
	var newChatHex string
	for _, rec := range store.GetAllChats() {
		if rec.Hash != hex.EncodeToString(initialHash) {
			newChatHex = rec.Hash
			break
		}
	}

	sent := mockL.getSent()
	last := sent[len(sent)-1]

	// Sent must originate from initialHash (source chat) to userHash
	if hex.EncodeToString(last.From) != hex.EncodeToString(initialHash) {
		t.Errorf("failure notification sent from %s, want source chat %s", hex.EncodeToString(last.From), hex.EncodeToString(initialHash))
	}

	// Message content must mention destination hash of the new chat
	expectedSubstring := fmt.Sprintf("<%s>", newChatHex)
	if !containsStr(last.Content, expectedSubstring) {
		t.Errorf("failure report does not contain destination hash %s: %q", expectedSubstring, last.Content)
	}
}

// Verify rate limiting and quota enforcement
func TestRateLimitingAndQuota(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	store, _ := storage.NewStore(stateFile)
	initialID, _ := rns.NewIdentity()
	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())

	userHash := []byte("user_rate_limit_1234567890123456")

	mockL := newMockLXMF(initialID)
	mockA := newMockACP(true)

	cfg := &config.Config{
		DisplayName:          "TestAgent",
		MaxIdentitiesPerUser: 2, // quota: max 2
		NewChatCooldownSec:   2, // cooldown: 2 sec
	}

	b := NewBridge(cfg, mockL, mockA, store)
	_ = b.Start(context.Background())
	defer b.Close()

	// 1st /bridge new -> succeeds
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("new-1"),
		Title:      "Command",
		Content:    "/bridge new",
	})
	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2
	})

	// 2nd /bridge new immediately -> triggers rate limit!
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("new-2"),
		Title:      "Command",
		Content:    "/bridge new",
	})
	time.Sleep(150 * time.Millisecond)

	// Should not have created a 3rd identity
	if mockL.identityCount() != 2 {
		t.Fatalf("rate limit did not block second /bridge new! count = %d", mockL.identityCount())
	}

	// Verify rate limit message was sent
	sent := mockL.getSent()
	last := sent[len(sent)-1]
	if !containsStr(last.Content, "Rate limit exceeded") {
		t.Errorf("expected rate limit message, got %q", last.Content)
	}

	// Manually reset cooldown timestamp for quota testing
	b.rateMu.Lock()
	b.lastNewChat[hex.EncodeToString(userHash)] = time.Now().Add(-10 * time.Second)
	b.rateMu.Unlock()

	// 2nd /bridge new after cooldown -> succeeds (count becomes 3 = initial + 2 dynamic)
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("new-3"),
		Title:      "Command",
		Content:    "/bridge new",
	})
	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 3
	})

	// Reset cooldown again to test quota
	b.rateMu.Lock()
	b.lastNewChat[hex.EncodeToString(userHash)] = time.Now().Add(-10 * time.Second)
	b.rateMu.Unlock()

	// 3rd /bridge new -> exceeds quota of 2!
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("new-4"),
		Title:      "Command",
		Content:    "/bridge new",
	})
	time.Sleep(150 * time.Millisecond)

	// Count must still be 3
	if mockL.identityCount() != 3 {
		t.Fatalf("quota did not block third /bridge new! count = %d", mockL.identityCount())
	}

	sent = mockL.getSent()
	last = sent[len(sent)-1]
	if !containsStr(last.Content, "Maximum number of chats") {
		t.Errorf("expected quota reached message, got %q", last.Content)
	}
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(s) > 0 && len(sub) > 0 && findSubstr(s, sub)))
}

func findSubstr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestReplyHandling(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewStore(filepath.Join(tmpDir, "state.json"))
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	initID, _ := rns.NewIdentity()
	initialHash := initID.DestinationHashFor(reticulumLxmf.FullName())
	mockL := newMockLXMF(initID)
	mockA := newMockACP(false)

	userID, _ := rns.NewIdentity()
	userHash := userID.DestinationHashFor(reticulumLxmf.FullName())
	userHex := hex.EncodeToString(userHash)

	cfg := &config.Config{
		DisplayName:     "TestAgent",
		Allowlist:       []string{userHex},
		OutboundReplies: true,
	}

	b := NewBridge(cfg, mockL, mockA, store)
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Bridge.Start failed: %v", err)
	}
	defer b.Close()

	// 1. Initial normal turn
	m1ID := []byte("user-msg-1")
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  m1ID,
		Title:      "Question",
		Content:    "Hello agent",
	})
	time.Sleep(150 * time.Millisecond)

	sent := mockL.getSent()
	if len(sent) == 0 {
		t.Fatalf("expected agent response, got none")
	}
	firstAgentReply := sent[len(sent)-1]
	// OutboundReplies attaches reply metadata to user prompts
	rep1, ok1 := firstAgentReply.Fields[lxmf.FieldReplyTo].([]byte)
	if !ok1 || !bytes.Equal(rep1, m1ID) {
		t.Errorf("expected response to carry FieldReplyTo=%x, got %x", m1ID, rep1)
	}
	quoteBytes1, ok1Quote := firstAgentReply.Fields[lxmf.FieldReplyQuote].([]byte)
	if !ok1Quote || string(quoteBytes1) != "Hello agent" {
		t.Errorf("expected response to carry FieldReplyQuote as []byte %q, got %T: %v", "Hello agent", firstAgentReply.Fields[lxmf.FieldReplyQuote], firstAgentReply.Fields[lxmf.FieldReplyQuote])
	}

	// 2. User replies to the assistant's previous message
	m2ID := []byte("user-msg-2")
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  m2ID,
		Title:      "Reply",
		Content:    "Option A please",
		Fields: map[any]any{
			lxmf.FieldReplyTo: firstAgentReply.ID,
		},
	})
	time.Sleep(150 * time.Millisecond)

	prompts := mockA.getPrompts()
	if len(prompts) < 2 {
		t.Fatalf("expected at least 2 prompts, got %d", len(prompts))
	}
	p2 := prompts[len(prompts)-1]
	if !containsStr(p2, "[In reply to message <"+hex.EncodeToString(firstAgentReply.ID)+">]") {
		t.Errorf("expected reply ID in prompt, got %q", p2)
	}
	if containsStr(p2, "Hello agent") {
		t.Errorf("reply without quote unexpectedly recovered message text: %q", p2)
	}
	if !containsStr(p2, "Option A please") {
		t.Errorf("expected user reply text in prompt, got %q", p2)
	}

	// The agent's response to a reply message SHOULD carry FieldReplyTo pointing to m2ID and FieldReplyQuote
	sent = mockL.getSent()
	secondAgentReply := sent[len(sent)-1]
	repField, ok := secondAgentReply.Fields[lxmf.FieldReplyTo].([]byte)
	if !ok || !bytes.Equal(repField, m2ID) {
		t.Errorf("expected response to carry FieldReplyTo=%x, got %x (ok=%v)", m2ID, repField, ok)
	}
	quoteField, ok := secondAgentReply.Fields[lxmf.FieldReplyQuote].([]byte)
	if !ok || string(quoteField) != "Option A please" {
		t.Errorf("expected response to carry FieldReplyQuote=%q, got %q (ok=%v)", "Option A please", string(quoteField), ok)
	}

	// 3. User supplies the quoted context through FieldReplyQuote
	unknownID := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	m3ID := []byte("user-msg-3")
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  m3ID,
		Title:      "Quote Reply",
		Content:    "I agree with that quote",
		Fields: map[any]any{
			lxmf.FieldReplyTo:    unknownID,
			lxmf.FieldReplyQuote: "Some ancient quote",
		},
	})
	time.Sleep(150 * time.Millisecond)

	prompts = mockA.getPrompts()
	p3 := prompts[len(prompts)-1]
	if !containsStr(p3, "[In reply to:\n> Some ancient quote]") {
		t.Errorf("expected prompt to contain '[In reply to:\\n> Some ancient quote]', got %q", p3)
	}
	if !containsStr(p3, "I agree with that quote") {
		t.Errorf("expected user text in prompt, got %q", p3)
	}

	// 4. User replies to a long Cyrillic/Russian text with special symbols (> 500 runes)
	// Must not produce invalid UTF-8 by slicing mid-codepoint.
	longCyrillic := strings.Repeat("Да, вижу реплай! Тестируем длинный текст с русскими буквами. 🛠️ ", 20)
	longID := []byte("long-msg-id-1")

	m4ID := []byte("user-msg-4")
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  m4ID,
		Title:      "Reply to long Russian",
		Content:    "а сейчас?",
		Fields: map[any]any{
			lxmf.FieldReplyTo:    longID,
			lxmf.FieldReplyQuote: longCyrillic,
		},
	})
	time.Sleep(150 * time.Millisecond)

	prompts = mockA.getPrompts()
	p4 := prompts[len(prompts)-1]
	if !containsStr(p4, "а сейчас?") {
		t.Errorf("expected user text in p4, got %q", p4)
	}
	if !containsStr(p4, "> Да, вижу реплай!") || !containsStr(p4, "...") {
		t.Fatalf("expected truncated client quote, got %q", p4)
	}
	// Verify valid UTF-8 and json marshaling
	if !utf8.ValidString(p4) {
		t.Fatalf("p4 contains invalid UTF-8!")
	}
	if _, err := json.Marshal(p4); err != nil {
		t.Fatalf("json.Marshal failed on p4: %v", err)
	}
}

// TestAdminWhitelistAndBotGreeting verifies:
// 1. Root chat ownership by admin
// 2. Non-admin cannot execute administrative commands
// 3. /bridge allow adds user, creates dedicated chat, and proactively sends welcome message to the user
// 4. Whitelisted user chatting in their dedicated chat works
// 5. Whitelisted user writing to root chat is redirected to their personal chat
// 6. /bridge whitelist shows allowed users
// 7. /bridge revoke removes access
func TestAdminWhitelistAndBotGreeting(t *testing.T) {
	b, mockL, mockA, store, initialID, adminHash := setupTestBridge(t)
	defer b.Close()

	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())
	userHash := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00}
	userHex := hex.EncodeToString(userHash)

	// 1. Admin messages initial chat to claim ownership & admin role
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: adminHash,
		MessageID:  []byte("admin-init"),
		Title:      "Hello",
		Content:    "Hello agent",
	})
	waitFor(t, 2*time.Second, func() bool {
		return len(mockL.getSent()) >= 1
	})

	if b.store.GetAdmin() != hex.EncodeToString(adminHash) {
		t.Fatalf("admin not set, got %q", b.store.GetAdmin())
	}

	// 2. Non-admin tries to run /bridge allow
	unauthHash := []byte("unauth_user_00000000000000000000")
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: unauthHash,
		MessageID:  []byte("unauth-allow"),
		Title:      "Hack",
		Content:    "/bridge allow " + userHex,
	})
	time.Sleep(100 * time.Millisecond)
	sent := mockL.getSent()
	last := sent[len(sent)-1]
	// Root chat belongs to admin, so unauth cannot message root chat
	if !containsStr(last.Content, "belongs to another user") && !containsStr(last.Content, "Permission denied") {
		t.Fatalf("expected rejection for unauth user, got: %q", last.Content)
	}

	// 3. Admin runs /bridge whitelist before any allowed users
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: adminHash,
		MessageID:  []byte("admin-wl-1"),
		Title:      "Command",
		Content:    "/bridge whitelist",
	})
	waitFor(t, 2*time.Second, func() bool {
		sent := mockL.getSent()
		return containsStr(sent[len(sent)-1].Content, "Whitelist is empty")
	})

	// 4. Admin runs /bridge allow <userHex>
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: adminHash,
		MessageID:  []byte("admin-allow-cmd"),
		Title:      "Command",
		Content:    "/bridge allow " + userHex,
	})

	// Wait for bot to:
	// a) Proactively send greeting to user from new dedicated chat
	// b) Send confirmation to admin
	waitFor(t, 2*time.Second, func() bool {
		allSent := mockL.getSent()
		hasGreetingToUser := false
		hasAdminConfirm := false
		for _, s := range allSent {
			if bytes.Equal(s.To, userHash) && containsStr(s.Content, "Started a new conversation") {
				hasGreetingToUser = true
			}
			if bytes.Equal(s.To, adminHash) && containsStr(s.Content, "Added user") {
				hasAdminConfirm = true
			}
		}
		return hasGreetingToUser && hasAdminConfirm
	})

	// Verify user is in allowlist in store
	if !store.IsAllowed(userHex) {
		t.Fatalf("userHex not allowed in store after /bridge allow")
	}

	// Find the dedicated chat created for user
	userChats := store.GetChatsForOwner(userHex)
	if len(userChats) != 1 {
		t.Fatalf("expected 1 dedicated chat for user, got %d", len(userChats))
	}
	userChatHash, _ := hex.DecodeString(userChats[0].Hash)

	// 5. User sends message to their dedicated chat
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  userChatHash,
		SenderHash: userHash,
		MessageID:  []byte("user-msg-1"),
		Title:      "Question",
		Content:    "Hello from invited user",
	})
	waitFor(t, 2*time.Second, func() bool {
		for _, s := range mockL.getSent() {
			if bytes.Equal(s.To, userHash) && containsStr(s.Content, "Agent reply: Hello from invited user") {
				return true
			}
		}
		return false
	})

	// 6. User attempts to write to root chat (owned by admin)
	preCount := len(mockA.getPrompts())
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("user-root-msg"),
		Title:      "Wrong chat",
		Content:    "Can I talk in root?",
	})
	waitFor(t, 2*time.Second, func() bool {
		allSent := mockL.getSent()
		last := allSent[len(allSent)-1]
		return bytes.Equal(last.To, userHash) && containsStr(last.Content, "belongs to another user")
	})
	if len(mockA.getPrompts()) != preCount {
		t.Fatalf("prompt was executed for message sent to someone else's chat!")
	}

	// 7. Admin checks /bridge whitelist
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: adminHash,
		MessageID:  []byte("admin-wl-2"),
		Title:      "Command",
		Content:    "/bridge whitelist",
	})
	waitFor(t, 2*time.Second, func() bool {
		allSent := mockL.getSent()
		last := allSent[len(allSent)-1]
		return bytes.Equal(last.To, adminHash) && containsStr(last.Content, userHex)
	})

	// 8. Admin revokes access: /bridge revoke <userHex>
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: adminHash,
		MessageID:  []byte("admin-revoke-cmd"),
		Title:      "Command",
		Content:    "/bridge revoke " + userHex,
	})
	waitFor(t, 2*time.Second, func() bool {
		allSent := mockL.getSent()
		last := allSent[len(allSent)-1]
		return bytes.Equal(last.To, adminHash) && containsStr(last.Content, "removed from whitelist")
	})

	if store.IsAllowed(userHex) {
		t.Fatalf("userHex still allowed after /bridge revoke")
	}

	// 9. Revoked user attempts to message their chat again -> rejected by allowlist
	promptsBefore := len(mockA.getPrompts())
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  userChatHash,
		SenderHash: userHash,
		MessageID:  []byte("user-msg-after-revoke"),
		Title:      "Hello",
		Content:    "Am I still here?",
	})
	time.Sleep(150 * time.Millisecond)
	if len(mockA.getPrompts()) != promptsBefore {
		t.Fatalf("revoked user's message was processed by agent!")
	}
}

func TestAutoClaimAdminFromDynamicChat(t *testing.T) {
	b, mockL, mockA, store, initialID, _ := setupTestBridge(t)
	defer b.Close()

	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())
	initHex := hex.EncodeToString(initialHash)

	// Simulate an existing database where root identity was unowned and admin is empty
	_ = store.SaveChat(storage.ChatRecord{
		Hash:      initHex,
		Owner:     "",
		CreatedAt: time.Now(),
	})

	userHash := []byte("user_with_dynamic_chat_12345678")
	userHex := hex.EncodeToString(userHash)

	// User creates/has a personal chat
	chatID, _ := rns.NewIdentity()
	chatHash := chatID.DestinationHashFor(reticulumLxmf.FullName())
	chatHex := hex.EncodeToString(chatHash)
	_ = mockL.RegisterIdentity(chatID)
	_ = store.SaveChat(storage.ChatRecord{
		Hash:       chatHex,
		PrivateKey: hex.EncodeToString(chatID.PrivateKey()),
		Owner:      userHex,
		CreatedAt:  time.Now(),
	})

	// User invokes /bridge allow from their personal chat
	newPeerHash := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00}
	newPeerHex := hex.EncodeToString(newPeerHash)

	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  chatHash,
		SenderHash: userHash,
		MessageID:  []byte("allow-from-dynamic"),
		Title:      "Allow",
		Content:    "/bridge allow " + newPeerHex,
	})

	waitFor(t, 2*time.Second, func() bool {
		for _, s := range mockL.getSent() {
			if bytes.Equal(s.To, userHash) && containsStr(s.Content, "Added user") {
				return true
			}
		}
		return false
	})

	// Verify user became admin and claimed root identity
	if store.GetAdmin() != userHex {
		t.Fatalf("expected admin to be %s, got %s", userHex, store.GetAdmin())
	}
	rootRec, found := store.GetChat(initHex)
	if !found || rootRec.Owner != userHex {
		t.Fatalf("expected root identity owner to be %s, got %+v", userHex, rootRec)
	}
	_ = mockA
}
