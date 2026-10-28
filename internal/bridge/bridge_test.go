package bridge

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
}

type mockLXMF struct {
	mu           sync.Mutex
	initialID    *rns.Identity
	identities   map[string]*rns.Identity
	sent         []sentMsg
	handler      lxmf.MessageHandler
	failWelcome  bool
	failLocalHex string
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

func (m *mockACP) Prompt(ctx context.Context, sessionID, text string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prompts[sessionID] = append(m.prompts[sessionID], text)
	return "Agent reply: " + text, nil
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

// Scenario 1: /new from initial chat creates a new identity and independent ACP session.
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

	// 2. Send /new from initial chat
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("cmd-new-1"),
		Title:      "Command",
		Content:    "/new",
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
	for _, rec := range store.GetAllIdentities() {
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

	// Verify ACP agent was NOT called with /new
	for sessID, prompts := range mockA.prompts {
		for _, p := range prompts {
			if p == "/new" {
				t.Fatalf("/new was sent to ACP agent session %s!", sessID)
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

// Scenario 2: /new from a newly created chat behaves identically, without inheriting context.
func TestScenario2_NewFromDynamicChat(t *testing.T) {
	b, mockL, mockA, store, initialID, userHash := setupTestBridge(t)
	defer b.Close()

	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())
	initialHex := hex.EncodeToString(initialHash)
	userHex := hex.EncodeToString(userHash)

	// Create first dynamic chat via /new
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("cmd-new-1"),
		Title:      "Command",
		Content:    "/new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2
	})

	var chat1Hash []byte
	var chat1Hex string
	for _, rec := range store.GetAllIdentities() {
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

	// Now send /new FROM chat1!
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  chat1Hash,
		SenderHash: userHash,
		MessageID:  []byte("cmd-new-2"),
		Title:      "Command",
		Content:    "/new",
	})

	// Wait for 3rd identity to be created
	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 3
	})

	// chat1 session must remain unchanged
	sessChat1After, _ := store.GetSession(chat1Hex, userHex)
	if sessChat1After != sessChat1 {
		t.Fatalf("chat1 session was altered by /new! got %s, want %s", sessChat1After, sessChat1)
	}

	// Find the 3rd identity
	var chat2Hash []byte
	var chat2Hex string
	for _, rec := range store.GetAllIdentities() {
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

	// Create a chat via /new
	mockL1.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("cmd-new-1"),
		Title:      "Command",
		Content:    "/new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL1.identityCount() == 2
	})

	var dynamicHash []byte
	var dynamicHex string
	for _, rec := range store.GetAllIdentities() {
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

// Scenario 4: Re-delivery of the same /new command does not create an extra chat.
func TestScenario4_DuplicateNewCommandDoesNotCreateDuplicateChat(t *testing.T) {
	b, mockL, _, _, initialID, userHash := setupTestBridge(t)
	defer b.Close()

	initialHash := initialID.DestinationHashFor(reticulumLxmf.FullName())

	duplicateMsgID := []byte("duplicate-message-id-1234567890123456")

	// First delivery of /new
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  duplicateMsgID,
		Title:      "Command",
		Content:    "/new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2
	})

	// Second delivery of identical /new (same message ID)
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  duplicateMsgID,
		Title:      "Command",
		Content:    "/new",
	})

	time.Sleep(150 * time.Millisecond)

	// Identity count must remain 2!
	if mockL.identityCount() != 2 {
		t.Fatalf("duplicate /new created additional identity! got %d identities, want 2", mockL.identityCount())
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
		Content:    "/new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2
	})

	var dynamicHash []byte
	for _, rec := range store.GetAllIdentities() {
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
		Content:    "/new",
	})

	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2 && len(mockL.getSent()) >= 1
	})

	// Find the created identity
	var newChatHex string
	for _, rec := range store.GetAllIdentities() {
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

	// 1st /new -> succeeds
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("new-1"),
		Title:      "Command",
		Content:    "/new",
	})
	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 2
	})

	// 2nd /new immediately -> triggers rate limit!
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("new-2"),
		Title:      "Command",
		Content:    "/new",
	})
	time.Sleep(150 * time.Millisecond)

	// Should not have created a 3rd identity
	if mockL.identityCount() != 2 {
		t.Fatalf("rate limit did not block second /new! count = %d", mockL.identityCount())
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

	// 2nd /new after cooldown -> succeeds (count becomes 3 = initial + 2 dynamic)
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("new-3"),
		Title:      "Command",
		Content:    "/new",
	})
	waitFor(t, 2*time.Second, func() bool {
		return mockL.identityCount() == 3
	})

	// Reset cooldown again to test quota
	b.rateMu.Lock()
	b.lastNewChat[hex.EncodeToString(userHash)] = time.Now().Add(-10 * time.Second)
	b.rateMu.Unlock()

	// 3rd /new -> exceeds quota of 2!
	mockL.handler(&lxmf.InboundMessage{
		LocalHash:  initialHash,
		SenderHash: userHash,
		MessageID:  []byte("new-4"),
		Title:      "Command",
		Content:    "/new",
	})
	time.Sleep(150 * time.Millisecond)

	// Count must still be 3
	if mockL.identityCount() != 3 {
		t.Fatalf("quota did not block third /new! count = %d", mockL.identityCount())
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
