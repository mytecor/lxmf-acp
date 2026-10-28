package bridge

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/mytecor/lxmf-acp/internal/config"
	"github.com/mytecor/lxmf-acp/internal/lxmf"
	"github.com/mytecor/lxmf-acp/internal/storage"
	rns "github.com/thatSFguy/reticulum-go/rns"
	reticulumLxmf "github.com/thatSFguy/reticulum-go/lxmf"
)

type LXMFClient interface {
	Start(ctx context.Context) error
	Close() error
	SendFrom(localDestHash, recipientHash []byte, title, content string) error
	Send(recipientHash []byte, title, content string) error
	// SendMessage sends a message with LXMF fields and returns its hash once
	// it is DELIVERED.
	SendMessage(localDestHash, recipientHash []byte, msg lxmf.OutboundMessage) ([]byte, error)
	RegisterIdentity(identity *rns.Identity) error
	Hash() []byte
	HexHash() string
	SetMessageHandler(h lxmf.MessageHandler)
	RestoreKnownIdentities(identities []*rns.KnownIdentity)
	KnownSnapshot() []*rns.KnownIdentity
	PinDestinations(destHashes [][]byte)
	RequestPath(destHash []byte) error
	SetAnnounceHandler(h func(*rns.Announce))
}

type ACPClient interface {
	Connect(ctx context.Context) error
	Close() error
	CanLoadSession() bool
	LoadSession(ctx context.Context, sessionID string) error
	CreateSession(ctx context.Context) (string, error)
	// PromptStream streams agent_message_chunk text to onChunk and returns
	// the turn's stop reason.
	PromptStream(ctx context.Context, sessionID, text string, onChunk func(string)) (string, error)
	CancelPrompt(sessionID string) bool
}

type Bridge struct {
	cfg        *config.Config
	lxmfClient LXMFClient
	acpClient  ACPClient
	store      *storage.Store
	sessions   *SessionManager
	queue      *QueueManager
	assembler  *Assembler
	history    *MessageHistory

	// Outbound part limits (zero selects the defaults).
	maxPartUnits int
	maxPartBytes int

	// Rate limiter for /new command: userHex -> last creation time
	rateMu      sync.Mutex
	lastNewChat map[string]time.Time

	// Deduplication for /new command: messageIDHex -> timestamp
	dedupMu        sync.Mutex
	processedNewID map[string]time.Time
}

func NewBridge(cfg *config.Config, lxmfClient LXMFClient, acpClient ACPClient, store *storage.Store) *Bridge {
	b := &Bridge{
		cfg:            cfg,
		lxmfClient:     lxmfClient,
		acpClient:      acpClient,
		store:          store,
		lastNewChat:    make(map[string]time.Time),
		processedNewID: make(map[string]time.Time),
	}

	b.sessions = NewSessionManager(store, acpClient)
	b.queue = NewQueueManager(b)
	b.assembler = NewAssembler(DefaultAssemblyTimeout)
	b.history = NewMessageHistory(1000)

	return b
}

func (b *Bridge) Start(ctx context.Context) error {
	// 1. Connect to ACP agent
	if err := b.acpClient.Connect(ctx); err != nil {
		log.Printf("[Bridge] Warning: ACP agent connection failed initially (%v). Will retry on first message.", err)
	}

	// 2. Seed in-memory message history from persistent store
	for _, rec := range b.store.GetRecentMessages() {
		b.history.RecordHex(rec.ID, rec.ChatKey, rec.Role, rec.Content, rec.Timestamp)
	}

	// 3. Restore and register all stored identities
	allRecords := b.store.GetAllIdentities()
	for _, rec := range allRecords {
		if rec.PrivateKey == "" {
			continue
		}
		privBytes, err := hex.DecodeString(rec.PrivateKey)
		if err != nil {
			log.Printf("[Bridge] Warning: invalid private key hex for stored identity %s: %v", rec.Hash, err)
			continue
		}
		id, err := rns.IdentityFromPrivateKey(privBytes)
		if err != nil {
			log.Printf("[Bridge] Warning: failed to rebuild stored identity %s: %v", rec.Hash, err)
			continue
		}
		if err := b.lxmfClient.RegisterIdentity(id); err != nil {
			log.Printf("[Bridge] Warning: failed to register stored identity %s: %v", rec.Hash, err)
			continue
		}
		log.Printf("[Bridge] Restored dynamic identity: %s (owner: %s)", rec.Hash, rec.Owner)
	}

	// 3. Sync CLI allowlist into store allowlist if provided
	if len(b.cfg.Allowlist) > 0 {
		for _, a := range b.cfg.Allowlist {
			_, _ = b.store.AddToAllowlist(a)
		}
		if b.store.GetAdmin() == "" {
			_ = b.store.SetAdmin(b.cfg.Allowlist[0])
		}
	}

	// 4. Ensure initial identity is stored with owner
	initHex := b.lxmfClient.HexHash()
	if initHex != "" {
		admin := b.store.GetAdmin()
		if rec, found := b.store.GetIdentity(initHex); !found {
			_ = b.store.SaveIdentity(storage.IdentityRecord{
				Hash:      initHex,
				Owner:     admin,
				CreatedAt: time.Now(),
			})
		} else if rec.Owner == "" && admin != "" {
			rec.Owner = admin
			_ = b.store.SaveIdentity(rec)
		} else if rec.Owner != "" && admin == "" {
			_ = b.store.SetAdmin(rec.Owner)
			_, _ = b.store.AddToAllowlist(rec.Owner)
		}
	}

	// 5. Register inbound message handler
	b.lxmfClient.SetMessageHandler(func(msg *lxmf.InboundMessage) {
		b.handleInbound(msg)
	})

	// 6. Register announce handler to persist peer identities
	b.lxmfClient.SetAnnounceHandler(func(a *rns.Announce) {
		snapshot := b.lxmfClient.KnownSnapshot()
		if len(snapshot) > 0 {
			_ = b.store.SaveKnownIdentities(snapshot)
		}
	})

	// 7. Start LXMF client
	if err := b.lxmfClient.Start(ctx); err != nil {
		return fmt.Errorf("start lxmf client: %w", err)
	}

	// 8. Restore cached peer identities so restarting never drops incoming messages
	storedKnown := b.store.GetKnownIdentities()
	if len(storedKnown) > 0 {
		b.lxmfClient.RestoreKnownIdentities(storedKnown)
		log.Printf("[Bridge] Restored %d known peer identities to Reticulum cache", len(storedKnown))
	}

	// 9. Pin local destinations, chat owner destinations, and allowlist destinations
	var pinned [][]byte
	if initHash := b.lxmfClient.Hash(); len(initHash) > 0 {
		pinned = append(pinned, initHash)
	}
	for _, rec := range allRecords {
		if h, err := hex.DecodeString(rec.Hash); err == nil {
			pinned = append(pinned, h)
		}
		if rec.Owner != "" {
			if o, err := hex.DecodeString(rec.Owner); err == nil {
				pinned = append(pinned, o)
				_ = b.lxmfClient.RequestPath(o)
			}
		}
	}
	for _, a := range b.store.GetAllowlist() {
		if o, err := hex.DecodeString(a); err == nil {
			pinned = append(pinned, o)
			_ = b.lxmfClient.RequestPath(o)
		}
	}
	if len(pinned) > 0 {
		b.lxmfClient.PinDestinations(pinned)
	}

	// 10. Periodically discard incomplete inbound multipart chains
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.assembler.Sweep()
			}
		}
	}()

	return nil
}

func (b *Bridge) isAllowed(senderHex string) bool {
	if !b.cfg.IsAllowed(senderHex) {
		return false
	}
	return b.store.IsAllowed(senderHex)
}

func (b *Bridge) isAdmin(senderHex string) bool {
	admin := b.store.GetAdmin()
	if admin == "" {
		initHex := b.lxmfClient.HexHash()
		if initHex != "" {
			if rec, found := b.store.GetIdentity(initHex); found && rec.Owner != "" {
				admin = rec.Owner
				_ = b.store.SetAdmin(admin)
			}
		}
	}
	return admin != "" && strings.EqualFold(admin, senderHex)
}

func (b *Bridge) handleInbound(msg *lxmf.InboundMessage) {
	localHex := hex.EncodeToString(msg.LocalHash)
	senderHex := hex.EncodeToString(msg.SenderHash)

	// Check allowlist
	if !b.isAllowed(senderHex) {
		log.Printf("[Bridge] Rejected message from unallowed peer: %s", senderHex)
		return
	}

	// Check identity ownership: chat must only accept messages from its owner
	idRecord, found := b.store.GetIdentity(localHex)
	if found {
		if idRecord.Owner == "" {
			// First user to message claims ownership & admin if unassigned
			idRecord.Owner = senderHex
			_ = b.store.SaveIdentity(idRecord)
			if b.store.GetAdmin() == "" {
				_ = b.store.SetAdmin(senderHex)
			}
			log.Printf("[Bridge] Chat %s claimed by owner: %s", localHex, senderHex)
		} else if !strings.EqualFold(idRecord.Owner, senderHex) {
			log.Printf("[Bridge] Rejected message for identity %s: sender %s is not the owner (%s)", localHex, senderHex, idRecord.Owner)
			// Guide allowed user to their own personal chat or create one
			userChats := b.store.GetIdentitiesForOwner(senderHex)
			if len(userChats) > 0 {
				notice := fmt.Sprintf("This chat belongs to another user. Please use your personal chat: `<%s>`", userChats[0].Hash)
				_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", notice)
			} else {
				_, _ = b.createDedicatedChatFor(msg.SenderHash, senderHex, msg.LocalHash)
			}
			return
		}
	}

	chatKey := localHex + ":" + senderHex

	// Record raw part into memory history cache
	if len(msg.MessageID) > 0 && msg.Content != "" && b.history != nil {
		b.history.Record(msg.MessageID, chatKey, "user", msg.Content)
	}

	// Reassemble multipart chains; only complete prompts reach the queue.
	ready := b.assembler.Process(msg)
	if ready == nil {
		return
	}

	// Record complete assembled message in history and persistent store
	if len(ready.MessageID) > 0 && ready.Content != "" {
		if b.history != nil {
			b.history.Record(ready.MessageID, chatKey, "user", ready.Content)
		}
		if b.store != nil {
			_ = b.store.SaveMessage(storage.MessageEntry{
				ID:        hex.EncodeToString(ready.MessageID),
				ChatKey:   chatKey,
				Role:      "user",
				Content:   ready.Content,
				Timestamp: time.Now(),
			})
		}
	}

	b.queue.Enqueue(ready)
}

// newReplyStream starts a streamed, possibly multipart, reply in a chat.
func (b *Bridge) newReplyStream(localHash, peerHash []byte, title string, replyTo []byte, replyQuote string) *replyStream {
	return newReplyStream(b.lxmfClient, localHash, peerHash, title, replyTo, replyQuote, b.history, b.store, NewSegmenter(b.maxPartUnits, b.maxPartBytes))
}

// formatPromptWithReply enriches prompt text with quoted context if msg is a reply to a previous message.
func (b *Bridge) formatPromptWithReply(msg *lxmf.InboundMessage, baseText string) string {
	baseText = strings.ToValidUTF8(baseText, "")
	if len(msg.ReplyTo) == 0 && msg.ReplyQuote == "" {
		return baseText
	}

	replyIDHex := hex.EncodeToString(msg.ReplyTo)
	var quoteText string
	var role string

	if len(msg.ReplyTo) > 0 && b.history != nil {
		if entry, ok := b.history.Get(msg.ReplyTo); ok {
			quoteText = entry.Content
			role = entry.Role
		}
	}

	if quoteText == "" && msg.ReplyQuote != "" {
		quoteText = msg.ReplyQuote
	}

	quoteText = strings.ToValidUTF8(quoteText, "")

	if quoteText == "" && replyIDHex != "" {
		return fmt.Sprintf("[In reply to message <%s>]\n\n%s", replyIDHex, baseText)
	}

	if quoteText != "" {
		quotedRunes := []rune(quoteText)
		quoted := quoteText
		if len(quotedRunes) > 500 {
			quoted = string(quotedRunes[:497]) + "..."
		}
		lines := strings.Split(quoted, "\n")
		for i, l := range lines {
			lines[i] = "> " + l
		}
		blockQuote := strings.Join(lines, "\n")

		if role != "" {
			return fmt.Sprintf("[In reply to %s:\n%s]\n\n%s", role, blockQuote, baseText)
		}
		return fmt.Sprintf("[In reply to:\n%s]\n\n%s", blockQuote, baseText)
	}

	return baseText
}

func (b *Bridge) handleNewCommand(msg *lxmf.InboundMessage) {
	senderHex := hex.EncodeToString(msg.SenderHash)
	localHex := hex.EncodeToString(msg.LocalHash)

	// 1. Deduplication by incoming LXMF message ID
	msgIDHex := hex.EncodeToString(msg.MessageID)
	if msgIDHex != "" {
		b.dedupMu.Lock()
		if _, seen := b.processedNewID[msgIDHex]; seen {
			b.dedupMu.Unlock()
			log.Printf("[Bridge] Duplicate /new command with message ID %s received from %s, ignoring", msgIDHex, senderHex)
			return
		}
		b.processedNewID[msgIDHex] = time.Now()
		// Clean older entries if map grows large
		if len(b.processedNewID) > 5000 {
			now := time.Now()
			for k, t := range b.processedNewID {
				if now.Sub(t) > time.Hour {
					delete(b.processedNewID, k)
				}
			}
		}
		b.dedupMu.Unlock()
	}

	// 2. Frequency limit (cooldown) per user
	cooldown := time.Duration(b.cfg.NewChatCooldownSec) * time.Second
	if cooldown > 0 {
		b.rateMu.Lock()
		lastTime, exists := b.lastNewChat[senderHex]
		if exists && time.Since(lastTime) < cooldown {
			b.rateMu.Unlock()
			log.Printf("[Bridge] Rate limit exceeded for /new from %s (cooldown %v)", senderHex, cooldown)
			_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "Rate Limit", "Rate limit exceeded. Please wait before creating another chat.")
			return
		}
		b.rateMu.Unlock()
	}

	// 3. Maximum identities quota per user
	if b.cfg.MaxIdentitiesPerUser > 0 {
		count := b.store.CountIdentitiesForOwner(senderHex)
		if count >= b.cfg.MaxIdentitiesPerUser {
			log.Printf("[Bridge] Maximum identities reached for %s (count: %d, max: %d)", senderHex, count, b.cfg.MaxIdentitiesPerUser)
			_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "Limit Reached", fmt.Sprintf("Maximum number of chats (%d) reached.", b.cfg.MaxIdentitiesPerUser))
			return
		}
	}

	// 4. Generate new LXMF identity
	newID, err := rns.NewIdentity()
	if err != nil {
		log.Printf("[Bridge] Failed to generate identity: %v", err)
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "Error", "Failed to generate new chat identity.")
		return
	}

	newDestHash := newID.DestinationHashFor(reticulumLxmf.FullName())
	newDestHex := hex.EncodeToString(newDestHash)

	// 5. Atomic persistence BEFORE sending welcome message
	record := storage.IdentityRecord{
		Hash:       newDestHex,
		PrivateKey: hex.EncodeToString(newID.PrivateKey()),
		Owner:      senderHex,
		CreatedAt:  time.Now(),
	}
	if err := b.store.SaveIdentity(record); err != nil {
		log.Printf("[Bridge] Failed to atomically save identity %s: %v", newDestHex, err)
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "Error", "Failed to save new chat.")
		return
	}

	// 6. Register delivery destination
	if err := b.lxmfClient.RegisterIdentity(newID); err != nil {
		log.Printf("[Bridge] Failed to register delivery destination %s: %v", newDestHex, err)
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "Error", "Failed to register new chat.")
		return
	}

	// Update rate limiter timestamp on success
	b.rateMu.Lock()
	b.lastNewChat[senderHex] = time.Now()
	b.rateMu.Unlock()

	log.Printf("[Bridge] Successfully created new chat %s for user %s (requested from chat %s)", newDestHex, senderHex, localHex)

	// 7. Inform originating chat of the new chat destination so the user sees immediate feedback
	if !bytes.Equal(msg.LocalHash, newDestHash) {
		confirmMsg := fmt.Sprintf("Started a new conversation.\n\nChat destination: `<%s>`", newDestHex)
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", confirmMsg)
	}

	// 8. Send welcome message from the new identity using opportunistic delivery
	welcomeContent := fmt.Sprintf("Started a new conversation with **%s**.\n\nSend a message to begin.", b.cfg.DisplayName)
	err = b.lxmfClient.SendFrom(newDestHash, msg.SenderHash, "", welcomeContent)
	if err != nil {
		log.Printf("[Bridge] Failed to deliver welcome message from %s to %s: %v", newDestHex, senderHex, err)
		// Upon delivery failure, report destination hash in source chat (wrapped in backticks so markdown does not strip it as an HTML tag)
		errMsg := fmt.Sprintf("New chat created, but failed to deliver welcome message. Chat destination: `<%s>`", newDestHex)
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "New Chat Info", errMsg)
		return
	}
}

func (b *Bridge) createDedicatedChatFor(targetHash []byte, targetHex string, notifyLocalHash []byte) (string, error) {
	// 1. Generate new LXMF identity
	newID, err := rns.NewIdentity()
	if err != nil {
		return "", fmt.Errorf("generate identity: %w", err)
	}

	newDestHash := newID.DestinationHashFor(reticulumLxmf.FullName())
	newDestHex := hex.EncodeToString(newDestHash)

	// 2. Persist identity
	record := storage.IdentityRecord{
		Hash:       newDestHex,
		PrivateKey: hex.EncodeToString(newID.PrivateKey()),
		Owner:      targetHex,
		CreatedAt:  time.Now(),
	}
	if err := b.store.SaveIdentity(record); err != nil {
		return "", fmt.Errorf("save identity: %w", err)
	}

	// 3. Register destination
	if err := b.lxmfClient.RegisterIdentity(newID); err != nil {
		return "", fmt.Errorf("register identity: %w", err)
	}

	// 4. Pin destinations and request path
	b.lxmfClient.PinDestinations([][]byte{newDestHash, targetHash})
	_ = b.lxmfClient.RequestPath(targetHash)

	// 5. Send welcome greeting from new identity directly to target
	welcomeContent := fmt.Sprintf("Started a new conversation with **%s**.\n\nSend a message to begin.", b.cfg.DisplayName)
	err = b.lxmfClient.SendFrom(newDestHash, targetHash, "", welcomeContent)
	if err != nil {
		log.Printf("[Bridge] Warning: failed to deliver welcome message from %s to %s: %v", newDestHex, targetHex, err)
	}

	// 6. Optional notification in the local chat where message was originally received
	if len(notifyLocalHash) > 0 {
		notice := fmt.Sprintf("This chat belongs to another user. Your personal chat has been created: `<%s>`. Please message there.", newDestHex)
		_ = b.lxmfClient.SendFrom(notifyLocalHash, targetHash, "", notice)
	}

	return newDestHex, nil
}

func (b *Bridge) handleAllowCommand(msg *lxmf.InboundMessage) {
	senderHex := hex.EncodeToString(msg.SenderHash)
	if !b.isAdmin(senderHex) {
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", "Permission denied: administrative commands are only available to the agent administrator.")
		return
	}

	parts := strings.Fields(msg.Content)
	if len(parts) < 2 {
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", "Usage: `/allow <destination_hash>`")
		return
	}

	targetHex := strings.ToLower(strings.Trim(parts[1], "<> \t\r\n"))
	targetBytes, err := hex.DecodeString(targetHex)
	if err != nil || len(targetBytes) != 16 {
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", "Invalid destination hash. Expected 32 hexadecimal characters (16 bytes).")
		return
	}

	added, err := b.store.AddToAllowlist(targetHex)
	if err != nil {
		log.Printf("[Bridge] Failed to add %s to allowlist: %v", targetHex, err)
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", "Failed to update whitelist.")
		return
	}

	newDestHex, err := b.createDedicatedChatFor(targetBytes, targetHex, nil)
	if err != nil {
		log.Printf("[Bridge] Failed to create dedicated chat for %s: %v", targetHex, err)
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", fmt.Sprintf("Added `<%s>` to whitelist, but failed to create personal chat: %v", targetHex, err))
		return
	}

	status := "Added"
	if !added {
		status = "Already in whitelist;"
	}
	confirm := fmt.Sprintf("%s user `<%s>` to whitelist.\n\nCreated dedicated chat and sent greeting from: `<%s>`", status, targetHex, newDestHex)
	_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", confirm)
}

func (b *Bridge) handleRevokeCommand(msg *lxmf.InboundMessage) {
	senderHex := hex.EncodeToString(msg.SenderHash)
	if !b.isAdmin(senderHex) {
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", "Permission denied: administrative commands are only available to the agent administrator.")
		return
	}

	parts := strings.Fields(msg.Content)
	if len(parts) < 2 {
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", "Usage: `/revoke <destination_hash>`")
		return
	}

	targetHex := strings.ToLower(strings.Trim(parts[1], "<> \t\r\n"))
	removed, err := b.store.RemoveFromAllowlist(targetHex)
	if err != nil {
		log.Printf("[Bridge] Failed to remove %s from allowlist: %v", targetHex, err)
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", "Failed to update whitelist.")
		return
	}

	if !removed {
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", fmt.Sprintf("User `<%s>` was not found in whitelist.", targetHex))
		return
	}

	_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", fmt.Sprintf("User `<%s>` removed from whitelist.", targetHex))
}

func (b *Bridge) handleWhitelistCommand(msg *lxmf.InboundMessage) {
	senderHex := hex.EncodeToString(msg.SenderHash)
	if !b.isAdmin(senderHex) {
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", "Permission denied: administrative commands are only available to the agent administrator.")
		return
	}

	list := b.store.GetAllowlist()
	if len(list) == 0 {
		_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", "Whitelist is empty (public access).")
		return
	}

	admin := b.store.GetAdmin()
	var bld strings.Builder
	bld.WriteString(fmt.Sprintf("**Whitelist (%d users):**\n\n", len(list)))
	for _, h := range list {
		if strings.EqualFold(h, admin) {
			bld.WriteString(fmt.Sprintf("- `<%s>` *(Admin)*\n", h))
		} else {
			bld.WriteString(fmt.Sprintf("- `<%s>`\n", h))
		}
	}

	_ = b.lxmfClient.SendFrom(msg.LocalHash, msg.SenderHash, "", bld.String())
}

func (b *Bridge) Close() error {
	if snapshot := b.lxmfClient.KnownSnapshot(); len(snapshot) > 0 {
		_ = b.store.SaveKnownIdentities(snapshot)
	}
	_ = b.lxmfClient.Close()
	_ = b.acpClient.Close()
	return nil
}
