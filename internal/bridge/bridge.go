package bridge

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
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

	return b
}

func (b *Bridge) Start(ctx context.Context) error {
	// 1. Connect to ACP agent
	if err := b.acpClient.Connect(ctx); err != nil {
		log.Printf("[Bridge] Warning: ACP agent connection failed initially (%v). Will retry on first message.", err)
	}

	// 2. Restore and register all stored identities
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

	// 3. Ensure initial identity is stored with public owner ("")
	initHex := b.lxmfClient.HexHash()
	if initHex != "" {
		if _, found := b.store.GetIdentity(initHex); !found {
			_ = b.store.SaveIdentity(storage.IdentityRecord{
				Hash:      initHex,
				Owner:     "",
				CreatedAt: time.Now(),
			})
		}
	}

	// 4. Register inbound message handler
	b.lxmfClient.SetMessageHandler(func(msg *lxmf.InboundMessage) {
		b.handleInbound(msg)
	})

	// 5. Start LXMF client
	if err := b.lxmfClient.Start(ctx); err != nil {
		return fmt.Errorf("start lxmf client: %w", err)
	}

	// 6. Periodically discard incomplete inbound multipart chains
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

func (b *Bridge) handleInbound(msg *lxmf.InboundMessage) {
	localHex := hex.EncodeToString(msg.LocalHash)
	senderHex := hex.EncodeToString(msg.SenderHash)

	// Check allowlist
	if !b.cfg.IsAllowed(senderHex) {
		log.Printf("[Bridge] Rejected message from unallowed peer: %s", senderHex)
		return
	}

	// Check identity ownership: dynamic identity must only accept messages from its owner
	idRecord, found := b.store.GetIdentity(localHex)
	if found && idRecord.Owner != "" {
		if idRecord.Owner != senderHex {
			log.Printf("[Bridge] Rejected message for identity %s: sender %s is not the owner (%s)", localHex, senderHex, idRecord.Owner)
			return
		}
	}

	// Reassemble multipart chains; only complete prompts reach the queue.
	ready := b.assembler.Process(msg)
	if ready == nil {
		return
	}

	b.queue.Enqueue(ready)
}

// newReplyStream starts a streamed, possibly multipart, reply in a chat.
func (b *Bridge) newReplyStream(localHash, peerHash []byte, title string) *replyStream {
	return newReplyStream(b.lxmfClient, localHash, peerHash, title, NewSegmenter(b.maxPartUnits, b.maxPartBytes))
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

	// 7. Send welcome message from the new identity using opportunistic delivery
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

func (b *Bridge) Close() error {
	_ = b.lxmfClient.Close()
	_ = b.acpClient.Close()
	return nil
}
