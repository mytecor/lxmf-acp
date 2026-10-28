package bridge

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/mytecor/lxmf-acp/internal/lxmf"
)

type peerQueue struct {
	localHex  string
	peerHex   string
	localHash []byte
	peerHash  []byte
	ch        chan *lxmf.InboundMessage
	bridge    *Bridge
}

type QueueManager struct {
	mu      sync.Mutex
	workers map[string]*peerQueue
	bridge  *Bridge
}

func NewQueueManager(bridge *Bridge) *QueueManager {
	return &QueueManager{
		workers: make(map[string]*peerQueue),
		bridge:  bridge,
	}
}

func (qm *QueueManager) Enqueue(msg *lxmf.InboundMessage) {
	localHex := hex.EncodeToString(msg.LocalHash)
	peerHex := hex.EncodeToString(msg.SenderHash)
	queueKey := localHex + ":" + peerHex

	qm.mu.Lock()
	q, ok := qm.workers[queueKey]
	if !ok {
		q = &peerQueue{
			localHex:  localHex,
			peerHex:   peerHex,
			localHash: append([]byte(nil), msg.LocalHash...),
			peerHash:  append([]byte(nil), msg.SenderHash...),
			ch:        make(chan *lxmf.InboundMessage, 50),
			bridge:    qm.bridge,
		}
		qm.workers[queueKey] = q
		go q.run()
	}
	qm.mu.Unlock()

	select {
	case q.ch <- msg:
	default:
		log.Printf("[Queue] Warning: message queue full for conversation (%s, %s), dropping message", localHex, peerHex)
	}
}

func (q *peerQueue) run() {
	log.Printf("[Queue] Worker started for conversation (%s, %s)", q.localHex, q.peerHex)

	for msg := range q.ch {
		q.processMessage(msg)
	}
}

func (q *peerQueue) processMessage(msg *lxmf.InboundMessage) {
	text := strings.TrimSpace(msg.Content)
	if text == "" {
		return
	}

	ctx := context.Background()
	log.Printf("[Queue] Processing message for %s from %s: %q", q.localHex, q.peerHex, text)

	// Command: /help
	if text == "/help" {
		reply := "LXMF-ACP Agent Bridge commands:\n" +
			"/new - Start a new chat with a fresh conversation session\n" +
			"/cancel - Cancel current generation\n" +
			"/help - Show this message"
		_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Help", reply)
		return
	}

	// Command: /new
	if text == "/new" {
		q.bridge.handleNewCommand(msg)
		return
	}

	// Command: /cancel
	if text == "/cancel" {
		sessionID, found := q.bridge.store.GetSession(q.localHex, q.peerHex)
		if found && sessionID != "" {
			cancelled := q.bridge.acpClient.CancelPrompt(sessionID)
			if cancelled {
				_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Cancelled", "Current prompt was cancelled.")
				return
			}
		}
		_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Cancelled", "No active generation to cancel.")
		return
	}

	// Regular message -> send to ACP
	sessionID, err := q.bridge.sessions.GetOrCreateSession(ctx, q.localHex, q.peerHex)
	if err != nil {
		log.Printf("[Bridge] Error getting session for (%s, %s): %v", q.localHex, q.peerHex, err)
		_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Error", "Could not create or retrieve ACP session.")
		return
	}

	// Prompt execution with timeout
	promptCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	reply, err := q.bridge.acpClient.Prompt(promptCtx, sessionID, text)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Printf("[Bridge] Prompt cancelled for (%s, %s)", q.localHex, q.peerHex)
			return
		}
		log.Printf("[Bridge] Prompt error for (%s, %s) (session %s): %v", q.localHex, q.peerHex, sessionID, err)
		_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Agent Error", fmt.Sprintf("Error from agent: %v", err))
		return
	}

	reply = strings.TrimSpace(reply)
	if reply == "" {
		reply = "(Empty response from agent)"
	}

	log.Printf("[Bridge] Sending agent reply from %s to %s (%d bytes)", q.localHex, q.peerHex, len(reply))
	if err := q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Reply", reply); err != nil {
		log.Printf("[Bridge] Error sending reply from %s to %s: %v", q.localHex, q.peerHex, err)
	}
}
