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

// stopReasonCancelled is the ACP stop reason of a cancelled turn.
const stopReasonCancelled = "cancelled"

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
	log.Printf("[Queue] Processing message for %s from %s (id: %x, isReply: %v, replyTo: %x, replyQuote: %q): %q",
		q.localHex, q.peerHex, msg.MessageID, msg.IsReply(), msg.ReplyTo, msg.ReplyQuote, text)

	if q.toolCommand(text) {
		return
	}

	// Command: /help
	if text == "/help" {
		reply := "**LXMF-ACP Agent Bridge commands:**\n\n" +
			"- `/new` — Start a new chat with a fresh conversation session\n" +
			"- `/cancel` — Cancel current generation\n" +
			"- `/help` — Show this message\n" +
			"- `/tools off|compact|verbose` — Set tool display for this chat\n" +
			"- `/tool <number>` — Show details of a recent tool call"
		if q.bridge.isAdmin(q.peerHex) {
			reply += "\n\n**Admin commands:**\n" +
				"- `/allow <hash>` — Add user to whitelist and open a personal chat with them\n" +
				"- `/revoke <hash>` — Remove user from whitelist\n" +
				"- `/whitelist` — Show whitelisted users"
		}
		_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Help", reply)
		return
	}

	// Command: /new
	if text == "/new" {
		q.bridge.handleNewCommand(msg)
		return
	}

	// Command: /allow
	if strings.HasPrefix(text, "/allow") {
		q.bridge.handleAllowCommand(msg)
		return
	}

	// Command: /revoke
	if strings.HasPrefix(text, "/revoke") {
		q.bridge.handleRevokeCommand(msg)
		return
	}

	// Command: /whitelist or /allowlist
	if text == "/whitelist" || text == "/allowlist" {
		q.bridge.handleWhitelistCommand(msg)
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

	// A reassembled multipart prompt is passed verbatim (whitespace intact).
	promptText := text
	if msg.Multipart {
		promptText = msg.Content
	}

	// Enrich prompt with reply quote context if message is a reply
	promptText = q.bridge.formatPromptWithReply(msg, promptText)
	promptText = strings.ToValidUTF8(promptText, "")

	// The reply is streamed: parts are delivered while the agent is still
	// generating. Waiting for the stream keeps replies of one chat ordered.
	var replyToID []byte
	var replyQuote string
	if q.bridge.cfg.OutboundReplies && len(msg.MessageID) > 0 {
		replyToID = msg.MessageID
		if msg.Content != "" {
			replyQuote = lxmf.MakeQuoteSnippet(msg.Content, 120)
		}
	}
	stream := q.bridge.newReplyStream(q.localHash, q.peerHash, "", replyToID, replyQuote)
	reporter := newToolReporter(stream, q.bridge.store, toolBatchInterval)
	stopReason, err := q.bridge.acpClient.PromptEvents(promptCtx, sessionID, promptText, reporter.Event)
	reporter.Finish(stopReason == stopReasonCancelled || errors.Is(err, context.Canceled))
	switch {
	case err == nil && stopReason == stopReasonCancelled:
		log.Printf("[Bridge] Prompt cancelled for (%s, %s)", q.localHex, q.peerHex)
		stream.Finish(lxmf.StatusCancelled, "", "")
	case err == nil:
		stream.Finish(lxmf.StatusCompleted, "", "")
	case errors.Is(err, context.Canceled):
		log.Printf("[Bridge] Prompt cancelled for (%s, %s)", q.localHex, q.peerHex)
		stream.Finish(lxmf.StatusCancelled, "", "")
	default:
		log.Printf("[Bridge] Prompt error for (%s, %s) (session %s): %v", q.localHex, q.peerHex, sessionID, err)
		stream.Finish(lxmf.StatusFailed, "Agent Error", fmt.Sprintf("Error from agent: %v", err))
	}

	if err := stream.Wait(); err != nil {
		log.Printf("[Bridge] Error sending reply from %s to %s: %v", q.localHex, q.peerHex, err)
		return
	}
	log.Printf("[Bridge] Agent reply from %s to %s delivered", q.localHex, q.peerHex)
}
