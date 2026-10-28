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
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewQueueManager(bridge *Bridge) *QueueManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &QueueManager{
		workers: make(map[string]*peerQueue),
		bridge:  bridge,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (qm *QueueManager) Enqueue(msg *lxmf.InboundMessage) {
	localHex := hex.EncodeToString(msg.LocalHash)
	peerHex := hex.EncodeToString(msg.SenderHash)
	queueKey := localHex + ":" + peerHex

	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.ctx.Err() != nil {
		return
	}
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
	select {
	case q.ch <- msg:
	default:
		log.Printf("[Queue] Warning: message queue full for conversation (%s, %s), dropping message", localHex, peerHex)
	}
}

func (qm *QueueManager) Close() { qm.cancel() }

func (q *peerQueue) run() {
	log.Printf("[Queue] Worker started for conversation (%s, %s)", q.localHex, q.peerHex)
	qm := q.bridge.queue
	key := q.localHex + ":" + q.peerHex
	defer func() {
		qm.mu.Lock()
		if qm.workers[key] == q {
			delete(qm.workers, key)
		}
		qm.mu.Unlock()
	}()
	timeout := q.bridge.cfg.QueueIdleTimeout
	var idle <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		defer timer.Stop()
		idle = timer.C
	}
	for {
		select {
		case <-qm.ctx.Done():
			return
		case msg := <-q.ch:
			q.processMessage(msg)
			if timer != nil {
				timer.Reset(timeout)
			}
		case <-idle:
			// Keep this worker registered during detach. Concurrent arrivals go
			// into its queue and cannot race a new worker's session/load.
			qm.mu.Lock()
			empty := len(q.ch) == 0
			qm.mu.Unlock()
			if empty {
				if err := q.detachSession(qm.ctx); err != nil {
					log.Printf("[Queue] Idle detach failed for %s: %v", key, err)
				} else {
					qm.mu.Lock()
					if len(q.ch) == 0 {
						delete(qm.workers, key)
						qm.mu.Unlock()
						log.Printf("[Queue] Released idle conversation %s", key)
						return
					}
					qm.mu.Unlock()
				}
			}
			timer.Reset(timeout)
		}
	}
}

func (q *peerQueue) detachSession(ctx context.Context) error {
	detacher, ok := q.bridge.acpClient.(interface {
		DetachSession(context.Context, string) error
	})
	if !ok {
		return nil
	}
	id, found := q.bridge.store.GetSession(q.localHex, q.peerHex)
	if !found {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return detacher.DetachSession(ctx, id)
}

func (q *peerQueue) processMessage(msg *lxmf.InboundMessage) {
	text := strings.TrimSpace(msg.Content)
	if text == "" {
		return
	}
	// Only /bridge owns local commands. Every other slash command belongs
	// to the upstream agent; forward can also escape the /bridge namespace.
	localCommand := false
	forwarded := false
	if strings.Fields(text)[0] == "/bridge" {
		payload := strings.TrimSpace(strings.TrimPrefix(text, "/bridge"))
		if payload == "" {
			payload = "help"
		}
		verb := strings.Fields(payload)[0]
		if verb == "forward" {
			text = strings.TrimSpace(strings.TrimPrefix(payload, "forward"))
			if text == "" {
				_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Help", "Usage: /bridge forward <text or command>")
				return
			}
			forwarded = true
		} else {
			switch verb {
			case "help", "new", "cancel", "tools", "tool", "allow", "revoke", "whitelist", "allowlist":
				localCommand = true
				text = "/" + payload
				copyMsg := *msg
				copyMsg.Content = text
				msg = &copyMsg
			default:
				_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Help", "Unknown bridge command. Use /bridge help.")
				return
			}
		}
	}
	command := strings.Fields(text)[0]

	ctx := q.bridge.queue.ctx
	defer func() {
		now := time.Now()
		q.bridge.lxmfClient.SetIdentityLastActivity(q.localHash, now)
		if err := q.bridge.store.TouchChat(q.localHex, now); err != nil {
			log.Printf("[Queue] Save chat activity: %v", err)
		}
	}()
	log.Printf("[Queue] Processing message for %s from %s (id: %x, isReply: %v, replyTo: %x, replyQuote: %q): %q",
		q.localHex, q.peerHex, msg.MessageID, msg.IsReply(), msg.ReplyTo, msg.ReplyQuote, text)

	if localCommand && q.toolCommand(text) {
		return
	}

	// Command: /help
	if localCommand && text == "/help" {
		reply := "**LXMF-ACP Agent Bridge commands:**\n\n" +
			"- `/bridge new` — Start a new chat with a fresh conversation session\n" +
			"- `/bridge cancel` — Cancel current generation\n" +
			"- `/bridge help` — Show this message\n" +
			"- `/bridge tools off|compact|verbose` — Set tool display for this chat\n" +
			"- `/bridge tool <number>` — Show details of a recent tool call\n\n" +
			"Only `/bridge` commands run locally; all other commands go upstream.\n" +
			"Upstream commands pass through: `/hydra` lists Hydra commands; `/hydra kill` stops its agent while retaining history; `/hydra restart` restarts it.\n" +
			"Use `/bridge forward <text or command>` to send text upstream verbatim."
		if q.bridge.isAdmin(q.peerHex) {
			reply += "\n\n**Admin commands:**\n" +
				"- `/bridge allow <hash>` — Add user to whitelist and open a personal chat with them\n" +
				"- `/bridge revoke <hash>` — Remove user from whitelist\n" +
				"- `/bridge whitelist` — Show whitelisted users"
		}
		_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Help", reply)
		return
	}

	// Command: /new
	if localCommand && text == "/new" {
		q.bridge.handleNewCommand(msg)
		return
	}

	// Command: /allow
	if localCommand && command == "/allow" {
		q.bridge.handleAllowCommand(msg)
		return
	}

	// Command: /revoke
	if localCommand && command == "/revoke" {
		q.bridge.handleRevokeCommand(msg)
		return
	}

	// Command: /whitelist or /allowlist
	if localCommand && (text == "/whitelist" || text == "/allowlist") {
		q.bridge.handleWhitelistCommand(msg)
		return
	}

	// Command: /cancel
	if localCommand && text == "/cancel" {
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
	if localCommand {
		_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Help", "Invalid bridge command arguments. Use /bridge help.")
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
	if msg.Multipart && !forwarded {
		promptText = msg.Content
	}

	// Enrich prompt with reply quote context if message is a reply
	// Slash commands must remain at the start of the prompt, even when sent
	// as a reply. Prepending quote context would hide them from Hydra's parser.
	if !strings.HasPrefix(strings.TrimSpace(promptText), "/") {
		promptText = q.bridge.formatPromptWithReply(msg, promptText)
	}
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
