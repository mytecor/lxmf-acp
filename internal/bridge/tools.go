package bridge

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mytecor/lxmf-acp/internal/acp"
	"github.com/mytecor/lxmf-acp/internal/lxmf"
	"github.com/mytecor/lxmf-acp/internal/storage"
)

const toolBatchInterval = 3 * time.Second
const maxTurnTools = 256

type trackedTool struct {
	record  storage.ToolRecord
	started time.Time
	ended   bool
	dirty   bool
}

// toolReporter serializes timer flushes and ACP events, keeping ordering with text.
// The ACP callback only enqueues delivery; it never waits for a mesh receipt.
type toolReporter struct {
	mu                    sync.Mutex
	stream                *replyStream
	store                 *storage.Store
	chatKey, mode, turnID string
	tools                 map[string]*trackedTool
	order                 []string
	sequence              int
	overflow              bool
	closed                bool
	stop                  chan struct{}
	done                  chan struct{}
	now                   func() time.Time
}

func newToolReporter(stream *replyStream, store *storage.Store, interval time.Duration) *toolReporter {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	r := &toolReporter{stream: stream, store: store, chatKey: stream.chatKey,
		mode: store.ToolMode(stream.chatKey), turnID: hex.EncodeToString(id[:]),
		tools: make(map[string]*trackedTool), stop: make(chan struct{}), done: make(chan struct{}), now: time.Now}
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-ticker.C:
				r.mu.Lock()
				if !r.closed {
					r.flushLocked()
				}
				r.mu.Unlock()
			}
		}
	}()
	return r
}

func (r *toolReporter) Event(event acp.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if event.Tool != nil {
		r.updateLocked(*event.Tool)
	}
	if event.Text != "" {
		r.flushLocked()
		r.stream.Write(event.Text)
	}
}

func (r *toolReporter) updateLocked(update acp.ToolUpdate) {
	if update.ID == "" {
		return
	}
	tool := r.tools[update.ID]
	if tool == nil {
		if len(r.tools) >= maxTurnTools {
			r.overflow = true
			return
		}
		record, err := r.store.AddTool(r.chatKey, storage.ToolRecord{ID: update.ID, TurnID: r.turnID, Status: "pending"})
		if err != nil {
			log.Printf("[Tools] Cannot reserve tool number: %v", err)
			return
		}
		tool = &trackedTool{record: record, started: r.now(), dirty: true}
		r.tools[update.ID] = tool
		r.order = append(r.order, update.ID)
	}
	previous := tool.record
	assign := func(dst *string, src *string) {
		if src != nil {
			*dst = acp.BoundedToolText(*src)
		}
	}
	assign(&tool.record.Title, update.Title)
	assign(&tool.record.Name, update.Name)
	assign(&tool.record.Kind, update.Kind)
	assign(&tool.record.Content, update.Content)
	assign(&tool.record.Input, update.Input)
	assign(&tool.record.Output, update.Output)
	// A late duplicate pending notification must not reopen a completed call.
	if !tool.ended || (update.Status != nil && terminalToolStatus(*update.Status)) {
		assign(&tool.record.Status, update.Status)
	}
	if terminalToolStatus(tool.record.Status) && !tool.ended {
		tool.ended = true
		tool.record.DurationMS = max(0, r.now().Sub(tool.started).Milliseconds())
	}
	tool.dirty = tool.dirty || previous != tool.record
}

func terminalToolStatus(status string) bool {
	return status == "completed" || status == "failed" || status == "cancelled" || status == "interrupted"
}

// Finish records unfinished calls honestly; end_turn does not imply tool success.
func (r *toolReporter) Finish(cancelled bool) {
	r.mu.Lock()
	if !r.closed {
		for _, tool := range r.tools {
			if !tool.ended {
				tool.record.Status = "interrupted"
				if cancelled {
					tool.record.Status = "cancelled"
				}
				tool.record.DurationMS = max(0, r.now().Sub(tool.started).Milliseconds())
				tool.ended = true
				tool.dirty = true
			}
		}
		r.flushLocked()
		r.closed = true
		close(r.stop)
	}
	r.mu.Unlock()
	<-r.done
}

func (r *toolReporter) flushLocked() {
	var records []storage.ToolRecord
	for _, id := range r.order {
		tool := r.tools[id]
		if tool.dirty {
			records = append(records, tool.record)
		}
	}
	if len(records) == 0 && !r.overflow {
		return
	}
	if len(records) > 0 {
		if err := r.store.UpdateTools(r.chatKey, records); err != nil {
			log.Printf("[Tools] Cannot persist tool details: %v", err)
			return // Keep dirty records for a later flush.
		}
	}
	if r.mode != "off" {
		// Small batches keep metadata and content bounded even on slow links.
		for start := 0; start < len(records); start += 8 {
			end := min(start+8, len(records))
			batch := records[start:end]
			var lines []string
			var calls []any
			for _, record := range batch {
				line := toolLine(record)
				if r.mode == "verbose" {
					if record.Input != "" {
						line += "\nInput: " + shortToolText(record.Input, 500)
					}
					if result := toolResult(record); result != "" {
						line += "\n" + shortToolText(result, 1500)
					}
				} else if terminalToolStatus(record.Status) {
					if result := toolResult(record); result != "" {
						line += " — " + shortToolText(result, 160)
					}
				}
				lines = append(lines, line)
				calls = append(calls, map[any]any{"number": record.Number, "tool_call_id": record.ID, "title": shortToolText(record.Title, 240), "status": record.Status, "kind": record.Kind, "name": shortToolText(record.Name, 240), "duration_ms": record.DurationMS})
			}
			r.sequence++
			fields := map[any]any{lxmf.FieldCustomMeta: map[any]any{lxmf.MetaNamespace: map[any]any{
				"version": 1, "type": "tool_calls", "turn_id": r.turnID, "sequence": r.sequence, "calls": calls,
			}}}
			r.stream.Notice("Tools", "Tools:\n"+strings.Join(lines, "\n")+"\nDetails: /bridge tool <number>", fields)
		}
		if r.overflow {
			r.stream.Notice("Tools", "Additional tool calls omitted (turn limit: 256).", nil)
		}
	}
	for _, id := range r.order {
		r.tools[id].dirty = false
	}
	r.overflow = false
}

func toolResult(record storage.ToolRecord) string {
	if record.Content != "" {
		return record.Content
	}
	return record.Output
}

func shortToolText(text string, limit int) string {
	text = strings.Join(strings.Fields(text), " ")
	runes := []rune(text)
	if len(runes) > limit {
		return string(runes[:limit]) + "…"
	}
	return text
}

func toolLine(record storage.ToolRecord) string {
	icon := "⏳"
	switch record.Status {
	case "completed":
		icon = "✓"
	case "failed":
		icon = "✗"
	case "cancelled":
		icon = "■"
	case "interrupted":
		icon = "⚠"
	}
	title := record.Title
	if title == "" {
		title = record.Name
	}
	if title == "" {
		title = "Tool call"
	}
	line := fmt.Sprintf("%s #%d %s [%s]", icon, record.Number, shortToolText(title, 240), record.Status)
	if terminalToolStatus(record.Status) {
		line += fmt.Sprintf(" (%.1fs)", float64(record.DurationMS)/1000)
	}
	return line
}

func toolDetails(record storage.ToolRecord) string {
	text := toolLine(record)
	if record.Name != "" {
		text += "\nTool: " + record.Name
	}
	if record.Kind != "" {
		text += "\nKind: " + record.Kind
	}
	for _, part := range []struct{ label, value string }{{"Input", record.Input}, {"Content", record.Content}, {"Output", record.Output}} {
		if part.value != "" {
			text += "\n\n" + part.label + ":\n" + part.value
		}
	}
	return text
}

// Tool commands run locally and never become agent prompts.
func (q *peerQueue) toolCommand(text string) bool {
	parts := strings.Fields(text)
	if len(parts) == 0 || (parts[0] != "/tools" && parts[0] != "/tool") {
		return false
	}
	key := q.localHex + ":" + q.peerHex
	var reply string
	if parts[0] == "/tools" {
		switch {
		case len(parts) == 1:
			reply = "Tool display: " + q.bridge.store.ToolMode(key) + ". Usage: /bridge tools off|compact|verbose"
		case len(parts) != 2 || !storage.ValidToolMode(parts[1]):
			reply = "Usage: /bridge tools off|compact|verbose"
		default:
			if err := q.bridge.store.SetToolMode(key, parts[1]); err != nil {
				reply = "Could not save tool display preference."
			} else {
				reply = "Tool display: " + parts[1]
			}
		}
	} else {
		number := 0
		if len(parts) == 2 {
			number, _ = strconv.Atoi(parts[1])
		}
		if number <= 0 {
			reply = "Usage: /bridge tool <number>"
		} else if record, ok := q.bridge.store.GetTool(key, number); ok {
			reply = toolDetails(record)
		} else {
			reply = "Tool not found. Details are retained for the last 50 calls in this chat."
		}
	}
	stream := q.bridge.newReplyStream(q.localHash, q.peerHash, "", nil, "")
	stream.Notice("Tools", reply, nil)
	stream.Finish(lxmf.StatusCompleted, "", "")
	if err := stream.Wait(); err != nil {
		log.Printf("[Tools] Command reply delivery failed: %v", err)
	}
	return true
}
