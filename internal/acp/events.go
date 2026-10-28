package acp

import (
	"strings"
	"unicode/utf8"

	"github.com/ironpark/acp-go/acp1"
	schema "github.com/ironpark/acp-go/schema/v1"
)

// Event is a prompt-scoped text delta or a partial tool-call update.
// Tool fields are pointers so omitted fields do not erase earlier values.
type Event struct {
	Text string
	Tool *ToolUpdate
}

type ToolUpdate struct {
	ID                        string
	Title, Name, Kind, Status *string
	Content, Input, Output    *string
}

// MaxToolDetailBytes bounds each retained tool payload. Truncation is explicit.
const MaxToolDetailBytes = 32 << 10

func BoundedToolText(s string) string {
	s = strings.ToValidUTF8(s, "")
	if len(s) <= MaxToolDetailBytes {
		return s
	}
	n := MaxToolDetailBytes - len("\n[truncated]")
	for !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "\n[truncated]"
}

func boundedPointer(s string) *string { v := BoundedToolText(s); return &v }
func stringPointer[T ~string](v *T) *string {
	if v == nil {
		return nil
	}
	s := string(*v)
	return &s
}

func toolContent(content []schema.ToolCallContent) *string {
	if content == nil {
		return nil
	}
	var b strings.Builder
	for _, item := range content {
		var text string
		if v, ok := item.As[schema.ToolCallContentContent](); ok {
			if t, ok := acp1.TextOf(v.Content); ok {
				text = t
			} else {
				text = "[non-text tool content]"
			}
		} else if v, ok := item.As[schema.ToolCallContentDiff](); ok {
			text = "File: " + v.Path + "\nBefore:\n" + v.GetOldText() + "\nAfter:\n" + v.NewText
		} else if v, ok := item.As[schema.ToolCallContentTerminal](); ok {
			text = "Terminal: " + string(v.TerminalID) + " (output not included by agent)"
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(BoundedToolText(text))
		if b.Len() > MaxToolDetailBytes {
			break
		}
	}
	return boundedPointer(b.String())
}

func eventFromUpdate(update schema.SessionUpdate) (Event, bool) {
	if v, ok := update.As[schema.SessionUpdateAgentMessageChunk](); ok {
		text, ok := acp1.TextOf(v.Content)
		return Event{Text: text}, ok && text != ""
	}
	var t ToolUpdate
	if v, ok := update.As[schema.SessionUpdateToolCall](); ok {
		t = ToolUpdate{ID: string(v.ToolCallID), Title: boundedPointer(v.Title), Name: stringPointer(v.Name), Kind: stringPointer(v.Kind), Status: stringPointer(v.Status), Content: toolContent(v.Content)}
		if v.RawInput != nil {
			t.Input = boundedPointer(string(v.RawInput))
		}
		if v.RawOutput != nil {
			t.Output = boundedPointer(string(v.RawOutput))
		}
	} else if v, ok := update.As[schema.SessionUpdateToolCallUpdate](); ok {
		t = ToolUpdate{ID: string(v.ToolCallID), Name: stringPointer(v.Name), Kind: stringPointer(v.Kind), Status: stringPointer(v.Status), Content: toolContent(v.Content)}
		if v.Title != nil {
			t.Title = boundedPointer(*v.Title)
		}
		if v.RawInput != nil {
			t.Input = boundedPointer(string(v.RawInput))
		}
		if v.RawOutput != nil {
			t.Output = boundedPointer(string(v.RawOutput))
		}
	} else {
		return Event{}, false
	}
	return Event{Tool: &t}, t.ID != ""
}
