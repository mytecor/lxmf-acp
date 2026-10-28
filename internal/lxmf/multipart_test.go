package lxmf

import (
	"bytes"
	"testing"

	reticulumLxmf "github.com/thatSFguy/reticulum-go/lxmf"
	"github.com/thatSFguy/reticulum-go/rns"
)

// roundTrip packs fields into a real signed LXMF direct body and parses it
// back, so the test exercises the actual msgpack key/value types.
func roundTrip(t *testing.T, content string, fields map[any]any) (*reticulumLxmf.Message, []byte) {
	t.Helper()
	sender, err := rns.NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := rns.NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	src := sender.DestinationHashFor(reticulumLxmf.FullName())
	dst := recipient.DestinationHashFor(reticulumLxmf.FullName())

	body, id, err := reticulumLxmf.SignAndPackDirect(sender, src, dst, []byte("Reply"), []byte(content), fields)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	msg, err := reticulumLxmf.ParseDirectBody(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := msg.Verify(sender.PublicKey()[32:]); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !bytes.Equal(msg.MessageID(), id) {
		t.Fatalf("message id mismatch")
	}
	return msg, id
}

func TestMultipartFieldsRoundTrip(t *testing.T) {
	prev := bytes.Repeat([]byte{0xAB}, 32)

	cases := []struct {
		name   string
		prev   []byte
		more   bool
		status Status
	}{
		{"first", nil, true, ""},
		{"middle", prev, false, ""},
		{"last", prev, false, StatusCompleted},
		{"single", nil, false, StatusCompleted},
		{"failed", prev, false, StatusFailed},
		{"cancelled", nil, false, StatusCancelled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fields := BuildMultipartFields(nil, tc.prev, tc.more, tc.status)
			msg, _ := roundTrip(t, "text stays untouched", fields)
			if string(msg.Content) != "text stays untouched" {
				t.Fatalf("content altered: %q", msg.Content)
			}
			info := ParseMultipartFields(msg.Fields)
			if !bytes.Equal(info.Prev, tc.prev) || info.More != tc.more || info.Status != tc.status {
				t.Fatalf("got %v, want prev=%x more=%v status=%q", info, tc.prev, tc.more, tc.status)
			}
			if !info.IsMultipart() {
				t.Fatalf("expected multipart signalling")
			}
		})
	}
}

func TestMultipartFieldsMinimalShape(t *testing.T) {
	// Middle part: only FIELD_CONTINUATION, no custom meta.
	f := BuildMultipartFields(nil, []byte{1, 2, 3}, false, "")
	if len(f) != 1 {
		t.Fatalf("middle part should carry exactly one field, got %v", f)
	}
	if _, ok := f[FieldContinuation]; !ok {
		t.Fatalf("missing FIELD_CONTINUATION")
	}
	// Single-part reply: only status, no continuation, no more.
	f = BuildMultipartFields(nil, nil, false, StatusCompleted)
	ns := f[FieldCustomMeta].(map[any]any)[MetaNamespace].(map[any]any)
	if len(f) != 1 || len(ns) != 1 || ns["status"] != "completed" {
		t.Fatalf("unexpected single-part fields: %v", f)
	}
}

func TestMultipartFieldsPreserveForeignMeta(t *testing.T) {
	existing := map[any]any{
		FieldCustomMeta: map[any]any{"other-app": "keep-me"},
		0x30:            []byte("reply-to"),
	}
	f := BuildMultipartFields(existing, nil, true, "")
	msg, _ := roundTrip(t, "x", f)

	meta, ok := asMap(func() any {
		for k, v := range msg.Fields {
			if intKey(k) == FieldCustomMeta {
				return v
			}
		}
		return nil
	}())
	if !ok {
		t.Fatalf("custom meta missing")
	}
	found := false
	for k, v := range meta {
		if s, _ := asString(k); s == "other-app" {
			if vs, _ := asString(v); vs == "keep-me" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("foreign FIELD_CUSTOM_META entry was overwritten: %v", meta)
	}
	if !ParseMultipartFields(msg.Fields).More {
		t.Fatalf("more flag lost")
	}
	// The caller's map must not be mutated.
	if _, ok := existing[FieldCustomMeta].(map[any]any)[MetaNamespace]; ok {
		t.Fatalf("existing fields map was mutated")
	}
}

func TestParsePlainMessageHasNoMultipart(t *testing.T) {
	msg, _ := roundTrip(t, "hello", nil)
	if ParseMultipartFields(msg.Fields).IsMultipart() {
		t.Fatalf("plain message detected as multipart")
	}
	// Unknown status values and foreign namespaces are ignored.
	info := ParseMultipartFields(map[any]any{
		FieldCustomMeta: map[any]any{
			"someone-else": map[any]any{"status": "completed"},
			MetaNamespace:  map[any]any{"status": "weird"},
		},
	})
	if info.IsMultipart() {
		t.Fatalf("unexpected multipart detection: %v", info)
	}
}

func TestRendererParsingAndModifying(t *testing.T) {
	// 1. Missing field
	if r, ok := ParseRenderer(nil); ok || r != -1 {
		t.Fatalf("expected (-1, false), got (%d, %v)", r, ok)
	}
	if r, ok := ParseRenderer(map[any]any{"foo": "bar"}); ok || r != -1 {
		t.Fatalf("expected (-1, false), got (%d, %v)", r, ok)
	}

	// 2. Integer variant key and values
	cases := []struct {
		name     string
		fields   map[any]any
		expected int
	}{
		{"int key & val", map[any]any{int(FieldRenderer): int(RendererMarkdown)}, RendererMarkdown},
		{"int8 key & val", map[any]any{int8(FieldRenderer): int8(RendererMarkdown)}, RendererMarkdown},
		{"uint8 key & val", map[any]any{uint8(FieldRenderer): uint8(RendererMarkdown)}, RendererMarkdown},
		{"int64 key & val", map[any]any{int64(FieldRenderer): int64(RendererPlain)}, RendererPlain},
		{"micron renderer", map[any]any{FieldRenderer: RendererMicron}, RendererMicron},
		{"bbcode renderer", map[any]any{FieldRenderer: RendererBBCode}, RendererBBCode},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, ok := ParseRenderer(tc.fields)
			if !ok || r != tc.expected {
				t.Fatalf("ParseRenderer(%v) = (%d, %v), want (%d, true)", tc.fields, r, ok, tc.expected)
			}
		})
	}

	// 3. WithRenderer
	orig := map[any]any{
		"unrelated": "preserved",
		FieldRenderer: RendererPlain,
	}
	updated := WithRenderer(orig, RendererMarkdown)
	if r, ok := ParseRenderer(updated); !ok || r != RendererMarkdown {
		t.Fatalf("updated renderer = (%d, %v), want (%d, true)", r, ok, RendererMarkdown)
	}
	if updated["unrelated"] != "preserved" {
		t.Fatalf("lost unrelated field: %v", updated)
	}
	// Original must not be mutated
	if orig[FieldRenderer] != RendererPlain {
		t.Fatalf("original mutated: %v", orig)
	}

	// 4. InboundMessage helper
	inboundMd := &InboundMessage{Renderer: RendererMarkdown}
	if !inboundMd.IsMarkdown() {
		t.Fatalf("expected IsMarkdown() == true")
	}
	inboundPlain := &InboundMessage{Renderer: RendererPlain}
	if inboundPlain.IsMarkdown() {
		t.Fatalf("expected IsMarkdown() == false for plain")
	}
}

func TestRendererRoundTrip(t *testing.T) {
	fields := WithRenderer(nil, RendererMarkdown)
	msg, _ := roundTrip(t, "# Hello Markdown\n\n- item 1\n- item 2", fields)

	r, ok := ParseRenderer(msg.Fields)
	if !ok || r != RendererMarkdown {
		t.Fatalf("ParseRenderer after roundTrip = (%d, %v), want (%d, true)", r, ok, RendererMarkdown)
	}
}

func TestReplyFields(t *testing.T) {
	replyID := bytes.Repeat([]byte{0x42}, 32)
	quote := "Original message snippet"

	// 1. WithReplyTo and ParseReplyFields
	fields := WithReplyTo(nil, replyID, quote)
	gotID, gotQuote := ParseReplyFields(fields)
	if !bytes.Equal(gotID, replyID) {
		t.Fatalf("gotID = %x, want %x", gotID, replyID)
	}
	if gotQuote != quote {
		t.Fatalf("gotQuote = %q, want %q", gotQuote, quote)
	}

	// 2. Hex string decoding in ParseReplyFields
	hexID := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	rawFields := map[any]any{
		FieldReplyTo:    hexID,
		FieldReplyQuote: []byte("byte quote"),
	}
	parsedID, parsedQuote := ParseReplyFields(rawFields)
	if len(parsedID) != 32 || parsedID[0] != 0x01 {
		t.Fatalf("expected 32-byte decoded hex, got %x", parsedID)
	}
	if parsedQuote != "byte quote" {
		t.Fatalf("expected string quote, got %q", parsedQuote)
	}

	// 3. Real wire round-trip
	msg, _ := roundTrip(t, "This is a reply", fields)
	rtID, rtQuote := ParseReplyFields(msg.Fields)
	if !bytes.Equal(rtID, replyID) {
		t.Fatalf("roundTrip reply ID = %x, want %x", rtID, replyID)
	}
	if rtQuote != quote {
		t.Fatalf("roundTrip quote = %q, want %q", rtQuote, quote)
	}

	// 4. InboundMessage helper
	msgWithReply := &InboundMessage{ReplyTo: replyID}
	if !msgWithReply.IsReply() {
		t.Fatalf("expected IsReply() == true")
	}
	msgWithQuote := &InboundMessage{ReplyQuote: quote}
	if !msgWithQuote.IsReply() {
		t.Fatalf("expected IsReply() == true with quote")
	}
	msgNormal := &InboundMessage{}
	if msgNormal.IsReply() {
		t.Fatalf("expected IsReply() == false for normal message")
	}
}

func TestMakeQuoteSnippet(t *testing.T) {
	cases := []struct {
		input  string
		maxLen int
		want   string
	}{
		{"", 50, ""},
		{"   ", 50, ""},
		{"видишь реплай?", 50, "видишь реплай?"},
		{"Line 1\nLine 2\n\nLine 3", 50, "Line 1 Line 2 Line 3"},
		{"This is a very long text that exceeds the limit specified for testing", 20, "This is a very lo..."},
	}
	for _, tc := range cases {
		got := MakeQuoteSnippet(tc.input, tc.maxLen)
		if got != tc.want {
			t.Errorf("MakeQuoteSnippet(%q, %d) = %q, want %q", tc.input, tc.maxLen, got, tc.want)
		}
	}
}
