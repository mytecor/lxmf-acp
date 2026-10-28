package lxmf

// Multipart wire format.
//
// A long reply is carried as a chain of ordinary text LXMF messages. Only
// two LXMF fields are used:
//
//   - FIELD_CONTINUATION (0x42): {0x00: <full hash of the previous message>}
//     on every part except the first.
//   - FIELD_CUSTOM_META (0xFD): {"lxmf-acp": {...}} carrying either
//     "more": true (first part of a multipart chain) or
//     "status": "completed" | "failed" | "cancelled" (final part).
//
// The message content is never altered: no numbering, prefixes or markers.

import (
	"fmt"
)

const (
	// FieldRenderer is LXMF FIELD_RENDERER (0x0F).
	FieldRenderer = 0x0F

	// Standard LXMF message renderers for FieldRenderer.
	RendererPlain    = 0x00
	RendererMicron   = 0x01
	RendererMarkdown = 0x02
	RendererBBCode   = 0x03

	// FieldContinuation is LXMF FIELD_CONTINUATION.
	FieldContinuation = 0x42
	// FieldCustomMeta is LXMF FIELD_CUSTOM_META.
	FieldCustomMeta = 0xFD

	// continuationPrevKey is the inner key of FIELD_CONTINUATION holding the
	// previous message hash.
	continuationPrevKey = 0x00

	// MetaNamespace is the namespaced key inside FIELD_CUSTOM_META.
	MetaNamespace = "lxmf-acp"

	metaKeyMore   = "more"
	metaKeyStatus = "status"
)

// Status is the completion status carried by the final part of a reply.
type Status string

const (
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// Valid reports whether s is one of the defined statuses.
func (s Status) Valid() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusCancelled:
		return true
	}
	return false
}

// MultipartInfo is the multipart-related metadata of one LXMF message.
type MultipartInfo struct {
	// Prev is the full hash of the previous message (FIELD_CONTINUATION),
	// nil for a first or standalone message.
	Prev []byte
	// More is true on the first message of a multipart chain.
	More bool
	// Status is set on the final message; empty otherwise.
	Status Status
	// HasMeta reports that the lxmf-acp namespace was present.
	HasMeta bool
}

// IsMultipart reports whether the message carries any multipart signalling.
func (i MultipartInfo) IsMultipart() bool {
	return i.Prev != nil || i.More || i.Status != ""
}

// BuildMultipartFields returns a fields map for one part. existing (may be
// nil) is copied, and any foreign entries inside FIELD_CUSTOM_META are kept:
// only the "lxmf-acp" namespace is written.
func BuildMultipartFields(existing map[any]any, prev []byte, more bool, status Status) map[any]any {
	fields := make(map[any]any, len(existing)+2)
	for k, v := range existing {
		fields[k] = v
	}

	if prev != nil {
		fields[FieldContinuation] = map[any]any{
			continuationPrevKey: append([]byte(nil), prev...),
		}
	}

	ns := map[any]any{}
	if more {
		ns[metaKeyMore] = true
	}
	if status != "" {
		ns[metaKeyStatus] = string(status)
	}
	if len(ns) == 0 {
		return fields
	}

	meta := map[any]any{}
	for k, v := range fields {
		if intKey(k) == FieldCustomMeta {
			if m, ok := v.(map[any]any); ok {
				for mk, mv := range m {
					meta[mk] = mv
				}
			}
			delete(fields, k)
		}
	}
	meta[MetaNamespace] = ns
	fields[FieldCustomMeta] = meta
	return fields
}

// ParseMultipartFields extracts multipart metadata from decoded LXMF fields.
// Unknown or malformed entries are ignored.
func ParseMultipartFields(fields map[any]any) MultipartInfo {
	var info MultipartInfo
	for k, v := range fields {
		switch intKey(k) {
		case FieldContinuation:
			if inner, ok := asMap(v); ok {
				for ik, iv := range inner {
					if intKey(ik) == continuationPrevKey {
						if b, ok := asBytes(iv); ok && len(b) > 0 {
							info.Prev = append([]byte(nil), b...)
						}
					}
				}
			}
		case FieldCustomMeta:
			meta, ok := asMap(v)
			if !ok {
				continue
			}
			for mk, mv := range meta {
				if s, ok := asString(mk); !ok || s != MetaNamespace {
					continue
				}
				ns, ok := asMap(mv)
				if !ok {
					continue
				}
				info.HasMeta = true
				for nk, nv := range ns {
					name, _ := asString(nk)
					switch name {
					case metaKeyMore:
						if b, ok := nv.(bool); ok {
							info.More = b
						}
					case metaKeyStatus:
						if s, ok := asString(nv); ok && Status(s).Valid() {
							info.Status = Status(s)
						}
					}
				}
			}
		}
	}
	return info
}

// ParseRenderer returns the renderer constant specified in FIELD_RENDERER,
// or -1 and false if no valid renderer was found.
func ParseRenderer(fields map[any]any) (int, bool) {
	for k, v := range fields {
		if intKey(k) == FieldRenderer {
			if r := intKey(v); r >= 0 {
				return r, true
			}
		}
	}
	return -1, false
}

// WithRenderer returns a copy of fields with FIELD_RENDERER set to renderer.
func WithRenderer(fields map[any]any, renderer int) map[any]any {
	out := make(map[any]any, len(fields)+1)
	for k, v := range fields {
		if intKey(k) != FieldRenderer {
			out[k] = v
		}
	}
	out[FieldRenderer] = renderer
	return out
}

// intKey normalises msgpack-decoded integer keys (int8, uint8, int64, ...)
// to int. Non-integer keys return -1.
func intKey(k any) int {
	switch v := k.(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		if v > 1<<31 {
			return -1
		}
		return int(v)
	}
	return -1
}

func asMap(v any) (map[any]any, bool) {
	switch m := v.(type) {
	case map[any]any:
		return m, true
	case map[string]any:
		out := make(map[any]any, len(m))
		for k, val := range m {
			out[k] = val
		}
		return out, true
	}
	return nil, false
}

func asBytes(v any) ([]byte, bool) {
	switch b := v.(type) {
	case []byte:
		return b, true
	case string:
		return []byte(b), true
	}
	return nil, false
}

func asString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	return "", false
}

// String implements fmt.Stringer for logging.
func (i MultipartInfo) String() string {
	return fmt.Sprintf("prev=%x more=%v status=%q", i.Prev, i.More, i.Status)
}
