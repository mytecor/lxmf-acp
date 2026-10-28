package bridge

import (
	"bytes"
	"unicode"
	"unicode/utf8"
)

const (
	// DefaultMaxPartUnits is the maximum number of UTF-16 code units per
	// LXMF message (Columba's display limit).
	DefaultMaxPartUnits = 10000
	// DefaultMaxPartBytes caps the UTF-8 content size of one LXMF message.
	// 10 000 UTF-16 units of BMP text encode to at most 30 000 bytes, so the
	// default only binds when configured lower; it keeps every part well
	// inside a single Reticulum Resource transfer.
	DefaultMaxPartBytes = 30000

	// softBoundaryDivisor sets the fraction (1/n) of a full part searched
	// backwards for a newline or whitespace to cut after, so that parts read
	// naturally in ordinary clients. Content is never dropped or altered.
	softBoundaryDivisor = 10
	// maxGraphemeBackoff bounds how far a hard cut moves back to avoid
	// separating a base character from combining marks / ZWJ sequences.
	maxGraphemeBackoff = 32
)

// Segmenter incrementally splits streamed text into LXMF-sized parts.
//
// Text is buffered in RAM. A part is cut only once the buffer exceeds the
// limit, i.e. when it is already known that more text follows the cut, so
// every part returned by Write is a non-final part. Whatever remains (at most
// one full part — the "held" fragment) is returned by Flush as the final
// part. A reply that ends exactly on the limit therefore produces a single
// message and never an extra empty one.
type Segmenter struct {
	maxUnits int
	maxBytes int

	buf   []byte
	units int
}

// NewSegmenter creates a segmenter; non-positive limits select the defaults.
func NewSegmenter(maxUnits, maxBytes int) *Segmenter {
	if maxUnits <= 0 {
		maxUnits = DefaultMaxPartUnits
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxPartBytes
	}
	// One code point needs up to 2 units / 4 bytes; guarantee progress.
	if maxUnits < 2 {
		maxUnits = 2
	}
	if maxBytes < utf8.UTFMax {
		maxBytes = utf8.UTFMax
	}
	return &Segmenter{maxUnits: maxUnits, maxBytes: maxBytes}
}

// Write appends streamed text and returns the parts that are complete and
// known to be followed by more text.
func (s *Segmenter) Write(chunk string) []string {
	if chunk == "" {
		return nil
	}
	s.buf = append(s.buf, chunk...)
	s.units += utf16Len(chunk)

	var parts []string
	for s.units > s.maxUnits || len(s.buf) > s.maxBytes {
		cut := s.cutIndex()
		part := string(s.buf[:cut])
		parts = append(parts, part)
		s.buf = append(s.buf[:0], s.buf[cut:]...)
		s.units -= utf16Len(part)
	}
	return parts
}

// Flush returns the remaining buffered text (the final part) and resets.
func (s *Segmenter) Flush() string {
	out := string(s.buf)
	s.buf = nil
	s.units = 0
	return out
}

// Pending reports whether buffered text is waiting to be flushed.
func (s *Segmenter) Pending() bool { return len(s.buf) > 0 }

// cutIndex returns the byte offset of the cut for the current buffer, which
// is known to exceed the limit. The result is always > 0, lies on a code
// point boundary and yields a prefix within both limits.
func (s *Segmenter) cutIndex() int {
	buf := s.buf
	hard := prefixWithin(buf, s.maxUnits, s.maxBytes)

	// Prefer cutting after a newline, then after other whitespace, within
	// the window near the end of the part.
	window := hard / softBoundaryDivisor
	if window < 10 {
		window = 10
	}
	if window > hard/2 {
		window = hard / 2
	}
	lo := hard - window
	if i := bytes.LastIndexByte(buf[:hard], '\n'); i >= lo {
		return i + 1
	}
	if i := bytes.LastIndexFunc(buf[:hard], unicode.IsSpace); i >= lo {
		_, size := utf8.DecodeRune(buf[i:])
		return i + size
	}

	// Hard cut: avoid splitting a grapheme where cheaply detectable.
	cut := hard
	for n := 0; n < maxGraphemeBackoff && cut > 0; n++ {
		next, _ := utf8.DecodeRune(buf[cut:])
		prev, size := utf8.DecodeLastRune(buf[:cut])
		if !isGraphemeExtender(next) && prev != zwj {
			return cut
		}
		cut -= size
	}
	return hard
}

// prefixWithin returns the largest code-point-aligned byte offset whose
// prefix fits both limits.
func prefixWithin(b []byte, maxUnits, maxBytes int) int {
	units := 0
	for i := 0; i < len(b); {
		r, size := utf8.DecodeRune(b[i:])
		u := 1
		if r >= 0x10000 {
			u = 2
		}
		if units+u > maxUnits || i+size > maxBytes {
			return i
		}
		units += u
		i += size
	}
	return len(b)
}

const zwj = '\u200D'

func isGraphemeExtender(r rune) bool {
	switch {
	case r == zwj:
		return true
	case r >= 0xFE00 && r <= 0xFE0F: // variation selectors
		return true
	case r >= 0x1F3FB && r <= 0x1F3FF: // emoji skin tone modifiers
		return true
	case r >= 0xE0020 && r <= 0xE007F: // tag characters (flag sequences)
		return true
	}
	return unicode.In(r, unicode.Mn, unicode.Me, unicode.Mc)
}

// utf16Len returns the number of UTF-16 code units needed to encode s.
func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		if r >= 0x10000 {
			n += 2
		} else {
			n++
		}
	}
	return n
}
