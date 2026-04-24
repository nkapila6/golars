package series

import (
	"strings"
)

// SQL-style LIKE matcher.
//
// Pattern syntax (matches PostgreSQL / polars str.contains(literal=False)
// with SQL-LIKE mode):
//
//   %    zero or more of any character
//   _    exactly one character
//   \x   literal x (escape for % and _; otherwise \ is literal too)
//
// Design goal: zero regex overhead for the common TPC-H shapes
// (`%needle%`, `prefix%`, `%suffix`, `prefix%needle%suffix`, …). We
// compile the pattern once into a small fixed-sized struct, then the
// hot match loop is a chain of strings.Index / strings.HasPrefix
// calls - each a SIMD-accelerated stdlib op.
//
// The general pattern (with `_` wildcards sprinkled in) falls back to
// a byte-level scan that understands single-char wildcards; still no
// regex engine.

type likePattern struct {
	// anchorStart: true when the pattern does not start with %. Forces
	// the first literal fragment to match at offset 0.
	anchorStart bool
	// anchorEnd: true when the pattern does not end with %. Forces the
	// last literal fragment to match at offset len(s)-len(tail).
	anchorEnd bool
	// parts are the literal fragments between %s, in order. Each is a
	// sequence of (byte, isWildcard) pairs where byte is the literal
	// to match when isWildcard=false, and ignored when true (matches
	// any single byte).
	parts []likePart
	// literalOnly = true when no `_` appears anywhere. Enables the
	// strings.Index fast path for every part.
	literalOnly bool
}

type likePart struct {
	// raw is the literal byte sequence, with placeholders. When
	// hasUnderscore is false, raw IS the match string (use
	// strings.Index / HasPrefix / HasSuffix directly). Otherwise the
	// matcher walks raw + mask byte-by-byte.
	raw  []byte
	mask []byte // 1 means the corresponding raw byte is a `_` wildcard
	// minLen = len(raw). Pre-stored for the bounds check.
	minLen int
}

// compileLike parses a LIKE pattern into a likePattern. The returned
// value is immutable - safe to share across goroutines.
func compileLike(pattern string) likePattern {
	anchorStart, anchorEnd := true, true
	if strings.HasPrefix(pattern, "%") {
		anchorStart = false
		pattern = pattern[1:]
	}
	if strings.HasSuffix(pattern, "%") {
		// %% is tricky: "a%%" should treat one % as escaped. But SQL
		// doesn't have %% as escape - polars doesn't either. We keep
		// it simple: an unescaped trailing % unanchors.
		if !endsWithEscapedPct(pattern) {
			anchorEnd = false
			pattern = pattern[:len(pattern)-1]
		}
	}

	segs := splitLikeSegments(pattern)
	lp := likePattern{anchorStart: anchorStart, anchorEnd: anchorEnd, parts: make([]likePart, 0, len(segs))}
	literalOnly := true
	for _, seg := range segs {
		part := compileLikePart(seg)
		if len(part.mask) > 0 {
			literalOnly = false
		}
		lp.parts = append(lp.parts, part)
	}
	lp.literalOnly = literalOnly
	return lp
}

// endsWithEscapedPct reports whether s ends with a `\%` (backslash-
// escaped percent), where the backslash itself is not escaped by a
// preceding backslash.
func endsWithEscapedPct(s string) bool {
	if !strings.HasSuffix(s, "%") {
		return false
	}
	// Count trailing backslashes before the %.
	i := len(s) - 2
	slashes := 0
	for i >= 0 && s[i] == '\\' {
		slashes++
		i--
	}
	return slashes%2 == 1
}

// splitLikeSegments splits a pattern on unescaped `%` markers.
func splitLikeSegments(pattern string) []string {
	segs := make([]string, 0, 2)
	var cur strings.Builder
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if c == '\\' && i+1 < len(pattern) {
			cur.WriteByte(pattern[i+1])
			i++
			continue
		}
		if c == '%' {
			segs = append(segs, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}
	if cur.Len() > 0 {
		segs = append(segs, cur.String())
	}
	return segs
}

// compileLikePart turns a literal segment (with `_` still possibly
// present) into a likePart. `_` markers become byte 0 with mask 1.
func compileLikePart(seg string) likePart {
	raw := make([]byte, 0, len(seg))
	var mask []byte
	hasWild := false
	for i := 0; i < len(seg); i++ {
		c := seg[i]
		if c == '_' {
			if !hasWild {
				// Upgrade to masked path; initialise the mask now.
				mask = make([]byte, len(raw), len(seg))
				hasWild = true
			}
			raw = append(raw, 0)
			mask = append(mask, 1)
			continue
		}
		raw = append(raw, c)
		if hasWild {
			mask = append(mask, 0)
		}
	}
	return likePart{raw: raw, mask: mask, minLen: len(raw)}
}

// match runs the pattern against s and returns true iff it matches.
//
// Algorithm: walk parts[] in order. parts[0] must match at offset 0
// when anchorStart; parts[-1] must match at the end when anchorEnd.
// Interior parts are found by a forward search (strings.Index for the
// literal-only fast path; matchPartForward for the masked path).
func (lp likePattern) match(s string) bool {
	if len(lp.parts) == 0 {
		// Pure %%%: unanchored empty - any string matches. A single
		// anchored empty pattern ("") only matches "".
		if lp.anchorStart && lp.anchorEnd {
			return s == ""
		}
		return true
	}
	var cursor int
	for i, part := range lp.parts {
		anchoredStart := lp.anchorStart && i == 0
		anchoredEnd := lp.anchorEnd && i == len(lp.parts)-1
		if lp.literalOnly {
			idx := findLiteral(s, cursor, part.raw, anchoredStart, anchoredEnd)
			if idx < 0 {
				return false
			}
			cursor = idx + part.minLen
			continue
		}
		idx := findMasked(s, cursor, part, anchoredStart, anchoredEnd)
		if idx < 0 {
			return false
		}
		cursor = idx + part.minLen
	}
	return true
}

// findLiteral locates part in s[cursor:], obeying anchor flags.
// Returns the absolute offset in s, or -1 when not found.
func findLiteral(s string, cursor int, part []byte, anchoredStart, anchoredEnd bool) int {
	need := len(part)
	switch {
	case anchoredStart && anchoredEnd:
		if len(s)-cursor != need {
			return -1
		}
		if bytesEqualStr(s[cursor:], part) {
			return cursor
		}
		return -1
	case anchoredStart:
		if len(s)-cursor < need {
			return -1
		}
		if bytesEqualStr(s[cursor:cursor+need], part) {
			return cursor
		}
		return -1
	case anchoredEnd:
		end := len(s) - need
		if end < cursor {
			return -1
		}
		if bytesEqualStr(s[end:], part) {
			return end
		}
		return -1
	default:
		if need == 0 {
			return cursor
		}
		idx := strings.Index(s[cursor:], string(part))
		if idx < 0 {
			return -1
		}
		return cursor + idx
	}
}

// findMasked is the masked-part equivalent of findLiteral. Slower
// because it can't use strings.Index directly.
func findMasked(s string, cursor int, part likePart, anchoredStart, anchoredEnd bool) int {
	need := part.minLen
	switch {
	case anchoredStart && anchoredEnd:
		if len(s)-cursor != need {
			return -1
		}
		if maskedEqual(s[cursor:], part.raw, part.mask) {
			return cursor
		}
		return -1
	case anchoredStart:
		if len(s)-cursor < need {
			return -1
		}
		if maskedEqual(s[cursor:cursor+need], part.raw, part.mask) {
			return cursor
		}
		return -1
	case anchoredEnd:
		end := len(s) - need
		if end < cursor {
			return -1
		}
		if maskedEqual(s[end:], part.raw, part.mask) {
			return end
		}
		return -1
	default:
		if need == 0 {
			return cursor
		}
		limit := len(s) - need
		for i := cursor; i <= limit; i++ {
			if maskedEqual(s[i:i+need], part.raw, part.mask) {
				return i
			}
		}
		return -1
	}
}

// bytesEqualStr compares a string against a []byte literal. Equivalent
// to s == string(b) but avoids the allocation.
func bytesEqualStr(s string, b []byte) bool {
	if len(s) != len(b) {
		return false
	}
	for i := range b {
		if s[i] != b[i] {
			return false
		}
	}
	return true
}

// maskedEqual compares s[:len(raw)] against raw, treating positions
// with mask[i]==1 as wildcards that match anything.
func maskedEqual(s string, raw, mask []byte) bool {
	if len(s) != len(raw) {
		return false
	}
	for i := range raw {
		if mask[i] != 0 {
			continue
		}
		if s[i] != raw[i] {
			return false
		}
	}
	return true
}

// Like returns a boolean Series that is true where the SQL-style
// pattern matches the row. % matches zero or more characters, _ matches
// exactly one; \ escapes the next character.
//
// The compiler classifies the pattern shape and routes literal-only
// forms to specialised predicates: bare `%X%` to strings.Contains,
// `X%` to HasPrefix, `%X` to HasSuffix, `X` to equality. These close
// over the raw []byte (or string) without going through the full
// likePattern.match machinery, eliminating the closure-dispatch
// overhead the general path pays. Null inputs stay null.
func (o StrOps) Like(pattern string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("Like")
	if err != nil {
		return nil, err
	}
	pred := compileLikePredicate(pattern)
	return boolResultFromStr(o.s.Name(), a, cfg.alloc, pred)
}

// NotLike is the inverse of Like. Same specialisation ladder, with the
// terminal negation folded into each specialised closure so the hot
// loop still makes a single predicate call.
func (o StrOps) NotLike(pattern string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("NotLike")
	if err != nil {
		return nil, err
	}
	pred := compileLikePredicate(pattern)
	return boolResultFromStr(o.s.Name(), a, cfg.alloc, func(s string) bool {
		return !pred(s)
	})
}

// compileLikePredicate returns the tightest matcher possible for
// pattern. Shape analysis:
//   - empty literal-only part list + both anchors -> match empty only
//   - empty part list + either anchor false       -> match anything
//   - exactly one part, literal-only:
//       anchored both ends -> string equality
//       anchored start     -> HasPrefix
//       anchored end       -> HasSuffix
//       unanchored         -> strings.Contains (or IndexByte for 1)
//   - otherwise fall through to the general likePattern.match
func compileLikePredicate(pattern string) func(string) bool {
	lp := compileLike(pattern)
	if len(lp.parts) == 0 {
		if lp.anchorStart && lp.anchorEnd {
			return func(s string) bool { return s == "" }
		}
		return func(string) bool { return true }
	}
	if len(lp.parts) == 1 && lp.literalOnly {
		lit := string(lp.parts[0].raw)
		switch {
		case lp.anchorStart && lp.anchorEnd:
			return func(s string) bool { return s == lit }
		case lp.anchorStart:
			return func(s string) bool { return strings.HasPrefix(s, lit) }
		case lp.anchorEnd:
			return func(s string) bool { return strings.HasSuffix(s, lit) }
		default:
			if len(lit) == 0 {
				return func(string) bool { return true }
			}
			if len(lit) == 1 {
				b := lit[0]
				return func(s string) bool { return strings.IndexByte(s, b) >= 0 }
			}
			return func(s string) bool { return strings.Contains(s, lit) }
		}
	}
	return lp.match
}
