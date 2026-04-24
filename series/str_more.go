package series

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Before returns the substring preceding the first occurrence of sep
// in each string. If sep is not found, the whole original string is
// returned. Useful for parsing: s.Str().Before("@") on an email
// column gives the local part.
func (o StrOps) Before(sep string, opts ...Option) (*Series, error) {
	return o.mapString("Before", func(s string) string {
		if before, _, found := strings.Cut(s, sep); found {
			return before
		}
		return s
	}, opts)
}

// After returns the substring after the first occurrence of sep.
// Strings that don't contain sep come back as the empty string.
func (o StrOps) After(sep string, opts ...Option) (*Series, error) {
	return o.mapString("After", func(s string) string {
		if _, after, found := strings.Cut(s, sep); found {
			return after
		}
		return ""
	}, opts)
}

// SplitNth returns the nth field (0-indexed) produced by splitting
// each string on sep. If n is out of range the empty string is
// emitted. This stops short of the full polars str.split (which
// returns a List column); SplitNth is the common case.
func (o StrOps) SplitNth(sep string, n int, opts ...Option) (*Series, error) {
	if n < 0 {
		return nil, fmt.Errorf("series.Str.SplitNth: n must be >= 0, got %d", n)
	}
	return o.mapString("SplitNth", func(s string) string {
		parts := strings.Split(s, sep)
		if n >= len(parts) {
			return ""
		}
		return parts[n]
	}, opts)
}

// SplitWide returns one string Series per split field up to maxN. The
// result is a DataFrame-friendly per-field projection: for
// s.Str().SplitWide("-", 3) on "a-b-c-d", output has 3 columns named
// `<series-name>_0`, `_1`, `_2` containing ["a"], ["b"], ["c-d"]. If
// the caller asks for more fields than any row has, extra columns are
// null-padded.
func (o StrOps) SplitWide(sep string, maxN int, opts ...Option) ([]*Series, error) {
	if maxN <= 0 {
		return nil, fmt.Errorf("series.Str.SplitWide: maxN must be positive, got %d", maxN)
	}
	cfg := resolve(opts)
	a, err := o.stringArr("SplitWide")
	if err != nil {
		return nil, err
	}
	n := a.Len()
	out := make([][]string, maxN)
	validCols := make([][]bool, maxN)
	for i := range maxN {
		out[i] = make([]string, n)
		validCols[i] = make([]bool, n)
	}
	for i := range n {
		if !a.IsValid(i) {
			continue
		}
		parts := strings.SplitN(a.Value(i), sep, maxN)
		for j, p := range parts {
			out[j][i] = p
			validCols[j][i] = true
		}
	}
	result := make([]*Series, maxN)
	for j := range maxN {
		name := fmt.Sprintf("%s_%d", o.s.Name(), j)
		s, err := FromString(name, out[j], validColsToSlice(validCols[j]), WithAllocator(cfg.alloc))
		if err != nil {
			for _, prev := range result[:j] {
				if prev != nil {
					prev.Release()
				}
			}
			return nil, err
		}
		result[j] = s
	}
	return result, nil
}

// validColsToSlice returns valid if any entry is false, else nil.
func validColsToSlice(v []bool) []bool {
	for _, ok := range v {
		if !ok {
			return v
		}
	}
	return nil
}

// IsNumeric returns a boolean mask that is true when the string parses
// as a decimal number (integer or float). Convenience for cleaning
// scraped data.
func (o StrOps) IsNumeric(opts ...Option) (*Series, error) {
	return o.mapBool("IsNumeric", func(s string) bool {
		if s == "" {
			return false
		}
		seenDot, seenDigit := false, false
		start := 0
		if s[0] == '+' || s[0] == '-' {
			start = 1
		}
		if start == len(s) {
			return false
		}
		for _, c := range s[start:] {
			switch {
			case c >= '0' && c <= '9':
				seenDigit = true
			case c == '.' && !seenDot:
				seenDot = true
			default:
				return false
			}
		}
		return seenDigit
	}, opts)
}

// Keep compute imports non-redundant: referenced via array.
var _ = array.NewStringData
