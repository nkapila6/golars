// Package selector builds column-set predicates for DataFrame
// operations. Users combine selectors and pass them to
// dataframe.SelectBy / dataframe.DropBy (and the equivalent lazy
// methods) instead of spelling every column by name.
//
// Mirrors polars' selectors module. The API is deliberately small:
// one Selector interface (Apply(schema) []string) and a handful of
// constructors plus boolean combinators.
package selector

import (
	"regexp"
	"slices"
	"strings"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/schema"
)

// Selector picks zero or more columns from a schema. Implementations
// must be pure (no side-effects) and deterministic: given the same
// schema the same set of names is returned in a consistent order.
type Selector interface {
	Apply(s *schema.Schema) []string
}

// SelectorFunc adapts a plain func into a Selector. Useful for ad-hoc
// one-off predicates.
type SelectorFunc func(*schema.Schema) []string

// Apply implements Selector.
func (f SelectorFunc) Apply(s *schema.Schema) []string { return f(s) }

// All returns every column name in the schema.
func All() Selector {
	return SelectorFunc(func(s *schema.Schema) []string { return s.Names() })
}

// Named returns exactly the listed column names that exist in the
// schema (missing names are silently dropped).
func Named(names ...string) Selector {
	want := make(map[string]struct{}, len(names))
	for _, n := range names {
		want[n] = struct{}{}
	}
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for _, n := range s.Names() {
			if _, ok := want[n]; ok {
				out = append(out, n)
			}
		}
		return out
	})
}

// Exclude returns every column whose name is NOT in the list.
func Exclude(names ...string) Selector {
	skip := make(map[string]struct{}, len(names))
	for _, n := range names {
		skip[n] = struct{}{}
	}
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for _, n := range s.Names() {
			if _, ok := skip[n]; ok {
				continue
			}
			out = append(out, n)
		}
		return out
	})
}

// ByDtype returns every column whose dtype equals any of dt.
func ByDtype(dt ...dtype.DType) Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for i := range s.Len() {
			f := s.Field(i)
			if slices.ContainsFunc(dt, f.DType.Equal) {
				out = append(out, f.Name)
			}
		}
		return out
	})
}

// Numeric selects every integer or float column.
func Numeric() Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for i := range s.Len() {
			f := s.Field(i)
			if f.DType.IsNumeric() {
				out = append(out, f.Name)
			}
		}
		return out
	})
}

// Integer selects every integer column (signed or unsigned).
func Integer() Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for i := range s.Len() {
			f := s.Field(i)
			if f.DType.IsInteger() {
				out = append(out, f.Name)
			}
		}
		return out
	})
}

// Float selects every floating-point column.
func Float() Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for i := range s.Len() {
			f := s.Field(i)
			if f.DType.IsFloating() {
				out = append(out, f.Name)
			}
		}
		return out
	})
}

// StringCols selects every utf8 string column.
func StringCols() Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for i := range s.Len() {
			f := s.Field(i)
			if f.DType.IsString() {
				out = append(out, f.Name)
			}
		}
		return out
	})
}

// StartsWith returns columns whose name has the given prefix.
func StartsWith(prefix string) Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for _, n := range s.Names() {
			if strings.HasPrefix(n, prefix) {
				out = append(out, n)
			}
		}
		return out
	})
}

// EndsWith returns columns whose name ends with the given suffix.
func EndsWith(suffix string) Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for _, n := range s.Names() {
			if strings.HasSuffix(n, suffix) {
				out = append(out, n)
			}
		}
		return out
	})
}

// Contains returns columns whose name contains sub.
func Contains(sub string) Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for _, n := range s.Names() {
			if strings.Contains(n, sub) {
				out = append(out, n)
			}
		}
		return out
	})
}

// Matching returns columns whose name matches the given regular
// expression. Panics at construction time if pattern fails to
// compile: selectors are typically built from string literals.
func Matching(pattern string) Selector {
	re := regexp.MustCompile(pattern)
	return SelectorFunc(func(s *schema.Schema) []string {
		var out []string
		for _, n := range s.Names() {
			if re.MatchString(n) {
				out = append(out, n)
			}
		}
		return out
	})
}

// Union returns every column matched by any of the given selectors
// (order preserved by schema traversal, duplicates removed).
func Union(sels ...Selector) Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		seen := make(map[string]struct{}, s.Len())
		for _, sel := range sels {
			for _, n := range sel.Apply(s) {
				seen[n] = struct{}{}
			}
		}
		return orderedFrom(s, seen)
	})
}

// Intersect returns columns matched by every selector.
func Intersect(sels ...Selector) Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		if len(sels) == 0 {
			return nil
		}
		counts := make(map[string]int, s.Len())
		for _, sel := range sels {
			for _, n := range sel.Apply(s) {
				counts[n]++
			}
		}
		keep := make(map[string]struct{}, len(counts))
		for n, c := range counts {
			if c == len(sels) {
				keep[n] = struct{}{}
			}
		}
		return orderedFrom(s, keep)
	})
}

// Minus returns columns matched by a but not by b.
func Minus(a, b Selector) Selector {
	return SelectorFunc(func(s *schema.Schema) []string {
		drop := make(map[string]struct{})
		for _, n := range b.Apply(s) {
			drop[n] = struct{}{}
		}
		var out []string
		for _, n := range a.Apply(s) {
			if _, ok := drop[n]; !ok {
				out = append(out, n)
			}
		}
		return out
	})
}

// orderedFrom returns the names of the schema in order, filtered by
// the keep set.
func orderedFrom(s *schema.Schema, keep map[string]struct{}) []string {
	out := make([]string, 0, len(keep))
	for _, n := range s.Names() {
		if _, ok := keep[n]; ok {
			out = append(out, n)
		}
	}
	return out
}
