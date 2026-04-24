// Package testutil provides test helpers for the golars module.
//
// The central helper is NewCheckedAllocator: it returns a memory.Allocator
// that panics at teardown if any allocation was not released. Use it in every
// test that constructs arrow arrays, Series, or DataFrames so that reference
// counting bugs fail loudly during CI rather than lingering as GC noise.
package testutil

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// NewCheckedAllocator returns a memory.CheckedAllocator bound to t. At the
// end of the test it asserts that every allocation was released and fails t
// if any remain.
func NewCheckedAllocator(t testing.TB) *memory.CheckedAllocator {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() {
		mem.AssertSize(t, 0)
	})
	return mem
}

// AssertStrings fails t if got and want do not have the same contents in the
// same order.
func AssertStrings(t testing.TB, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("length mismatch: got %d, want %d\n  got: %v\n  want: %v",
			len(got), len(want), got, want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("mismatch at index %d: got %q, want %q\n  got: %v\n  want: %v",
				i, got[i], want[i], got, want)
			return
		}
	}
}

// AssertEqualAny compares two slices elementwise using equality. It reports
// all mismatches in a single error, up to 10.
func AssertEqualAny[T comparable](t testing.TB, got, want []T) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("length mismatch: got %d, want %d", len(got), len(want))
		return
	}
	var b strings.Builder
	n := 0
	for i := range got {
		if got[i] != want[i] {
			if n < 10 {
				if n > 0 {
					b.WriteString("; ")
				}
				b.WriteString("index ")
				writeInt(&b, i)
			}
			n++
		}
	}
	if n > 0 {
		t.Errorf("%d mismatches (first %s): got=%v want=%v",
			n, b.String(), got, want)
	}
}

func writeInt(b *strings.Builder, n int) {
	if n == 0 {
		b.WriteByte('0')
		return
	}
	if n < 0 {
		b.WriteByte('-')
		n = -n
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	b.Write(digits[i:])
}
