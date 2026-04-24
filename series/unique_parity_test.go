// Behavioural parity with polars' tests/unit/operations/unique/test_*.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported (documented deliberate gaps):
//   - DataFrame.unique / LazyFrame.unique: golars has Series.Unique
//     only; there's no row-level distinct on a DataFrame.
//   - unique(subset=...)            : needs DF.unique.
//   - unique(keep="last"/"none")    : golars keeps first occurrence
//     always (polars' "any"/"first" default).
//   - arg_unique / unique_counts    : not implemented.
//   - unique on List / Struct dtypes: no such dtypes in golars.

package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: list/test_n_unique.py style: unique returns distinct
// values in first-occurrence order for the int path.
func TestParityUniqueInt64MaintainOrder(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{3, 1, 3, 2, 1, 4, 3}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Int64)
	want := []int64{3, 1, 2, 4}
	if arr.Len() != len(want) {
		t.Fatalf("len = %d, want %d", arr.Len(), len(want))
	}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: unique on nullable Series keeps exactly one null at the
// end (maintain_order + null_collapse).
func TestParityUniqueCollapsesNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// [1, null, 2, null, 1, 3, null] → [1, 2, 3, null]
	s, _ := series.FromInt64("a",
		[]int64{1, 0, 2, 0, 1, 3, 0},
		[]bool{true, false, true, false, true, true, false},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Len() != 4 {
		t.Fatalf("len = %d, want 4", got.Len())
	}
	if got.NullCount() != 1 {
		t.Fatalf("null_count = %d, want 1", got.NullCount())
	}
	arr := got.Chunk(0).(*array.Int64)
	if !arr.IsValid(0) || arr.Value(0) != 1 ||
		!arr.IsValid(1) || arr.Value(1) != 2 ||
		!arr.IsValid(2) || arr.Value(2) != 3 ||
		arr.IsValid(3) {
		t.Errorf("values = [%d, %d, %d, valid=%v]; last entry should be null",
			arr.Value(0), arr.Value(1), arr.Value(2), arr.IsValid(3))
	}
}

// Polars: test_unique NaN collapse: all NaN payloads map to the same
// unique slot (single NaN in output).
func TestParityUniqueFloat64CollapsesNaN(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	nan1 := math.NaN()
	nan2 := math.Float64frombits(0x7FF8000000000001) // distinct NaN bits
	s, _ := series.FromFloat64("a",
		[]float64{1.5, nan1, 2.5, nan2, 1.5},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	// Expect 3 entries: 1.5, NaN, 2.5 (in first-occurrence order).
	if got.Len() != 3 {
		t.Fatalf("len = %d, want 3", got.Len())
	}
	arr := got.Chunk(0).(*array.Float64)
	if arr.Value(0) != 1.5 {
		t.Errorf("[0] = %v, want 1.5", arr.Value(0))
	}
	if !math.IsNaN(arr.Value(1)) {
		t.Errorf("[1] = %v, want NaN", arr.Value(1))
	}
	if arr.Value(2) != 2.5 {
		t.Errorf("[2] = %v, want 2.5", arr.Value(2))
	}
}

// Polars: test_unique float -0/+0 collapse: a Series containing both
// zeros returns a single 0 entry.
func TestParityUniqueFloat64CollapsesSignedZero(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	negZero := math.Copysign(0, -1)
	s, _ := series.FromFloat64("a", []float64{0, negZero, 0}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Len() != 1 {
		t.Fatalf("len = %d, want 1 (both zeros collapse)", got.Len())
	}
}

// Polars: test_unique on strings keeps first occurrence order.
func TestParityUniqueStringOrdered(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"banana", "apple", "banana", "cherry", "apple"},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.String)
	want := []string{"banana", "apple", "cherry"}
	if arr.Len() != len(want) {
		t.Fatalf("len = %d, want %d", arr.Len(), len(want))
	}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %q, want %q", i, arr.Value(i), w)
		}
	}
}

// Polars: test_unique on booleans yields at most 3 entries
// (true, false, null).
func TestParityUniqueBoolean(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromBool("b",
		[]bool{true, false, true, false, false, true, false},
		[]bool{true, true, true, false, true, true, true},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Len() != 3 {
		t.Fatalf("len = %d, want 3 (true, false, null)", got.Len())
	}
	if got.NullCount() != 1 {
		t.Errorf("null_count = %d, want 1", got.NullCount())
	}
}

// Polars: s.n_unique() excludes nulls.
func TestParityNUniqueExcludesNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 1, 2, 0, 3},
		[]bool{true, false, true, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.NUnique()
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Errorf("n_unique = %d, want 3", got)
	}
}

// Polars: empty Series -> empty unique.
func TestParityUniqueEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{}, nil, series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Len() != 0 {
		t.Errorf("len = %d, want 0", got.Len())
	}
}
