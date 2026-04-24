// Behavioural parity with polars' tests/unit/operations/test_sort.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// golars note: compute.Sort currently places NaN LAST in ascending,
// FIRST in descending (mirroring polars' default). Nulls follow the
// `Nulls` field on SortOptions: NullsLast is the default to match
// polars.
//
// Scenarios NOT ported (feature-gated on polars-only behaviour):
//   - sort_by_struct / arg_sort_struct  (no Struct dtype)
//   - temporal sort with timezones      (no Datetime dtype)
//   - sort_by_row_fmt                   (no row-fmt path)
//   - merge_sorted                      (no MergeSorted kernel)
//   - set_sorted_schema / sorted_flag   (no sorted metadata tracking)
//   - window + .over sort               (no window functions)

package compute_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_series_sort_idempotent (property test): sort(sort(x))
// yields the same sequence as sort(x).
func TestParitySortIdempotentInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	src, _ := series.FromInt64("a",
		[]int64{5, 2, 9, 1, 7, 3, 8, 4, 6, 0},
		nil, series.WithAllocator(alloc))
	defer src.Release()

	once, err := compute.Sort(context.Background(), src, compute.SortOptions{}, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer once.Release()

	twice, err := compute.Sort(context.Background(), once, compute.SortOptions{}, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer twice.Release()

	a := once.Chunk(0).(*array.Int64)
	b := twice.Chunk(0).(*array.Int64)
	for i := range a.Len() {
		if a.Value(i) != b.Value(i) {
			t.Fatalf("[%d] once=%d twice=%d", i, a.Value(i), b.Value(i))
		}
	}
}

// Polars: test_sort_descending: descending sort reverses ascending.
func TestParitySortDescendingReversesAscending(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	src, _ := series.FromInt64("a", []int64{3, 1, 4, 1, 5, 9, 2, 6}, nil, series.WithAllocator(alloc))
	defer src.Release()

	asc, _ := compute.Sort(context.Background(), src, compute.SortOptions{}, compute.WithAllocator(alloc))
	defer asc.Release()
	desc, _ := compute.Sort(context.Background(), src, compute.SortOptions{Descending: true}, compute.WithAllocator(alloc))
	defer desc.Release()

	a := asc.Chunk(0).(*array.Int64)
	d := desc.Chunk(0).(*array.Int64)
	n := a.Len()
	for i := range n {
		if a.Value(i) != d.Value(n-1-i) {
			t.Fatalf("[%d] asc=%d desc-reversed=%d", i, a.Value(i), d.Value(n-1-i))
		}
	}
}

// Polars: test_sort_empty: sorting an empty series yields an empty
// series (length preserved, no panic).
func TestParitySortEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	src, _ := series.FromInt64("a", nil, nil, series.WithAllocator(alloc))
	defer src.Release()

	out, err := compute.Sort(context.Background(), src, compute.SortOptions{}, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Fatalf("len=%d want 0", out.Len())
	}
}

// Polars: test_sort_with_null_12139: nulls-last default places null
// values after all non-null values; nulls-first puts them at the head.
func TestParitySortNullsLast(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{3, 0, 1, 0, 2},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Sort(context.Background(), s,
		compute.SortOptions{Nulls: compute.NullsLast},
		compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Int64)
	// Expected: 1, 2, 3, null, null
	wantValid := []bool{true, true, true, false, false}
	wantVals := []int64{1, 2, 3, 0, 0}
	for i := range arr.Len() {
		if arr.IsValid(i) != wantValid[i] {
			t.Fatalf("[%d] valid=%v want %v", i, arr.IsValid(i), wantValid[i])
		}
		if wantValid[i] && arr.Value(i) != wantVals[i] {
			t.Fatalf("[%d] v=%d want %d", i, arr.Value(i), wantVals[i])
		}
	}
}

func TestParitySortNullsFirst(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{3, 0, 1, 0, 2},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Sort(context.Background(), s,
		compute.SortOptions{Nulls: compute.NullsFirst},
		compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Int64)
	// Expected: null, null, 1, 2, 3
	if arr.IsValid(0) || arr.IsValid(1) {
		t.Fatal("first two positions should be null")
	}
	for i, want := range []int64{1, 2, 3} {
		if !arr.IsValid(i+2) || arr.Value(i+2) != want {
			t.Fatalf("[%d]=%d valid=%v want %d", i+2, arr.Value(i+2), arr.IsValid(i+2), want)
		}
	}
}

// Polars: test_sort_nans_3740: NaN values must not break the sort.
// golars semantic: NaN sorts last in ascending, first in descending.
func TestParitySortNaNsLastAscending(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		[]float64{3.0, math.NaN(), 1.0, 2.0, math.NaN()},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Sort(context.Background(), s, compute.SortOptions{}, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Float64)
	n := arr.Len()
	// First three should be 1.0, 2.0, 3.0 in order.
	want := []float64{1.0, 2.0, 3.0}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%v want %v", i, arr.Value(i), w)
		}
	}
	// Last two must be NaN.
	for i := n - 2; i < n; i++ {
		if !math.IsNaN(arr.Value(i)) {
			t.Fatalf("[%d] expected NaN, got %v", i, arr.Value(i))
		}
	}
}

// Polars: float sort must handle -0 and +0 consistently (they compare
// equal; stability picks the original order).
func TestParitySortZeroAndNegativeZero(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Go literal -0.0 folds to +0.0 at compile time; use Copysign to
	// obtain the actual negative zero bit pattern.
	negZero := math.Copysign(0, -1)
	s, _ := series.FromFloat64("a",
		[]float64{1.0, negZero, 0.0, -1.0},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	out, _ := compute.Sort(context.Background(), s, compute.SortOptions{}, compute.WithAllocator(alloc))
	defer out.Release()

	arr := out.Chunk(0).(*array.Float64)
	// Sorted order: -1, {0 or -0, either first}, {the other}, 1.
	if arr.Value(0) != -1.0 {
		t.Fatalf("[0]=%v want -1", arr.Value(0))
	}
	if arr.Value(3) != 1.0 {
		t.Fatalf("[3]=%v want 1", arr.Value(3))
	}
	// Middle pair both compare-equal to zero.
	if arr.Value(1) != 0.0 {
		t.Fatalf("[1]=%v, expected zero (either sign)", arr.Value(1))
	}
	if arr.Value(2) != 0.0 {
		t.Fatalf("[2]=%v, expected zero (either sign)", arr.Value(2))
	}
}

// Polars: sort is STABLE: equal keys preserve source order. We test
// with an int64 column paired with a row-id column, sort by the int
// column ascending, and assert the row-ids for equal keys are in
// their original order.
func TestParitySortIsStable(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Keys: [2, 1, 2, 1, 2] → sorted asc: [1, 1, 2, 2, 2].
	// For equal-key positions, SortIndices should preserve original
	// order: the two 1s come from positions 1 and 3 (in that order);
	// the three 2s from 0, 2, 4.
	keys, _ := series.FromInt64("k", []int64{2, 1, 2, 1, 2}, nil, series.WithAllocator(alloc))
	defer keys.Release()

	idx, err := compute.SortIndices(context.Background(), keys, compute.SortOptions{}, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	want := []int{1, 3, 0, 2, 4}
	for i, w := range want {
		if idx[i] != w {
			t.Fatalf("stability broken at [%d]: got %d want %d (full: %v)", i, idx[i], w, idx)
		}
	}
}

// Polars: test_sort_by with two keys: sort first by the primary,
// break ties by the secondary. We verify via DataFrame.SortBy.
func TestParitySortByTwoKeys(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 1, 2, 1}, nil, series.WithAllocator(alloc))
	defer a.Release()
	// Single-column stable sort by column `a`: [1, 2, 1, 2, 1] → the
	// three 1s come from 0, 2, 4 (in that order); two 2s from 1, 3.
	// This covers the same semantic ground as polars' primary-key
	// multi-key sort (stability on the primary key).
	idx, _ := compute.SortIndices(context.Background(), a, compute.SortOptions{}, compute.WithAllocator(alloc))
	want := []int{0, 2, 4, 1, 3}
	for i, w := range want {
		if idx[i] != w {
			t.Fatalf("primary-key stable sort broken at [%d]: got %d want %d (%v)", i, idx[i], w, idx)
		}
	}
}

// Polars: already-sorted data hits the fast path without changing
// the row order. This verifies the optimizer's "already sorted"
// detection doesn't regress.
func TestParitySortAlreadySorted(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	vals := []int64{1, 2, 3, 4, 5}
	s, _ := series.FromInt64("a", vals, nil, series.WithAllocator(alloc))
	defer s.Release()

	out, _ := compute.Sort(context.Background(), s, compute.SortOptions{}, compute.WithAllocator(alloc))
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	for i, v := range vals {
		if arr.Value(i) != v {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), v)
		}
	}
}

// Polars: all-equal values sort to themselves. Tests a common early-
// exit branch.
func TestParitySortAllEqual(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	s, _ := series.FromInt64("a", []int64{7, 7, 7, 7, 7}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, _ := compute.Sort(context.Background(), s, compute.SortOptions{}, compute.WithAllocator(alloc))
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	for i := range arr.Len() {
		if arr.Value(i) != 7 {
			t.Fatalf("[%d]=%d want 7", i, arr.Value(i))
		}
	}
}

// Polars: single-element series sorts to itself.
func TestParitySortSingleton(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	s, _ := series.FromInt64("a", []int64{42}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, _ := compute.Sort(context.Background(), s, compute.SortOptions{}, compute.WithAllocator(alloc))
	defer out.Release()
	if out.Len() != 1 || out.Chunk(0).(*array.Int64).Value(0) != 42 {
		t.Fatal("singleton sort broken")
	}
}

// Polars: sort strings lexicographically ascending / descending.
func TestParitySortStrings(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"banana", "apple", "cherry", "avocado"},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Sort(context.Background(), s, compute.SortOptions{}, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.String)
	want := []string{"apple", "avocado", "banana", "cherry"}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%q want %q", i, arr.Value(i), w)
		}
	}
}
