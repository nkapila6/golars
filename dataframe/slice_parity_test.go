// Behavioural parity with polars' tests/unit/operations/test_slice.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported (documented so the reader knows they're
// deliberate gaps, not oversights):
//
//   - Negative offsets (`df.slice(-5, 4)`)      : polars: count from
//     end; golars: returns ErrSliceOutOfBounds.
//   - Python-slice syntax (`df[3:-3:-1]`)        : Python-specific.
//   - Out-of-bounds length on eager DataFrame   : polars: clamps to
//     height; golars' DataFrame.Slice: error. LazyFrame.Slice does
//     clamp, so a test below covers the LazyFrame side. Eager Slice
//     has its own TestSliceOutOfBounds already.
//   - Slice-pushdown planner assertions         : we don't expose
//     explain-string keywords matching polars'.
//   - set_sorted / sorted metadata               : no sorted flag
//     tracking in golars yet.
//   - hconcat / hstack pushdown                 : no horizontal
//     concat planner fast path.

package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_python_slicing_data_frame first case: df.slice(1, 2)
// on 3-row frame yields the middle+last rows, preserving schema.
func TestParitySliceBasic(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromString("b", []string{"a", "b", "c"}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	got, err := df.Slice(1, 2)
	if err != nil {
		t.Fatalf("Slice: %v", err)
	}
	defer got.Release()

	if got.Height() != 2 {
		t.Fatalf("Height = %d, want 2", got.Height())
	}
	ac, _ := got.Column("a")
	bc, _ := got.Column("b")
	if v := ac.Chunk(0).(*array.Int64).Value(0); v != 2 {
		t.Errorf("a[0] = %d, want 2", v)
	}
	if v := ac.Chunk(0).(*array.Int64).Value(1); v != 3 {
		t.Errorf("a[1] = %d, want 3", v)
	}
	if v := bc.Chunk(0).(*array.String).Value(0); v != "b" {
		t.Errorf("b[0] = %q, want %q", v, "b")
	}
	if v := bc.Chunk(0).(*array.String).Value(1); v != "c" {
		t.Errorf("b[1] = %q, want %q", v, "c")
	}
}

// Polars: test_head_tail_limit: head/tail/limit clamp to height on
// out-of-bounds N, never error.
func TestParityHeadTailClampOOB(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil,
		series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	head := df.Head(100)
	defer head.Release()
	if head.Height() != 10 {
		t.Errorf("Head(100) height = %d, want 10", head.Height())
	}

	tail := df.Tail(100)
	defer tail.Release()
	if tail.Height() != 10 {
		t.Errorf("Tail(100) height = %d, want 10", tail.Height())
	}
}

// Polars: test_head_tail_limit: limit is an alias of head.
func TestParityLimitMatchesHead(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	lf := lazy.FromDataFrame(df)
	head, err := lf.Head(5).Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer head.Release()
	lim, err := lf.Limit(5).Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Release()

	if head.Height() != lim.Height() {
		t.Fatalf("head=%d, limit=%d", head.Height(), lim.Height())
	}
	hc := head.Columns()[0].Chunk(0).(*array.Int64)
	lc := lim.Columns()[0].Chunk(0).(*array.Int64)
	for i := range hc.Len() {
		if hc.Value(i) != lc.Value(i) {
			t.Fatalf("[%d] head=%d, limit=%d", i, hc.Value(i), lc.Value(i))
		}
	}
}

// Polars: test_python_slicing_lazy_frame first case: LazyFrame.slice
// clamps when length exceeds remaining rows.
func TestParityLazySliceClampsOOB(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	b, _ := series.FromString("b", []string{"a", "b", "c", "d"}, nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	lf := lazy.FromDataFrame(df)
	got, err := lf.Slice(2, 10).Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Height() != 2 {
		t.Fatalf("height = %d, want 2 (clamped)", got.Height())
	}
	ac := got.Columns()[0].Chunk(0).(*array.Int64)
	if ac.Value(0) != 3 || ac.Value(1) != 4 {
		t.Errorf("a = [%d, %d], want [3, 4]", ac.Value(0), ac.Value(1))
	}
}

// Polars: test_slice_nullcount parametrised: slice(offset, length)
// preserves each window's null count exactly, including nested
// slice(...).slice(...) composition.
func TestParitySliceNullCount(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Mixed: [0, null, 0, null, ...] x 128 → 256 values, 128 nulls.
	vals := make([]int64, 256)
	valid := make([]bool, 256)
	for i := range vals {
		if i%2 == 1 {
			valid[i] = false // null
		} else {
			valid[i] = true
			vals[i] = 0
		}
	}
	s, _ := series.FromInt64("a", vals, valid, series.WithAllocator(alloc))
	defer s.Release()

	if got := s.NullCount(); got != 128 {
		t.Fatalf("input null_count = %d, want 128", got)
	}

	// slice(64): from 64 onward. 256 - 64 = 192 values, alternating
	// starting at valid (index 64 is even) → half null = 96.
	tail, err := s.Slice(64, 256-64)
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Release()
	if got := tail.NullCount(); got != 96 {
		t.Fatalf("slice(64) null_count = %d, want 96", got)
	}

	// slice(50, 60) → rows [50..110), then .slice(25) → rows [75..110).
	// 110-75 = 35 values, indices 75..109. Odd indices (null) count:
	// 75,77,...,109 → 18 nulls.
	mid, err := s.Slice(50, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer mid.Release()
	sub, err := mid.Slice(25, mid.Len()-25)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Release()
	if got, want := sub.NullCount(), 18; got != want {
		t.Fatalf("nested slice null_count = %d, want %d", got, want)
	}
}

// Polars: test_python_slicing_series: series.slice positive cases.
// Negative offsets, None length, and slice-past-end are gated behind
// golars' stricter Series.Slice, which errors on OOB. We cover the
// behaviour-matching cases here.
func TestParitySeriesSlice(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{0, 1, 2, 3, 4, 5}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	cases := []struct {
		offset, length int
		want           []int64
	}{
		{2, 3, []int64{2, 3, 4}},
		{4, 1, []int64{4}},
		{4, 2, []int64{4, 5}},
		{3, 3, []int64{3, 4, 5}},
		{0, 6, []int64{0, 1, 2, 3, 4, 5}},
		{6, 0, []int64{}},
	}
	for _, tc := range cases {
		got, err := s.Slice(tc.offset, tc.length)
		if err != nil {
			t.Fatalf("slice(%d, %d): %v", tc.offset, tc.length, err)
		}
		if got.Len() != len(tc.want) {
			got.Release()
			t.Errorf("slice(%d, %d) len = %d, want %d",
				tc.offset, tc.length, got.Len(), len(tc.want))
			continue
		}
		if got.Len() > 0 {
			arr := got.Chunk(0).(*array.Int64)
			for i, w := range tc.want {
				if arr.Value(i) != w {
					t.Errorf("slice(%d, %d)[%d] = %d, want %d",
						tc.offset, tc.length, i, arr.Value(i), w)
				}
			}
		}
		got.Release()
	}
}

// Polars: test_tail_union: concat(a,b,c).tail(1) returns the last
// element across all frames.
func TestParityConcatTailReachesLastFrame(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	mk := func(vals []int64) *dataframe.DataFrame {
		s, _ := series.FromInt64("a", vals, nil, series.WithAllocator(alloc))
		df, _ := dataframe.New(s)
		return df
	}
	d1 := mk([]int64{1, 2})
	defer d1.Release()
	d2 := mk([]int64{3, 4})
	defer d2.Release()
	d3 := mk([]int64{5, 6})
	defer d3.Release()

	cat, err := dataframe.Concat(d1, d2, d3)
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Release()

	tail := cat.Tail(1)
	defer tail.Release()
	if tail.Height() != 1 {
		t.Fatalf("tail height = %d, want 1", tail.Height())
	}
	col, _ := tail.Column("a")
	if got := col.Chunk(0).(*array.Int64).Value(0); got != 6 {
		t.Errorf("tail[0] = %d, want 6", got)
	}
}
