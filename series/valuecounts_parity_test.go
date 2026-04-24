// Behavioural parity with polars' tests/unit/operations/test_value_counts.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported:
//   - normalize=True (proportions)   : golars returns raw counts only.
//   - value_counts as expression inside groupby agg: no expression-
//     level value_counts.
//   - struct-typed output (polars packs into a single struct column);
//     golars returns two parallel Series instead.
//   - Categorical / Enum key dtype    : not supported.

package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_value_counts: counts on a small integer series match
// hand-computed values. We test first-occurrence order here, then the
// sorted variant separately.
func TestParityValueCountsInt64FirstOccurrenceOrder(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 2, 3}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	vals, counts, err := s.ValueCounts(false, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()

	if vals.Len() != 3 {
		t.Fatalf("vals.Len() = %d, want 3", vals.Len())
	}
	vArr := vals.Chunk(0).(*array.Int64)
	cArr := counts.Chunk(0).(*array.Uint32)
	wantV := []int64{1, 2, 3}
	wantC := []uint32{1, 2, 1}
	for i := range 3 {
		if vArr.Value(i) != wantV[i] || cArr.Value(i) != wantC[i] {
			t.Errorf("[%d] = (%d, %d), want (%d, %d)",
				i, vArr.Value(i), cArr.Value(i), wantV[i], wantC[i])
		}
	}
}

// Polars: value_counts(sort=True): descending count order.
func TestParityValueCountsSortedDescending(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// From polars' test: [12, 3345, 12, 3, 4, 4, 1, 12]: counts are
	// 12→3, 4→2, 3345→1, 3→1, 1→1 (ties broken by first-occurrence).
	s, _ := series.FromInt64("a",
		[]int64{12, 3345, 12, 3, 4, 4, 1, 12},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	vals, counts, err := s.ValueCounts(true, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()

	cArr := counts.Chunk(0).(*array.Uint32)
	for i := 1; i < cArr.Len(); i++ {
		if cArr.Value(i) > cArr.Value(i-1) {
			t.Errorf("counts[%d]=%d > counts[%d]=%d (not descending)",
				i, cArr.Value(i), i-1, cArr.Value(i-1))
		}
	}

	vArr := vals.Chunk(0).(*array.Int64)
	if vArr.Value(0) != 12 {
		t.Errorf("top value = %d, want 12 (count 3)", vArr.Value(0))
	}
	if cArr.Value(0) != 3 {
		t.Errorf("top count = %d, want 3", cArr.Value(0))
	}
}

// Polars: test_value_counts_expr: string keys; sort=True brings the
// most frequent value to the front.
func TestParityValueCountsString(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("id",
		[]string{"a", "b", "b", "c", "c", "c", "d", "d"},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	vals, counts, err := s.ValueCounts(true, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()

	vArr := vals.Chunk(0).(*array.String)
	cArr := counts.Chunk(0).(*array.Uint32)
	if vArr.Value(0) != "c" || cArr.Value(0) != 3 {
		t.Errorf("top = (%q, %d), want (c, 3)", vArr.Value(0), cArr.Value(0))
	}
	// b and d both have count=2; first-occurrence keeps b before d.
	if cArr.Value(1) != 2 || cArr.Value(2) != 2 {
		t.Errorf("ties = (%d, %d), want (2, 2)", cArr.Value(1), cArr.Value(2))
	}
	if vArr.Value(1) != "b" || vArr.Value(2) != "d" {
		t.Errorf("tie order = (%q, %q), want (b, d) by first occurrence",
			vArr.Value(1), vArr.Value(2))
	}
}

// Polars: nulls are dropped from value_counts (default).
func TestParityValueCountsDropsNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 2, 0, 1},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	vals, counts, err := s.ValueCounts(false, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()

	// Expect 1→2, 2→1; nulls dropped.
	if vals.Len() != 2 {
		t.Fatalf("len = %d, want 2", vals.Len())
	}
	cArr := counts.Chunk(0).(*array.Uint32)
	if cArr.Value(0) != 2 || cArr.Value(1) != 1 {
		t.Errorf("counts = [%d, %d], want [2, 1]",
			cArr.Value(0), cArr.Value(1))
	}
}

// Polars: value_counts on boolean: at most {true, false}.
func TestParityValueCountsBoolean(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromBool("b",
		[]bool{true, false, true, true, false, true},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	vals, counts, err := s.ValueCounts(true, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()

	if vals.Len() != 2 {
		t.Fatalf("len = %d, want 2", vals.Len())
	}
	vArr := vals.Chunk(0).(*array.Boolean)
	cArr := counts.Chunk(0).(*array.Uint32)
	// Sorted descending: true(4) first, then false(2).
	if vArr.Value(0) != true || cArr.Value(0) != 4 {
		t.Errorf("top = (%v, %d), want (true, 4)", vArr.Value(0), cArr.Value(0))
	}
	if vArr.Value(1) != false || cArr.Value(1) != 2 {
		t.Errorf("second = (%v, %d), want (false, 2)",
			vArr.Value(1), cArr.Value(1))
	}
}

// Polars: value_counts on empty Series returns empty pair.
func TestParityValueCountsEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{}, nil, series.WithAllocator(alloc))
	defer s.Release()

	vals, counts, err := s.ValueCounts(false, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()

	if vals.Len() != 0 {
		t.Errorf("vals.Len() = %d, want 0", vals.Len())
	}
	if counts.Len() != 0 {
		t.Errorf("counts.Len() = %d, want 0", counts.Len())
	}
}

// Polars: total count equals len(s) minus nulls.
func TestParityValueCountsSumEqualsNonNullLen(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"a", "", "a", "b", "c", "", "b"},
		[]bool{true, false, true, true, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	nonNull := s.Len() - s.NullCount()

	vals, counts, err := s.ValueCounts(false, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()

	cArr := counts.Chunk(0).(*array.Uint32)
	var sum uint32
	for i := range cArr.Len() {
		sum += cArr.Value(i)
	}
	if int(sum) != nonNull {
		t.Errorf("sum(counts) = %d, want %d", sum, nonNull)
	}
}
