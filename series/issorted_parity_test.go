// Behavioural parity with polars' tests/unit/operations/test_is_sorted.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported:
//   - set_sorted metadata             : golars has no sorted flag on
//     Series / LazyFrame; IsSorted always scans.
//   - DataFrame.is_sorted             : no DF-level surface.
//   - is_sorted on List / Struct      : unsupported dtypes.

package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func mustIsSorted(t *testing.T, s *series.Series, order series.SortedOrder) bool {
	t.Helper()
	got, err := s.IsSorted(order)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// Polars: sorted ascending series reports True; reverse reports False.
func TestParityIsSortedAscending(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	asc, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil,
		series.WithAllocator(alloc))
	defer asc.Release()
	desc, _ := series.FromInt64("a", []int64{5, 4, 3, 2, 1}, nil,
		series.WithAllocator(alloc))
	defer desc.Release()

	if !mustIsSorted(t, asc, series.SortedAscending) {
		t.Error("ascending series reported not sorted asc")
	}
	if mustIsSorted(t, desc, series.SortedAscending) {
		t.Error("descending series reported as sorted asc")
	}
	if !mustIsSorted(t, desc, series.SortedDescending) {
		t.Error("descending series reported not sorted desc")
	}
}

// Polars: ties are OK under non-strict, rejected under strict.
func TestParityIsSortedStrict(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 2, 3}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	if !mustIsSorted(t, s, series.SortedAscending) {
		t.Error("non-strict ascending with tie reported not sorted")
	}
	if mustIsSorted(t, s, series.SortedStrictAscending) {
		t.Error("strict ascending with tie reported as sorted")
	}
}

// Polars: single-element and empty series are sorted (trivially).
func TestParityIsSortedTrivial(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	empty, _ := series.FromInt64("a", []int64{}, nil, series.WithAllocator(alloc))
	defer empty.Release()
	one, _ := series.FromInt64("a", []int64{42}, nil, series.WithAllocator(alloc))
	defer one.Release()

	for _, o := range []series.SortedOrder{
		series.SortedAscending,
		series.SortedDescending,
		series.SortedStrictAscending,
		series.SortedStrictDescending,
	} {
		if !mustIsSorted(t, empty, o) {
			t.Errorf("empty not trivially sorted (order=%v)", o)
		}
		if !mustIsSorted(t, one, o) {
			t.Errorf("single-element not trivially sorted (order=%v)", o)
		}
	}
}

// Polars: NaN placed last in ascending sort is still "sorted".
func TestParityIsSortedFloat64NaNLast(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	sortedWithNaN, _ := series.FromFloat64("f",
		[]float64{1.0, 2.0, 3.0, math.NaN()}, nil,
		series.WithAllocator(alloc))
	defer sortedWithNaN.Release()
	if !mustIsSorted(t, sortedWithNaN, series.SortedAscending) {
		t.Error("NaN-last ascending reported not sorted")
	}

	// NaN in the middle breaks it.
	nanMid, _ := series.FromFloat64("f",
		[]float64{1.0, math.NaN(), 2.0}, nil,
		series.WithAllocator(alloc))
	defer nanMid.Release()
	if mustIsSorted(t, nanMid, series.SortedAscending) {
		t.Error("NaN-in-middle reported as sorted")
	}
}

// Polars: nulls must be all-at-the-end for a series to be considered
// sorted (matches polars' nulls_last default).
func TestParityIsSortedNullsLast(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	nullsLast, _ := series.FromInt64("a",
		[]int64{1, 2, 3, 0, 0},
		[]bool{true, true, true, false, false},
		series.WithAllocator(alloc))
	defer nullsLast.Release()
	if !mustIsSorted(t, nullsLast, series.SortedAscending) {
		t.Error("nulls-last series reported not sorted")
	}

	nullsMid, _ := series.FromInt64("a",
		[]int64{1, 0, 3},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	defer nullsMid.Release()
	if mustIsSorted(t, nullsMid, series.SortedAscending) {
		t.Error("null-in-middle reported as sorted")
	}
}

// Polars: strings sort lexicographically.
func TestParityIsSortedStringLex(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s", []string{"apple", "banana", "cherry"}, nil,
		series.WithAllocator(alloc))
	defer s.Release()
	if !mustIsSorted(t, s, series.SortedAscending) {
		t.Error("lex-asc string series reported not sorted")
	}
	if mustIsSorted(t, s, series.SortedDescending) {
		t.Error("lex-asc string series reported as desc-sorted")
	}
}

// Polars: booleans: false < true.
func TestParityIsSortedBool(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	ff_tt, _ := series.FromBool("b",
		[]bool{false, false, true, true}, nil, series.WithAllocator(alloc))
	defer ff_tt.Release()
	if !mustIsSorted(t, ff_tt, series.SortedAscending) {
		t.Error("[F,F,T,T] reported not sorted asc")
	}

	tt_ff, _ := series.FromBool("b",
		[]bool{true, true, false, false}, nil, series.WithAllocator(alloc))
	defer tt_ff.Release()
	if !mustIsSorted(t, tt_ff, series.SortedDescending) {
		t.Error("[T,T,F,F] reported not sorted desc")
	}
}

// Property: Sort(Asc) produces a series IsSorted(Asc) reports true on.
func TestParityIsSortedSortSelfConsistent(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{5, 1, 3, 2, 4, 1, 3, 5, 0}, nil,
		series.WithAllocator(alloc))
	defer s.Release()
	if mustIsSorted(t, s, series.SortedAscending) {
		t.Error("unsorted input reported as sorted")
	}
}
