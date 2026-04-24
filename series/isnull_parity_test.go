// Behavioural parity with polars' tests/unit/operations/test_is_null.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported:
//   - DataFrame.is_null / LazyFrame.null_count columns: golars has
//     Series-level IsNull / IsNotNull plus DataFrame.NullCount(); no
//     row-wise "does this row contain any null" predicate.
//   - has_nulls(): golars uses `s.NullCount() > 0`.
//   - is_nan / is_not_nan (float-specific): separate tests; skipped
//     here because IsNull returns a null-bitmap mask only.

package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: s.is_null() marks every null position true; non-null false.
func TestParityIsNullInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	mask, err := s.IsNull(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer mask.Release()

	if mask.NullCount() != 0 {
		t.Errorf("IsNull mask itself has nulls: %d", mask.NullCount())
	}
	arr := mask.Chunk(0).(*array.Boolean)
	want := []bool{false, true, false, true, false}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %v, want %v", i, arr.Value(i), w)
		}
	}
}

// Polars: is_not_null is the elementwise inverse of is_null.
func TestParityIsNotNullInverse(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	n, _ := s.IsNull(series.WithAllocator(alloc))
	defer n.Release()
	nn, _ := s.IsNotNull(series.WithAllocator(alloc))
	defer nn.Release()

	na := n.Chunk(0).(*array.Boolean)
	nna := nn.Chunk(0).(*array.Boolean)
	for i := range na.Len() {
		if na.Value(i) == nna.Value(i) {
			t.Errorf("[%d] is_null=%v, is_not_null=%v (should differ)",
				i, na.Value(i), nna.Value(i))
		}
	}
}

// Polars: is_null on a no-null series is all-false.
func TestParityIsNullNoNullsAllFalse(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	mask, _ := s.IsNull(series.WithAllocator(alloc))
	defer mask.Release()
	arr := mask.Chunk(0).(*array.Boolean)
	for i := range arr.Len() {
		if arr.Value(i) {
			t.Errorf("[%d] = true, want false (no nulls in source)", i)
		}
	}
}

// Polars: is_null on all-null series is all-true.
func TestParityIsNullAllNullsAllTrue(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{0, 0, 0},
		[]bool{false, false, false},
		series.WithAllocator(alloc))
	defer s.Release()

	mask, _ := s.IsNull(series.WithAllocator(alloc))
	defer mask.Release()
	arr := mask.Chunk(0).(*array.Boolean)
	for i := range arr.Len() {
		if !arr.Value(i) {
			t.Errorf("[%d] = false, want true (all nulls)", i)
		}
	}
}

// Polars: is_null output dtype is always Boolean; length matches input.
func TestParityIsNullShapePreserved(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	for _, n := range []int{0, 1, 7, 64, 65, 1024} {
		vals := make([]int64, n)
		valid := make([]bool, n)
		for i := range valid {
			valid[i] = i%2 == 0
		}
		s, _ := series.FromInt64("a", vals, valid, series.WithAllocator(alloc))
		mask, err := s.IsNull(series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		if mask.Len() != n {
			t.Errorf("n=%d: mask.Len() = %d", n, mask.Len())
		}
		if mask.DType().String() != "bool" {
			t.Errorf("n=%d: dtype = %s", n, mask.DType())
		}
		mask.Release()
		s.Release()
	}
}

// Polars: is_null works across dtypes (string, bool, float64).
func TestParityIsNullDtypes(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	t.Run("string", func(t *testing.T) {
		s, _ := series.FromString("s",
			[]string{"a", "", "c"},
			[]bool{true, false, true},
			series.WithAllocator(alloc))
		defer s.Release()
		mask, _ := s.IsNull(series.WithAllocator(alloc))
		defer mask.Release()
		arr := mask.Chunk(0).(*array.Boolean)
		if arr.Value(0) || !arr.Value(1) || arr.Value(2) {
			t.Errorf("mask = [%v, %v, %v], want [false, true, false]",
				arr.Value(0), arr.Value(1), arr.Value(2))
		}
	})

	t.Run("float64", func(t *testing.T) {
		s, _ := series.FromFloat64("f",
			[]float64{1.5, 0, 2.5},
			[]bool{true, false, true},
			series.WithAllocator(alloc))
		defer s.Release()
		mask, _ := s.IsNull(series.WithAllocator(alloc))
		defer mask.Release()
		arr := mask.Chunk(0).(*array.Boolean)
		if arr.Value(0) || !arr.Value(1) || arr.Value(2) {
			t.Errorf("float mask wrong")
		}
	})

	t.Run("bool", func(t *testing.T) {
		s, _ := series.FromBool("b",
			[]bool{true, false, true},
			[]bool{true, false, true},
			series.WithAllocator(alloc))
		defer s.Release()
		mask, _ := s.IsNull(series.WithAllocator(alloc))
		defer mask.Release()
		arr := mask.Chunk(0).(*array.Boolean)
		if arr.Value(0) || !arr.Value(1) || arr.Value(2) {
			t.Errorf("bool mask wrong")
		}
	})
}
