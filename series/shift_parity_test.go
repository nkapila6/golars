// Behavioural parity with polars' tests/unit/operations/test_shift.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported (documented deliberate gaps):
//   - fill_value parameter           : golars' Shift always fills
//     the boundary with nulls. Polars optionally injects a literal
//     or expression. Feature-gated; add if needed.
//   - DataFrame.shift / LazyFrame.shift: golars has no DF-level
//     shift; Series.Shift is the only surface. (Scenarios: test_shift_
//     frame, test_shift_frame_with_fill, test_shift_fill_value.)
//   - Object / Categorical dtypes     : not supported.
//   - group-aware shift (.over(group)): no window ops.

package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_shift: positive period shifts values forward,
// filling the head with nulls.
func TestParityShiftForward(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Shift(1, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Int64)
	if got.NullCount() != 1 {
		t.Fatalf("null_count = %d, want 1", got.NullCount())
	}
	if arr.IsValid(0) {
		t.Errorf("[0] should be null")
	}
	if v := arr.Value(1); v != 1 || !arr.IsValid(1) {
		t.Errorf("[1] = %d (valid=%v), want 1 (valid=true)", v, arr.IsValid(1))
	}
	if v := arr.Value(2); v != 2 || !arr.IsValid(2) {
		t.Errorf("[2] = %d (valid=%v), want 2 (valid=true)", v, arr.IsValid(2))
	}
}

// Polars: test_shift with periods=-1: backward shift, tail null.
func TestParityShiftBackward(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Shift(-1, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Int64)
	if got.NullCount() != 1 {
		t.Fatalf("null_count = %d, want 1", got.NullCount())
	}
	if v := arr.Value(0); v != 2 || !arr.IsValid(0) {
		t.Errorf("[0] = %d, want 2", v)
	}
	if v := arr.Value(1); v != 3 || !arr.IsValid(1) {
		t.Errorf("[1] = %d, want 3", v)
	}
	if arr.IsValid(2) {
		t.Errorf("[2] should be null")
	}
}

// Polars: test_shift with periods=-2: larger absolute shift.
func TestParityShiftBackwardLarge(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Shift(-2, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Int64)
	if got.NullCount() != 2 {
		t.Fatalf("null_count = %d, want 2", got.NullCount())
	}
	if v := arr.Value(0); v != 3 || !arr.IsValid(0) {
		t.Errorf("[0] = %d (valid=%v), want 3", v, arr.IsValid(0))
	}
	for i := 1; i < 3; i++ {
		if arr.IsValid(i) {
			t.Errorf("[%d] should be null", i)
		}
	}
}

// Polars property: shift by abs(n) >= length returns all-null.
func TestParityShiftBeyondLenAllNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()

	for _, periods := range []int{3, 4, -3, -100} {
		got, err := s.Shift(periods, series.WithAllocator(alloc))
		if err != nil {
			t.Fatalf("shift(%d): %v", periods, err)
		}
		if got.Len() != 3 {
			t.Errorf("shift(%d) len = %d, want 3", periods, got.Len())
		}
		if got.NullCount() != 3 {
			t.Errorf("shift(%d) null_count = %d, want 3", periods, got.NullCount())
		}
		got.Release()
	}
}

// Polars property: shift(0) is identity.
func TestParityShiftZero(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{10, 20, 30, 40}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Shift(0, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.NullCount() != 0 {
		t.Errorf("null_count = %d, want 0", got.NullCount())
	}
	arr := got.Chunk(0).(*array.Int64)
	want := []int64{10, 20, 30, 40}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: test_shift across supported dtypes (float, string, bool).
func TestParityShiftDtypes(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	t.Run("float64", func(t *testing.T) {
		s, _ := series.FromFloat64("f", []float64{1.5, 2.5, 3.5}, nil,
			series.WithAllocator(alloc))
		defer s.Release()
		got, err := s.Shift(1, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		arr := got.Chunk(0).(*array.Float64)
		if arr.IsValid(0) {
			t.Errorf("[0] should be null")
		}
		if arr.Value(1) != 1.5 {
			t.Errorf("[1] = %v, want 1.5", arr.Value(1))
		}
	})

	t.Run("string", func(t *testing.T) {
		s, _ := series.FromString("s", []string{"a", "b", "c"}, nil,
			series.WithAllocator(alloc))
		defer s.Release()
		got, err := s.Shift(-1, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		arr := got.Chunk(0).(*array.String)
		if arr.Value(0) != "b" || !arr.IsValid(0) {
			t.Errorf("[0] = %q, want %q", arr.Value(0), "b")
		}
		if arr.Value(1) != "c" || !arr.IsValid(1) {
			t.Errorf("[1] = %q, want %q", arr.Value(1), "c")
		}
		if arr.IsValid(2) {
			t.Errorf("[2] should be null")
		}
	})

	t.Run("bool", func(t *testing.T) {
		s, _ := series.FromBool("b", []bool{true, false, true}, nil,
			series.WithAllocator(alloc))
		defer s.Release()
		got, err := s.Shift(1, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		arr := got.Chunk(0).(*array.Boolean)
		if arr.IsValid(0) {
			t.Errorf("[0] should be null")
		}
		if arr.Value(1) != true {
			t.Errorf("[1] = %v, want true", arr.Value(1))
		}
		if arr.Value(2) != false {
			t.Errorf("[2] = %v, want false", arr.Value(2))
		}
	})
}

// Polars property: shift preserves nulls from source.
func TestParityShiftPreservesSourceNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// [1, null, 3, 4] shifted by 1 → [null, 1, null, 3]: the original
	// null at position 1 propagates to position 2.
	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 4},
		[]bool{true, false, true, true},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.Shift(1, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Int64)
	if arr.IsValid(0) {
		t.Errorf("[0] should be null (shift-induced)")
	}
	if v := arr.Value(1); v != 1 || !arr.IsValid(1) {
		t.Errorf("[1] = %d (valid=%v), want 1", v, arr.IsValid(1))
	}
	if arr.IsValid(2) {
		t.Errorf("[2] should be null (propagated from source)")
	}
	if v := arr.Value(3); v != 3 || !arr.IsValid(3) {
		t.Errorf("[3] = %d (valid=%v), want 3", v, arr.IsValid(3))
	}
	if got.NullCount() != 2 {
		t.Errorf("null_count = %d, want 2", got.NullCount())
	}
}

// Shift composition: shift(k).shift(-k) recovers a middle window
// of the original values with nulls padding both ends.
func TestParityShiftRoundTrip(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{10, 20, 30, 40, 50}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	a, _ := s.Shift(2, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := a.Shift(-2, series.WithAllocator(alloc))
	defer b.Release()

	// Result: [10, 20, 30, null, null]: the last two rows were shifted
	// in from null in `a` and then shifted left, leaving them null.
	arr := b.Chunk(0).(*array.Int64)
	want := []int64{10, 20, 30}
	for i, w := range want {
		if !arr.IsValid(i) || arr.Value(i) != w {
			t.Errorf("[%d] = %d (valid=%v), want %d", i, arr.Value(i),
				arr.IsValid(i), w)
		}
	}
	for i := 3; i < 5; i++ {
		if arr.IsValid(i) {
			t.Errorf("[%d] should be null", i)
		}
	}
}
