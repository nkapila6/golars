// Behavioural parity with polars' tests/unit/operations/test_fill_null.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported:
//   - Strategy-based fill (forward/backward/min/max/mean/zero/one)
//    : golars supports only constant-value fill.
//   - fill_null(expression)        : expression-level fill.
//   - DataFrame.fill_null / fill_nan: no DF-level surface.
//   - Dtype upcast on too-large fill value: golars errors instead
//     (the int32 caller must pick a fitting value).
//   - Decimal / Enum / Categorical / Date dtypes: not implemented.

package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_fill_null: int64 fill replaces every null with the
// given scalar and produces a fully-valid series.
func TestParityFillNullInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.FillNull(int64(-1), series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.NullCount() != 0 {
		t.Fatalf("null_count = %d, want 0", got.NullCount())
	}
	arr := got.Chunk(0).(*array.Int64)
	want := []int64{1, -1, 3, -1, 5}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: fill_null on a series with no nulls is a no-op.
func TestParityFillNullNoOpWhenNoNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.FillNull(int64(0), series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.NullCount() != 0 {
		t.Fatalf("null_count = %d, want 0", got.NullCount())
	}
	arr := got.Chunk(0).(*array.Int64)
	want := []int64{1, 2, 3}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: fill_null on an all-null series returns an all-filled series.
func TestParityFillNullAllNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{0, 0, 0},
		[]bool{false, false, false},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.FillNull(int64(99), series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.NullCount() != 0 {
		t.Fatalf("null_count = %d, want 0", got.NullCount())
	}
	arr := got.Chunk(0).(*array.Int64)
	for i := range 3 {
		if arr.Value(i) != 99 {
			t.Errorf("[%d] = %d, want 99", i, arr.Value(i))
		}
	}
}

// Polars: dtype preservation: fill_null(Float64, 0) stays f64.
func TestParityFillNullFloat64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("f",
		[]float64{1.5, 0, 3.5},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.FillNull(0.0, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.NullCount() != 0 {
		t.Fatalf("null_count = %d, want 0", got.NullCount())
	}
	if got.DType().String() != "f64" {
		t.Errorf("dtype = %s, want f64", got.DType())
	}
	arr := got.Chunk(0).(*array.Float64)
	if arr.Value(1) != 0.0 {
		t.Errorf("[1] = %v, want 0", arr.Value(1))
	}
}

// Polars: fill_null on string series.
func TestParityFillNullString(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"a", "", "c"},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.FillNull("MISSING", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.NullCount() != 0 {
		t.Fatalf("null_count = %d, want 0", got.NullCount())
	}
	arr := got.Chunk(0).(*array.String)
	want := []string{"a", "MISSING", "c"}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %q, want %q", i, arr.Value(i), w)
		}
	}
}

// Polars: fill_null on booleans.
func TestParityFillNullBool(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromBool("b",
		[]bool{true, false, false},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.FillNull(true, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.NullCount() != 0 {
		t.Fatalf("null_count = %d, want 0", got.NullCount())
	}
	arr := got.Chunk(0).(*array.Boolean)
	if arr.Value(0) != true || arr.Value(1) != true || arr.Value(2) != false {
		t.Errorf("= [%v, %v, %v], want [true, true, false]",
			arr.Value(0), arr.Value(1), arr.Value(2))
	}
}

// Polars: test_fill_null_date_with_int_11362 style: passing a value
// of the wrong Go type returns an error rather than silently coercing.
func TestParityFillNullWrongTypeErrors(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"a", ""},
		[]bool{true, false},
		series.WithAllocator(alloc))
	defer s.Release()

	if _, err := s.FillNull(42, series.WithAllocator(alloc)); err == nil {
		t.Errorf("expected error for int fill into string series")
	}
}
