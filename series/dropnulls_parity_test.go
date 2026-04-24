// Behavioural parity with polars' tests/unit/operations/test_drop_nulls.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported:
//   - DataFrame.drop_nulls(subset=...)       : golars Series.DropNulls
//     is the only surface; callers do DF-level null pruning by filter.
//   - drop_nulls on List / Struct / Object   : no such dtypes.

package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: Series.drop_nulls() on a mixed series removes exactly the
// null positions, preserving order and dtype.
func TestParityDropNullsInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.DropNulls(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Len() != 3 {
		t.Fatalf("len = %d, want 3", got.Len())
	}
	if got.NullCount() != 0 {
		t.Errorf("null_count = %d, want 0", got.NullCount())
	}
	arr := got.Chunk(0).(*array.Int64)
	want := []int64{1, 3, 5}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: drop_nulls on a no-null series is a no-op.
func TestParityDropNullsNoOp(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.DropNulls(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Len() != 3 {
		t.Fatalf("len = %d, want 3", got.Len())
	}
	if got.NullCount() != 0 {
		t.Errorf("null_count = %d, want 0", got.NullCount())
	}
}

// Polars: drop_nulls on an all-null series returns empty.
func TestParityDropNullsAllNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{0, 0, 0},
		[]bool{false, false, false},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := s.DropNulls(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Len() != 0 {
		t.Errorf("len = %d, want 0", got.Len())
	}
}

// Polars: drop_nulls across dtypes.
func TestParityDropNullsDtypes(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	t.Run("float64", func(t *testing.T) {
		s, _ := series.FromFloat64("f",
			[]float64{1.5, 0, 3.5},
			[]bool{true, false, true},
			series.WithAllocator(alloc))
		defer s.Release()
		got, err := s.DropNulls(series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		arr := got.Chunk(0).(*array.Float64)
		if got.Len() != 2 || arr.Value(0) != 1.5 || arr.Value(1) != 3.5 {
			t.Errorf("got = [len=%d, %v, %v]", got.Len(),
				arr.Value(0), arr.Value(1))
		}
	})

	t.Run("string", func(t *testing.T) {
		s, _ := series.FromString("s",
			[]string{"a", "", "c", ""},
			[]bool{true, false, true, false},
			series.WithAllocator(alloc))
		defer s.Release()
		got, err := s.DropNulls(series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		arr := got.Chunk(0).(*array.String)
		if got.Len() != 2 || arr.Value(0) != "a" || arr.Value(1) != "c" {
			t.Errorf("got = [len=%d, %q, %q]", got.Len(),
				arr.Value(0), arr.Value(1))
		}
	})

	t.Run("bool", func(t *testing.T) {
		s, _ := series.FromBool("b",
			[]bool{true, false, true},
			[]bool{true, false, true},
			series.WithAllocator(alloc))
		defer s.Release()
		got, err := s.DropNulls(series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		arr := got.Chunk(0).(*array.Boolean)
		if got.Len() != 2 || arr.Value(0) != true || arr.Value(1) != true {
			t.Errorf("got = [len=%d, %v, %v]", got.Len(),
				arr.Value(0), arr.Value(1))
		}
	})
}

// drop_nulls then len = len - null_count.
func TestParityDropNullsLenProperty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	vals := make([]int64, 100)
	valid := make([]bool, 100)
	for i := range vals {
		vals[i] = int64(i)
		valid[i] = (i%3 != 0) // 34 of 100 are null
	}
	s, _ := series.FromInt64("a", vals, valid, series.WithAllocator(alloc))
	defer s.Release()

	before := s.NullCount()
	got, err := s.DropNulls(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Len() != 100-before {
		t.Errorf("len = %d, want %d", got.Len(), 100-before)
	}
	if got.NullCount() != 0 {
		t.Errorf("null_count = %d, want 0", got.NullCount())
	}
}
