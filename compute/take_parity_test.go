// Behavioural parity with polars' tests/unit/operations/test_gather.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Polars' gather and take are aliases; golars calls the kernel
// compute.Take.
//
// Divergences NOT ported:
//   - Negative indices (`gather([-1])` → last row): polars supports
//     Python-style wraparound, golars returns an error (test below
//     asserts the error).
//   - gather inside group_by aggregation (`pl.col("a").gather(...)`
//     in .agg()): no expression-level gather.
//   - List-level get (`.list.get(i)`)              : no List dtype.
//   - null_on_oob=True parameter                   : strict OOB
//     errors only.

package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: df.select(pl.col("a").gather([0, 2, 4])): take returns
// the rows at the given indices in the given order.
func TestParityTakeBasic(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{10, 20, 30, 40, 50, 60},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	got, err := compute.Take(context.Background(), s, []int{0, 2, 4},
		compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Int64)
	want := []int64{10, 30, 50}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: gather with repeated indices picks the same row twice.
func TestParityTakeRepeatedIndices(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{7, 8, 9}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := compute.Take(context.Background(), s, []int{0, 0, 1, 1, 2, 2},
		compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Int64)
	want := []int64{7, 7, 8, 8, 9, 9}
	if arr.Len() != len(want) {
		t.Fatalf("len = %d, want %d", arr.Len(), len(want))
	}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: gather with out-of-bounds index errors. Polars differentiates
// via null_on_oob=True; golars is strict only.
func TestParityTakeOutOfBoundsErrors(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	if _, err := compute.Take(context.Background(), s, []int{0, 5},
		compute.WithAllocator(alloc)); err == nil {
		t.Errorf("expected error for OOB index")
	}
	if _, err := compute.Take(context.Background(), s, []int{-1},
		compute.WithAllocator(alloc)); err == nil {
		t.Errorf("expected error for negative index (polars allows this)")
	}
}

// Polars: gather preserves null values at the picked indices.
func TestParityTakePreservesNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// [1, null, 3, 4, null]
	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 4, 0},
		[]bool{true, false, true, true, false},
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := compute.Take(context.Background(), s, []int{1, 0, 4, 2},
		compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Int64)
	if arr.IsValid(0) {
		t.Errorf("[0] should be null (picked from index 1)")
	}
	if !arr.IsValid(1) || arr.Value(1) != 1 {
		t.Errorf("[1] = %d (valid=%v), want 1", arr.Value(1), arr.IsValid(1))
	}
	if arr.IsValid(2) {
		t.Errorf("[2] should be null (picked from index 4)")
	}
	if !arr.IsValid(3) || arr.Value(3) != 3 {
		t.Errorf("[3] = %d (valid=%v), want 3", arr.Value(3), arr.IsValid(3))
	}
	if got.NullCount() != 2 {
		t.Errorf("null_count = %d, want 2", got.NullCount())
	}
}

// Polars: gather with empty indices returns an empty Series of the
// same dtype.
func TestParityTakeEmptyIndices(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	got, err := compute.Take(context.Background(), s, []int{},
		compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Len() != 0 {
		t.Errorf("len = %d, want 0", got.Len())
	}
	if got.DType().String() != "i64" {
		t.Errorf("dtype = %s, want i64", got.DType())
	}
}

// Polars: gather works across supported scalar dtypes.
func TestParityTakeDtypes(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	t.Run("float64", func(t *testing.T) {
		s, _ := series.FromFloat64("f", []float64{1.5, 2.5, 3.5, 4.5}, nil,
			series.WithAllocator(alloc))
		defer s.Release()
		got, err := compute.Take(context.Background(), s, []int{3, 1},
			compute.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		arr := got.Chunk(0).(*array.Float64)
		if arr.Value(0) != 4.5 || arr.Value(1) != 2.5 {
			t.Errorf("= [%v, %v], want [4.5, 2.5]", arr.Value(0), arr.Value(1))
		}
	})

	t.Run("string", func(t *testing.T) {
		s, _ := series.FromString("s", []string{"a", "b", "c", "d"}, nil,
			series.WithAllocator(alloc))
		defer s.Release()
		got, err := compute.Take(context.Background(), s, []int{3, 0},
			compute.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		arr := got.Chunk(0).(*array.String)
		if arr.Value(0) != "d" || arr.Value(1) != "a" {
			t.Errorf("= [%q, %q], want [d, a]", arr.Value(0), arr.Value(1))
		}
	})

	t.Run("bool", func(t *testing.T) {
		s, _ := series.FromBool("b", []bool{true, false, true, false}, nil,
			series.WithAllocator(alloc))
		defer s.Release()
		got, err := compute.Take(context.Background(), s, []int{1, 2},
			compute.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		arr := got.Chunk(0).(*array.Boolean)
		if arr.Value(0) != false || arr.Value(1) != true {
			t.Errorf("= [%v, %v], want [false, true]", arr.Value(0), arr.Value(1))
		}
	})
}
