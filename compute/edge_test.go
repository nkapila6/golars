package compute_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// These tests lock down edge cases that the fast paths must continue to
// handle correctly: zero-length inputs, all-null, all-false filters,
// adversarial float values (NaN, Inf, -0.0), boundary integer values,
// empty chain in joins, etc.

func TestAddEmpty(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", nil, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", nil, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	out, err := compute.Add(ctx, a, b, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Add empty: %v", err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Errorf("Len = %d, want 0", out.Len())
	}
}

func TestAddBoundaryInt64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a",
		[]int64{math.MaxInt64, math.MinInt64, 0, -1, 1},
		nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b",
		[]int64{1, -1, 0, 1, -1},
		nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	out, err := compute.Add(ctx, a, b, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	defer out.Release()

	got := out.Chunk(0).(*array.Int64).Int64Values()
	// MaxInt64 + 1 wraps to MinInt64 (Go's silent two's complement wrap).
	if got[0] != math.MinInt64 {
		t.Errorf("max+1 = %d, want MinInt64", got[0])
	}
	if got[1] != math.MaxInt64 {
		t.Errorf("min-1 = %d, want MaxInt64", got[1])
	}
	if got[2] != 0 || got[3] != 0 || got[4] != 0 {
		t.Errorf("rest = %v", got[2:])
	}
}

func TestFilterAllFalse(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("v", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(mem))
	mask, _ := series.FromBool("m",
		[]bool{false, false, false, false, false},
		nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	out, err := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Errorf("Len = %d, want 0", out.Len())
	}
}

func TestFilterAllTrue(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	vals := []int64{10, 20, 30, 40, 50}
	s, _ := series.FromInt64("v", vals, nil, series.WithAllocator(mem))
	mask, _ := series.FromBool("m",
		[]bool{true, true, true, true, true},
		nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	out, _ := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	defer out.Release()

	got := out.Chunk(0).(*array.Int64).Int64Values()
	for i := range vals {
		if got[i] != vals[i] {
			t.Errorf("[%d] = %d, want %d", i, got[i], vals[i])
		}
	}
}

func TestFilterLargeSparse(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// Test the u64-wide fused path at a size that requires both full words
	// and a partial tail.
	const n = 130 // two full u64 words + 2 tail bits
	vals := make([]int64, n)
	mask := make([]bool, n)
	for i := range vals {
		vals[i] = int64(i)
		mask[i] = i%13 == 0
	}
	s, _ := series.FromInt64("v", vals, nil, series.WithAllocator(mem))
	m, _ := series.FromBool("m", mask, nil, series.WithAllocator(mem))
	defer s.Release()
	defer m.Release()

	out, _ := compute.Filter(ctx, s, m, compute.WithAllocator(mem))
	defer out.Release()

	expected := 0
	for _, ok := range mask {
		if ok {
			expected++
		}
	}
	if out.Len() != expected {
		t.Fatalf("Len = %d, want %d", out.Len(), expected)
	}
	got := out.Chunk(0).(*array.Int64).Int64Values()
	idx := 0
	for i, ok := range mask {
		if ok {
			if got[idx] != int64(i) {
				t.Errorf("at idx %d: got %d, want %d", idx, got[idx], i)
			}
			idx++
		}
	}
}

func TestSortBoundaryInt64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("x",
		[]int64{0, math.MinInt64, math.MaxInt64, -1, 1, math.MinInt64 + 1, math.MaxInt64 - 1},
		nil, series.WithAllocator(mem))
	defer s.Release()

	out, _ := compute.Sort(ctx, s, compute.SortOptions{}, compute.WithAllocator(mem))
	defer out.Release()

	got := out.Chunk(0).(*array.Int64).Int64Values()
	want := []int64{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestSortLarge(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	const n = 10000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64((i*2654435761 + 12345) % 1000)
	}
	s, _ := series.FromInt64("x", vals, nil, series.WithAllocator(mem))
	defer s.Release()

	out, _ := compute.Sort(ctx, s, compute.SortOptions{}, compute.WithAllocator(mem))
	defer out.Release()

	got := out.Chunk(0).(*array.Int64).Int64Values()
	for i := 1; i < n; i++ {
		if got[i-1] > got[i] {
			t.Errorf("unsorted at %d: %d > %d", i, got[i-1], got[i])
			return
		}
	}
}

func TestSortDescending(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("x", []int64{3, 1, 4, 1, 5, 9, 2, 6}, nil, series.WithAllocator(mem))
	defer s.Release()

	out, _ := compute.Sort(ctx, s,
		compute.SortOptions{Descending: true}, compute.WithAllocator(mem))
	defer out.Release()

	got := out.Chunk(0).(*array.Int64).Int64Values()
	want := []int64{9, 6, 5, 4, 3, 2, 1, 1}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestSumInt64Overflow(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// Overflow wraps per Go semantics (matches polars).
	s, _ := series.FromInt64("x",
		[]int64{math.MaxInt64, 1, 0},
		nil, series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.SumInt64(ctx, s, compute.WithAllocator(mem))
	// MaxInt64 + 1 = MinInt64 (wraps)
	if got != math.MinInt64 {
		t.Errorf("Sum = %d, want MinInt64 (wrap)", got)
	}
}

func TestSumFloat64NaNPropagation(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromFloat64("x",
		[]float64{1.0, math.NaN(), 3.0},
		nil, series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.SumFloat64(ctx, s, compute.WithAllocator(mem))
	if !math.IsNaN(got) {
		t.Errorf("Sum = %v, want NaN (NaN propagates)", got)
	}
}

func TestDivByZeroFloat(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// Go literal "-0" evaluates to 0.0; to get negative zero we use Copysign.
	negZero := math.Copysign(0, -1)
	a, _ := series.FromFloat64("a", []float64{1, 1, 1}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{0, negZero, math.NaN()}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	out, _ := compute.Div(ctx, a, b, compute.WithAllocator(mem))
	defer out.Release()

	got := out.Chunk(0).(*array.Float64).Float64Values()
	if !math.IsInf(got[0], 1) {
		t.Errorf("1/0 = %v, want +Inf", got[0])
	}
	if !math.IsInf(got[1], -1) {
		t.Errorf("1/-0 = %v, want -Inf", got[1])
	}
	if !math.IsNaN(got[2]) {
		t.Errorf("1/NaN = %v, want NaN", got[2])
	}
}

func TestCastExtremes(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromFloat64("x",
		[]float64{math.Inf(1), math.Inf(-1), math.NaN(), 1e300, -1e300},
		nil, series.WithAllocator(mem))
	defer s.Release()

	out, _ := compute.Cast(ctx, s, dtype.Int64(), compute.WithAllocator(mem))
	defer out.Release()

	// All should become null: out-of-range or NaN.
	if out.NullCount() != 5 {
		t.Errorf("NullCount = %d, want 5", out.NullCount())
	}
}

func TestFilterStringPreservedOrder(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromString("s",
		[]string{"alice", "bob", "carol", "dave", "eve", "frank"},
		nil, series.WithAllocator(mem))
	mask, _ := series.FromBool("m",
		[]bool{true, false, true, false, true, false},
		nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	out, _ := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	defer out.Release()

	got := out.Chunk(0).(*array.String)
	want := []string{"alice", "carol", "eve"}
	for i, w := range want {
		if got.Value(i) != w {
			t.Errorf("[%d] = %q, want %q", i, got.Value(i), w)
		}
	}
}

func TestFilterPreservesNullSource(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// Source with nulls. Fused path should NOT run; fallback keeps null
	// tracking correct.
	s, _ := series.FromInt64("s",
		[]int64{10, 0, 30, 0, 50},
		[]bool{true, false, true, false, true},
		series.WithAllocator(mem))
	mask, _ := series.FromBool("m",
		[]bool{true, true, true, true, true},
		nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	out, _ := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	defer out.Release()
	if out.NullCount() != 2 {
		t.Errorf("NullCount = %d, want 2", out.NullCount())
	}
}
