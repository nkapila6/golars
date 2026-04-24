package compute_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestFilterInt64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("x", []int64{10, 20, 30, 40, 50}, nil, series.WithAllocator(mem))
	mask, _ := series.FromBool("m",
		[]bool{true, false, true, false, true},
		nil,
		series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	got, err := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer got.Release()

	assertInt64Values(t, got, []int64{10, 30, 50}, nil)
}

func TestFilterNullMaskTreatedFalse(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("x", []int64{1, 2, 3, 4}, nil, series.WithAllocator(mem))
	mask, _ := series.FromBool("m",
		[]bool{true, true, true, false},
		[]bool{true, false, true, true},
		series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	got, _ := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	defer got.Release()
	assertInt64Values(t, got, []int64{1, 3}, nil)
}

func TestFilterPreservesNullsInSource(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("x",
		[]int64{10, 0, 30, 0},
		[]bool{true, false, true, false},
		series.WithAllocator(mem))
	mask, _ := series.FromBool("m",
		[]bool{true, true, false, true},
		nil,
		series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	got, _ := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	defer got.Release()

	if got.Len() != 3 {
		t.Errorf("Len = %d, want 3", got.Len())
	}
	if got.NullCount() != 2 {
		t.Errorf("NullCount = %d, want 2", got.NullCount())
	}
	assertInt64Values(t, got, []int64{10, 0, 0}, []bool{true, false, false})
}

func TestFilterString(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromString("x",
		[]string{"a", "b", "c", "d"},
		nil,
		series.WithAllocator(mem))
	mask, _ := series.FromBool("m",
		[]bool{false, true, true, false},
		nil,
		series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	got, _ := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	defer got.Release()

	if got.Len() != 2 {
		t.Fatalf("Len = %d, want 2", got.Len())
	}
	got0 := stringValuesAt(got)
	if got0[0] != "b" || got0[1] != "c" {
		t.Errorf("values = %v, want [b c]", got0)
	}
}

func TestFilterBool(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromBool("x", []bool{true, false, true, false}, nil, series.WithAllocator(mem))
	mask, _ := series.FromBool("m", []bool{true, true, false, true}, nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	got, _ := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	defer got.Release()
	assertBoolValues(t, got, []bool{true, false, false}, nil)
}

func TestFilterLengthMismatch(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("x", []int64{1, 2}, nil, series.WithAllocator(mem))
	mask, _ := series.FromBool("m", []bool{true}, nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	_, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(mem))
	if !errors.Is(err, compute.ErrLengthMismatch) {
		t.Errorf("expected ErrLengthMismatch, got %v", err)
	}
}

func TestFilterMaskNotBool(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("x", []int64{1, 2}, nil, series.WithAllocator(mem))
	mask, _ := series.FromInt64("m", []int64{1, 0}, nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	_, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(mem))
	if !errors.Is(err, compute.ErrMaskNotBool) {
		t.Errorf("expected ErrMaskNotBool, got %v", err)
	}
}

func TestTake(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("x", []int64{10, 20, 30, 40, 50}, nil, series.WithAllocator(mem))
	defer s.Release()

	got, err := compute.Take(context.Background(), s, []int{4, 2, 0}, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	defer got.Release()

	assertInt64Values(t, got, []int64{50, 30, 10}, nil)
}

func TestTakeOutOfRange(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	defer s.Release()

	if _, err := compute.Take(context.Background(), s, []int{3}, compute.WithAllocator(mem)); err == nil {
		t.Error("expected out-of-range error")
	}
	if _, err := compute.Take(context.Background(), s, []int{-1}, compute.WithAllocator(mem)); err == nil {
		t.Error("expected out-of-range error")
	}
}

func BenchmarkFilterInt64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1024, 1 << 14, 1 << 18, 1 << 22} {
		b.Run(benchSize(n), func(b *testing.B) {
			vals := make([]int64, n)
			mask := make([]bool, n)
			for i := range vals {
				vals[i] = int64(i)
				mask[i] = i%3 == 0
			}
			s, _ := series.FromInt64("x", vals, nil)
			m, _ := series.FromBool("m", mask, nil)
			defer s.Release()
			defer m.Release()

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 8)
			for b.Loop() {
				out, err := compute.Filter(ctx, s, m)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}
