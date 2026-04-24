package compute_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestAddInt64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", []int64{10, 20, 30, 40}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	got, err := compute.Add(context.Background(), a, b, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	defer got.Release()

	if got.Name() != "a" {
		t.Errorf("name = %q, want a", got.Name())
	}
	if !got.DType().Equal(dtype.Int64()) {
		t.Errorf("dtype = %s, want i64", got.DType())
	}

	assertInt64Values(t, got, []int64{11, 22, 33, 44}, nil)
}

func TestAddFloat64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromFloat64("a", []float64{1.5, 2.5, 3.5}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{0.5, 0.5, 0.5}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	got, err := compute.Add(context.Background(), a, b, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	defer got.Release()

	assertFloat64Values(t, got, []float64{2.0, 3.0, 4.0}, nil)
}

func TestAddWithNullPropagation(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromInt64("a",
		[]int64{1, 2, 3, 4},
		[]bool{true, false, true, true},
		series.WithAllocator(mem))
	b, _ := series.FromInt64("b",
		[]int64{10, 20, 30, 40},
		[]bool{true, true, false, true},
		series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	got, err := compute.Add(context.Background(), a, b, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	defer got.Release()

	if got.NullCount() != 2 {
		t.Errorf("NullCount = %d, want 2", got.NullCount())
	}
	assertInt64Values(t, got, []int64{11, 0, 0, 44}, []bool{true, false, false, true})
}

func TestSubMulDivFloat64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromFloat64("a", []float64{10, 20, 30}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{2, 4, 5}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	sub, _ := compute.Sub(context.Background(), a, b, compute.WithAllocator(mem))
	defer sub.Release()
	assertFloat64Values(t, sub, []float64{8, 16, 25}, nil)

	mul, _ := compute.Mul(context.Background(), a, b, compute.WithAllocator(mem))
	defer mul.Release()
	assertFloat64Values(t, mul, []float64{20, 80, 150}, nil)

	div, _ := compute.Div(context.Background(), a, b, compute.WithAllocator(mem))
	defer div.Release()
	assertFloat64Values(t, div, []float64{5, 5, 6}, nil)
}

func TestDivIntByZeroEmitsNull(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromInt64("a", []int64{10, 20, 30, 40}, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", []int64{2, 0, 3, 0}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	got, err := compute.Div(context.Background(), a, b, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Div: %v", err)
	}
	defer got.Release()

	if got.NullCount() != 2 {
		t.Errorf("NullCount = %d, want 2", got.NullCount())
	}
	assertInt64Values(t, got, []int64{5, 0, 10, 0}, []bool{true, false, true, false})
}

func TestDivFloatByZeroIsInf(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromFloat64("a", []float64{1, 1}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{0, -0}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	got, _ := compute.Div(context.Background(), a, b, compute.WithAllocator(mem))
	defer got.Release()

	vals := float64Values(got)
	if !math.IsInf(vals[0], 1) && !math.IsInf(vals[0], -1) {
		t.Errorf("expected Inf, got %v", vals[0])
	}
}

func TestAddDTypeMismatch(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromInt64("a", []int64{1}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{1}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	_, err := compute.Add(context.Background(), a, b, compute.WithAllocator(mem))
	if !errors.Is(err, compute.ErrDTypeMismatch) {
		t.Errorf("expected ErrDTypeMismatch, got %v", err)
	}
}

func TestAddLengthMismatch(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", []int64{1}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	_, err := compute.Add(context.Background(), a, b, compute.WithAllocator(mem))
	if !errors.Is(err, compute.ErrLengthMismatch) {
		t.Errorf("expected ErrLengthMismatch, got %v", err)
	}
}

func TestAddUnsupportedDType(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromString("a", []string{"x"}, nil, series.WithAllocator(mem))
	b, _ := series.FromString("b", []string{"y"}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	_, err := compute.Add(context.Background(), a, b, compute.WithAllocator(mem))
	if !errors.Is(err, compute.ErrUnsupportedDType) {
		t.Errorf("expected ErrUnsupportedDType, got %v", err)
	}
}

func TestAddWithNameOption(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	a, _ := series.FromInt64("a", []int64{1}, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", []int64{2}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	got, _ := compute.Add(context.Background(), a, b,
		compute.WithAllocator(mem), compute.WithName("sum"))
	defer got.Release()
	if got.Name() != "sum" {
		t.Errorf("name = %q, want sum", got.Name())
	}
}

func TestAddLargeParallel(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	const n = 1 << 17 // triggers parallel path
	av := make([]int64, n)
	bv := make([]int64, n)
	want := make([]int64, n)
	for i := range av {
		av[i] = int64(i)
		bv[i] = int64(2 * i)
		want[i] = av[i] + bv[i]
	}
	a, _ := series.FromInt64("a", av, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", bv, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	got, err := compute.Add(context.Background(), a, b, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	defer got.Release()

	assertInt64Values(t, got, want, nil)
}

// Benchmarks ---------------------------------------------------------------

func BenchmarkAddInt64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1024, 1 << 14, 1 << 18, 1 << 22} {
		b.Run(benchSize(n), func(b *testing.B) {
			av := make([]int64, n)
			bv := make([]int64, n)
			for i := range av {
				av[i] = int64(i)
				bv[i] = int64(i * 2)
			}
			sa, _ := series.FromInt64("a", av, nil)
			sb, _ := series.FromInt64("b", bv, nil)
			defer sa.Release()
			defer sb.Release()

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 8)
			for b.Loop() {
				out, err := compute.Add(ctx, sa, sb)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

func BenchmarkAddFloat64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1024, 1 << 14, 1 << 18, 1 << 22} {
		b.Run(benchSize(n), func(b *testing.B) {
			av := make([]float64, n)
			bv := make([]float64, n)
			for i := range av {
				av[i] = float64(i)
				bv[i] = float64(i) * 0.5
			}
			sa, _ := series.FromFloat64("a", av, nil)
			sb, _ := series.FromFloat64("b", bv, nil)
			defer sa.Release()
			defer sb.Release()

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 8)
			for b.Loop() {
				out, err := compute.Add(ctx, sa, sb)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}
