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

func TestCastIdentity(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	defer s.Release()
	out, err := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()
	assertInt64Values(t, out, []int64{1, 2, 3}, nil)
}

func TestCastIntToFloat(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	defer s.Release()
	out, _ := compute.Cast(context.Background(), s, dtype.Float64(), compute.WithAllocator(mem))
	defer out.Release()
	assertFloat64Values(t, out, []float64{1, 2, 3}, nil)
}

func TestCastFloatToIntTruncates(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromFloat64("x", []float64{1.9, -1.9, 2.0, math.NaN()}, nil, series.WithAllocator(mem))
	defer s.Release()
	out, _ := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(mem))
	defer out.Release()
	// Expected: 1, -1, 2, null (NaN -> null).
	assertInt64Values(t, out, []int64{1, -1, 2, 0}, []bool{true, true, true, false})
}

func TestCastIntNarrowingOverflows(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x", []int64{1, 1 << 31, -(1 << 31)}, nil, series.WithAllocator(mem))
	defer s.Release()
	out, _ := compute.Cast(context.Background(), s, dtype.Int32(), compute.WithAllocator(mem))
	defer out.Release()
	// 1<<31 overflows int32; 1 and -(1<<31) fit.
	got := out.Chunk(0).(*array.Int32).Int32Values()
	if got[0] != 1 {
		t.Errorf("[0] = %d, want 1", got[0])
	}
	if out.NullCount() != 1 {
		t.Errorf("NullCount = %d, want 1", out.NullCount())
	}
	if !out.Chunk(0).IsNull(1) {
		t.Error("[1] should be null (overflow)")
	}
	if got[2] != -(1 << 31) {
		t.Errorf("[2] = %d, want %d", got[2], -(1 << 31))
	}
}

func TestCastStringToInt(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromString("x",
		[]string{"1", "42", "bad", "-7", "3.14"},
		nil, series.WithAllocator(mem))
	defer s.Release()
	out, _ := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(mem))
	defer out.Release()
	// "bad" and "3.14" are not valid int64; expect nulls.
	assertInt64Values(t, out,
		[]int64{1, 42, 0, -7, 0},
		[]bool{true, true, false, true, false})
}

func TestCastStringToFloat(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromString("x",
		[]string{"1.5", "2", "nope", "-3.14"},
		nil, series.WithAllocator(mem))
	defer s.Release()
	out, _ := compute.Cast(context.Background(), s, dtype.Float64(), compute.WithAllocator(mem))
	defer out.Release()
	assertFloat64Values(t, out,
		[]float64{1.5, 2, 0, -3.14},
		[]bool{true, true, false, true})
}

func TestCastIntToString(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x", []int64{1, 42, -7}, nil, series.WithAllocator(mem))
	defer s.Release()
	out, _ := compute.Cast(context.Background(), s, dtype.String(), compute.WithAllocator(mem))
	defer out.Release()
	want := []string{"1", "42", "-7"}
	got := out.Chunk(0).(*array.String)
	for i, w := range want {
		if g := got.Value(i); g != w {
			t.Errorf("[%d] = %q, want %q", i, g, w)
		}
	}
}

func TestCastBoolRoundTrip(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromBool("x", []bool{true, false, true}, nil, series.WithAllocator(mem))
	defer s.Release()
	i64, _ := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(mem))
	defer i64.Release()
	assertInt64Values(t, i64, []int64{1, 0, 1}, nil)

	back, _ := compute.Cast(context.Background(), i64, dtype.Bool(), compute.WithAllocator(mem))
	defer back.Release()
	assertBoolValues(t, back, []bool{true, false, true}, nil)
}

func TestCastPreservesNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x",
		[]int64{10, 0, 30},
		[]bool{true, false, true},
		series.WithAllocator(mem))
	defer s.Release()
	out, _ := compute.Cast(context.Background(), s, dtype.Float64(), compute.WithAllocator(mem))
	defer out.Release()
	if out.NullCount() != 1 {
		t.Errorf("NullCount = %d, want 1", out.NullCount())
	}
	assertFloat64Values(t, out, []float64{10, 0, 30}, []bool{true, false, true})
}

func BenchmarkCastInt64ToFloat64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 14, 1 << 18, 1 << 22} {
		b.Run(benchSize(n), func(b *testing.B) {
			vals := make([]int64, n)
			for i := range vals {
				vals[i] = int64(i)
			}
			s, _ := series.FromInt64("x", vals, nil)
			defer s.Release()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 8)
			for b.Loop() {
				out, err := compute.Cast(ctx, s, dtype.Float64())
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

func BenchmarkCastStringToInt64(b *testing.B) {
	ctx := context.Background()
	const n = 1 << 16
	vals := make([]string, n)
	for i := range vals {
		if i%7 == 0 {
			vals[i] = "bad"
		} else {
			vals[i] = "12345"
		}
	}
	s, _ := series.FromString("x", vals, nil)
	defer s.Release()
	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	for b.Loop() {
		out, err := compute.Cast(ctx, s, dtype.Int64())
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
