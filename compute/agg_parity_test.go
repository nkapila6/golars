// Behavioural parity with polars' tests/unit/operations/aggregation/test_aggregations.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Coverage focuses on scalar aggregation kernels we expose:
//   SumInt64 / SumFloat64 / MeanFloat64 / MinInt64 / MaxInt64 /
//   MinFloat64 / MaxFloat64 / Count / NullCount. Returns follow the
//   polars contract: `(value, valid bool, err)` where valid=false
//   indicates "no contributing rows" (empty or fully null input).
//
// Scenarios NOT ported (feature-gated on polars-only behaviour):
//   - Quantile / Median       (no Quantile kernel yet)
//   - std / var               (no online variance kernel yet)
//   - Horizontal sum          (no horizontal fold)
//   - Duration / Datetime agg (no temporal dtypes)
//   - implode                 (no List dtype)

package compute_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: sum over an empty series returns 0 (polars convention for
// the additive identity; `is_empty() ? 0 : total`). golars'
// SumInt64/SumFloat64 return 0 without error.
func TestParityAggSumEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", nil, nil, series.WithAllocator(alloc))
	defer s.Release()
	got, err := compute.SumInt64(context.Background(), s, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("sum(empty) = %d, want 0", got)
	}
}

// Polars: sum of an all-null series is 0 (same additive identity).
func TestParityAggSumAllNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		make([]int64, 5), make([]bool, 5), series.WithAllocator(alloc))
	defer s.Release()
	got, _ := compute.SumInt64(context.Background(), s, compute.WithAllocator(alloc))
	if got != 0 {
		t.Fatalf("sum(all-null) = %d, want 0", got)
	}
}

// Polars: mean on empty or all-null returns (NaN, false). The bool
// return is the polars "did we have data" indicator.
func TestParityAggMeanEmptyIsInvalid(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a", nil, nil, series.WithAllocator(alloc))
	defer s.Release()
	_, valid, err := compute.MeanFloat64(context.Background(), s, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	if valid {
		t.Fatal("empty mean should be invalid (valid=false)")
	}
}

func TestParityAggMeanAllNullIsInvalid(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		make([]float64, 4), make([]bool, 4), series.WithAllocator(alloc))
	defer s.Release()
	_, valid, _ := compute.MeanFloat64(context.Background(), s, compute.WithAllocator(alloc))
	if valid {
		t.Fatal("all-null mean should be invalid")
	}
}

// Polars: test_mean_null_simd: mean over a series with interleaved
// nulls ignores the null positions. Specifically, mean([1, null, 3])
// is 2.0 (sum 4 / count 2), not 1.333... (sum/len).
func TestParityAggMeanIgnoresNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		[]float64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	m, valid, err := compute.MeanFloat64(context.Background(), s, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	if !valid {
		t.Fatal("mean should be valid")
	}
	// (1 + 3 + 5) / 3 = 3.0, NOT 9/5 = 1.8
	if m != 3.0 {
		t.Fatalf("mean=%v want 3.0 (nulls must be dropped, not treated as 0)", m)
	}
}

// Polars: test_mean_overflow: large positive integers are summed
// into an int64 safely when their total still fits; MeanFloat64
// promotes to f64 which handles the conversion exactly for the
// magnitudes in polars' own test.
func TestParityAggMeanSmallIntPromotion(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{100, 200, 300, 400, 500},
		nil, series.WithAllocator(alloc))
	defer s.Release()
	got, valid, err := compute.MeanFloat64(context.Background(), s, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	if !valid || got != 300.0 {
		t.Fatalf("mean=%v valid=%v want 300.0 true", got, valid)
	}
}

// Polars: test_nan_inf_aggregation: Min of a series containing NaN
// either returns a real value (NaN filtered) or returns NaN; polars'
// default is "NaN sorts last" so Min is the smallest real value. We
// mirror that contract: MinFloat64 returns a non-NaN if any exists.
func TestParityAggMinSkipsNaN(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		[]float64{math.NaN(), 2.0, 1.0, math.NaN(), 3.0},
		nil, series.WithAllocator(alloc))
	defer s.Release()
	got, valid, err := compute.MinFloat64(context.Background(), s, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	if !valid {
		t.Fatal("Min should be valid")
	}
	if got != 1.0 {
		t.Fatalf("Min=%v want 1.0 (real min, NaN filtered)", got)
	}
}

// Polars: Max respects +Inf as a valid maximum (no NaN interference).
func TestParityAggMaxWithInf(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		[]float64{1, 2, math.Inf(1), 3},
		nil, series.WithAllocator(alloc))
	defer s.Release()
	got, valid, err := compute.MaxFloat64(context.Background(), s, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	if !valid || !math.IsInf(got, 1) {
		t.Fatalf("Max=%v valid=%v want +Inf", got, valid)
	}
}

// Polars: Count ignores nulls, Len counts every row. This is the
// polars `count` / `len` distinction. golars has compute.Count(s) for
// the non-null count and series.Len() for physical length.
func TestParityAggCountVsLen(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()
	if got := compute.Count(s); got != 3 {
		t.Fatalf("Count=%d want 3 (non-null)", got)
	}
	if got := s.Len(); got != 5 {
		t.Fatalf("Len=%d want 5 (physical)", got)
	}
	if got := compute.NullCount(s); got != 2 {
		t.Fatalf("NullCount=%d want 2", got)
	}
}

// Polars: single-element aggregation: value aggregates to itself.
func TestParityAggSingleElement(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a", []float64{42.5}, nil, series.WithAllocator(alloc))
	defer s.Release()

	sum, _ := compute.SumFloat64(context.Background(), s, compute.WithAllocator(alloc))
	if sum != 42.5 {
		t.Fatalf("sum=%v", sum)
	}
	mean, valid, _ := compute.MeanFloat64(context.Background(), s, compute.WithAllocator(alloc))
	if !valid || mean != 42.5 {
		t.Fatalf("mean=%v valid=%v", mean, valid)
	}
	mn, _, _ := compute.MinFloat64(context.Background(), s, compute.WithAllocator(alloc))
	mx, _, _ := compute.MaxFloat64(context.Background(), s, compute.WithAllocator(alloc))
	if mn != 42.5 || mx != 42.5 {
		t.Fatalf("min=%v max=%v", mn, mx)
	}
}

// Polars: sum of integers crossing the signed boundary (within
// int64) is exact: no overflow wrap at the kernel level for sums
// that fit.
func TestParityAggSumInt64Large(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	vals := make([]int64, 1024)
	const elem int64 = 1 << 30 // 2^30; total 2^40 fits easily in i64
	for i := range vals {
		vals[i] = elem
	}
	s, _ := series.FromInt64("a", vals, nil, series.WithAllocator(alloc))
	defer s.Release()
	got, err := compute.SumInt64(context.Background(), s, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	want := elem * int64(len(vals))
	if got != want {
		t.Fatalf("sum=%d want %d", got, want)
	}
}

// Polars: test_boolean_aggs: booleans summed via cast to int yield
// the count of true. golars doesn't expose SumBool directly, but
// Count + casting via compute.Cast to Int64 + SumInt64 gives the
// same number for a dense bool column.
func TestParityAggBooleanCountOfTrues(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromBool("a",
		[]bool{true, false, true, true, false, true},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	// Cast to int64 then sum. Polars' `pl.col(b).sum()` on a bool
	// column does the same internally.
	asI, err := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer asI.Release()

	got, err := compute.SumInt64(context.Background(), asI, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	if got != 4 {
		t.Fatalf("sum(bool→int) = %d, want 4 (four trues)", got)
	}
}
