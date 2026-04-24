// Behavioural parity with polars' tests/unit/operations/test_cast.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// golars Cast semantics (mirroring polars' strict=false default):
//   - Integer widening  (i32 → i64) never fails
//   - Integer narrowing (i64 → i32) produces null on overflow
//   - Float → int truncates; out-of-range produces null
//   - Numeric → string uses strconv formatting
//   - String → numeric uses strconv.Parse*; unparseable produces null
//   - Bool ↔ numeric: false=0, true=1
//   - Cast to same dtype returns a clone
//
// Scenarios NOT ported:
//   - Date/Datetime/Duration casts (no temporal dtypes)
//   - Categorical name retention    (no Categorical dtype)
//   - Decimal casts                 (no Decimal dtype)
//   - List/Array inner-type casts   (no List/Array dtypes)
//   - Strict-cast errors             (golars is always lenient)

package compute_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_cast_consistency: casting to the same dtype returns
// an equivalent series (golars returns a clone).
func TestParityCastToSameDtype(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 3 || out.DType().String() != "i64" {
		t.Fatalf("shape/dtype drift: len=%d dtype=%s", out.Len(), out.DType())
	}
	arr := out.Chunk(0).(*array.Int64)
	for i, w := range []int64{1, 2, 3} {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: integer widening is exact.
func TestParityCastInt32ToInt64Widens(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt32("a", []int32{-1, 0, 2147483647}, nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	want := []int64{-1, 0, 2147483647}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: integer narrowing produces null on overflow (non-strict).
// 2^40 doesn't fit in int32 → null.
func TestParityCastInt64ToInt32OverflowProducesNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 1 << 40, -1, math.MaxInt32, math.MinInt32 - 1, 0},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Int32(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Int32)
	// Expected valid positions: 1, MaxInt32, 0. Rows 1, 2(=-1 fits),
	// 4 (below MinInt32): wait, -1 fits in i32. Let me re-spec:
	// Positions:
	//   0: 1                  -> valid, 1
	//   1: 1 << 40            -> overflow, null
	//   2: -1                 -> valid, -1
	//   3: MaxInt32           -> valid, MaxInt32
	//   4: MinInt32 - 1       -> overflow, null
	//   5: 0                  -> valid, 0
	wantValid := []bool{true, false, true, true, false, true}
	wantVals := []int32{1, 0, -1, math.MaxInt32, 0, 0}
	for i := range arr.Len() {
		if arr.IsValid(i) != wantValid[i] {
			t.Fatalf("[%d] valid=%v want %v", i, arr.IsValid(i), wantValid[i])
		}
		if wantValid[i] && arr.Value(i) != wantVals[i] {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), wantVals[i])
		}
	}
}

// Polars: int → float is exact for int32; i64 may lose precision for
// very large magnitudes but exact-size ints round-trip.
func TestParityCastInt64ToFloat64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{-1, 0, 1, 1_000_000}, nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Float64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	want := []float64{-1, 0, 1, 1_000_000}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%v want %v", i, arr.Value(i), w)
		}
	}
}

// Polars: float → int truncates (no rounding).
func TestParityCastFloat64ToInt64Truncates(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		[]float64{1.9, -1.9, 0.5, -0.5, 42.0},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	// Go's int() of a float truncates toward zero.
	want := []int64{1, -1, 0, 0, 42}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: float → int with NaN / ±Inf produces null.
func TestParityCastFloat64SpecialValuesProduceNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		[]float64{1.0, math.NaN(), math.Inf(1), math.Inf(-1), 2.0},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Int64)
	wantValid := []bool{true, false, false, false, true}
	wantVals := []int64{1, 0, 0, 0, 2}
	for i := range arr.Len() {
		if arr.IsValid(i) != wantValid[i] {
			t.Fatalf("[%d] valid=%v want %v", i, arr.IsValid(i), wantValid[i])
		}
		if wantValid[i] && arr.Value(i) != wantVals[i] {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), wantVals[i])
		}
	}
}

// Polars: numeric → string uses default formatting. We don't pin the
// exact format (e.g. whether int formats as "1" vs " 1 "), just that
// round-trip from int → string → int preserves the value.
func TestParityCastInt64ToStringRoundTrips(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{-1, 0, 42, 1_000_000}, nil, series.WithAllocator(alloc))
	defer s.Release()

	asStr, err := compute.Cast(context.Background(), s, dtype.String(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer asStr.Release()
	if asStr.DType().String() != "str" {
		t.Fatalf("dtype=%s want str", asStr.DType())
	}
	backToInt, err := compute.Cast(context.Background(), asStr, dtype.Int64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer backToInt.Release()
	arr := backToInt.Chunk(0).(*array.Int64)
	want := []int64{-1, 0, 42, 1_000_000}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("round-trip [%d]=%d want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: string → int on unparseable values yields null (non-strict).
func TestParityCastStringToInt64InvalidBecomesNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("a",
		[]string{"1", "not-a-number", "42", "", "-7"},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Int64)
	wantValid := []bool{true, false, true, false, true}
	wantVals := []int64{1, 0, 42, 0, -7}
	for i := range arr.Len() {
		if arr.IsValid(i) != wantValid[i] {
			t.Fatalf("[%d] valid=%v want %v (v=%q)", i, arr.IsValid(i), wantValid[i], "")
		}
		if wantValid[i] && arr.Value(i) != wantVals[i] {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), wantVals[i])
		}
	}
}

// Polars: test_bool_numeric_supertype family: bool → numeric is 0/1.
func TestParityCastBoolToInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromBool("a",
		[]bool{true, false, true, true, false},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Int64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	want := []int64{1, 0, 1, 1, 0}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: test_all_null_cast: an all-null column preserves its
// length and yields an all-null result of the target dtype.
func TestParityCastAllNullsPreservesLength(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	vals := make([]int64, 5)
	valid := make([]bool, 5) // zero = all false = all null
	s, _ := series.FromInt64("a", vals, valid, series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Float64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 5 {
		t.Fatalf("len=%d want 5", out.Len())
	}
	if out.NullCount() != 5 {
		t.Fatalf("null count=%d want 5", out.NullCount())
	}
}

// Polars: empty series cast preserves length (0) and switches dtype.
func TestParityCastEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", nil, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := compute.Cast(context.Background(), s, dtype.Float64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Fatalf("len=%d want 0", out.Len())
	}
	if out.DType().String() != "f64" {
		t.Fatalf("dtype=%s want f64", out.DType())
	}
}

// Polars: nulls in the source propagate through the cast.
func TestParityCastPreservesNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	out, err := compute.Cast(context.Background(), s, dtype.Float64(), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.NullCount() != 2 {
		t.Fatalf("null count=%d want 2", out.NullCount())
	}
	arr := out.Chunk(0).(*array.Float64)
	if !arr.IsValid(0) || arr.Value(0) != 1.0 {
		t.Fatal("[0] should be 1.0")
	}
	if !arr.IsValid(2) || arr.Value(2) != 3.0 {
		t.Fatal("[2] should be 3.0")
	}
	if arr.IsValid(1) || arr.IsValid(3) {
		t.Fatal("nulls not preserved")
	}
}
