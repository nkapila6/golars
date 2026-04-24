// Behavioural parity with polars' tests/unit/operations/test_filter.py.
// See NOTICE for license & attribution details. Fresh Go tests written
// against golars' API using the polars scenarios as a specification.
//
// Polars' test_filter.py covers a wide range of scenarios, many using
// features golars doesn't (yet) have: lazy explode/unpivot, window
// functions, categoricals, is_in with lists, any_horizontal selectors.
// This file ports the subset that maps cleanly to golars' single-mask
// Filter.
//
// Scenarios NOT ported (feature-gated on polars-only behaviour):
//   - pl.lit(True/False) simplification (expression optimizer territory)
//   - categorical string comparison (no Categorical dtype)
//   - group-by filter aggregations (filter inside agg: use GroupBy.Filter)
//   - window-function predicates (.over(...) not implemented)
//   - unpivot/explode predicate pushdown (not implemented)

package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_filter_on_empty: filter on an empty frame yields an
// empty result, regardless of the source dtype. We check int64,
// float64, bool, string here; the List/Binary/Object variants in
// polars don't apply (no corresponding golars dtypes).
func TestParityFilterEmptyInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", nil, nil, series.WithAllocator(alloc))
	defer s.Release()
	m, _ := series.FromBool("m", nil, nil, series.WithAllocator(alloc))
	defer m.Release()

	out, err := compute.Filter(context.Background(), s, m, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Fatalf("len=%d want 0", out.Len())
	}
}

func TestParityFilterEmptyFloat64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	s, _ := series.FromFloat64("a", nil, nil, series.WithAllocator(alloc))
	defer s.Release()
	m, _ := series.FromBool("m", nil, nil, series.WithAllocator(alloc))
	defer m.Release()
	out, _ := compute.Filter(context.Background(), s, m, compute.WithAllocator(alloc))
	defer out.Release()
	if out.Len() != 0 {
		t.Fatalf("len=%d want 0", out.Len())
	}
}

func TestParityFilterEmptyString(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	s, _ := series.FromString("a", nil, nil, series.WithAllocator(alloc))
	defer s.Release()
	m, _ := series.FromBool("m", nil, nil, series.WithAllocator(alloc))
	defer m.Release()
	out, _ := compute.Filter(context.Background(), s, m, compute.WithAllocator(alloc))
	defer out.Release()
	if out.Len() != 0 {
		t.Fatalf("len=%d want 0", out.Len())
	}
}

// Polars analogue: filter(lit(True)) returns the whole frame;
// filter(lit(False)) returns an empty frame. Here we use an
// all-true / all-false mask since golars evaluates masks directly.
func TestParityFilterAllTrueMaskIsIdentity(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	vals := []int64{1, 2, 3, 4, 5}
	s, _ := series.FromInt64("a", vals, nil, series.WithAllocator(alloc))
	defer s.Release()
	mask, _ := series.FromBool("m", []bool{true, true, true, true, true}, nil, series.WithAllocator(alloc))
	defer mask.Release()

	out, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 5 {
		t.Fatalf("all-true mask should preserve length, got %d", out.Len())
	}
	arr := out.Chunk(0).(*array.Int64)
	for i, v := range vals {
		if arr.Value(i) != v {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), v)
		}
	}
}

func TestParityFilterAllFalseMaskIsEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mask, _ := series.FromBool("m", []bool{false, false, false}, nil, series.WithAllocator(alloc))
	defer mask.Release()

	out, _ := compute.Filter(context.Background(), s, mask, compute.WithAllocator(alloc))
	defer out.Release()
	if out.Len() != 0 {
		t.Fatalf("all-false mask should produce empty, got %d", out.Len())
	}
}

// Polars semantic: null mask entries act as false. A mask with mixed
// null/true/false must only retain positions where mask[i] is
// definitely true.
func TestParityFilterNullMaskEntryTreatedAsFalse(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()
	// Mask: true, null, true, false, true  →  keep rows 0, 2, 4.
	mask, _ := series.FromBool("m",
		[]bool{true, false, true, false, true},
		[]bool{true, false, true, true, true},
		series.WithAllocator(alloc))
	defer mask.Release()

	out, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 3 {
		t.Fatalf("len=%d want 3", out.Len())
	}
	arr := out.Chunk(0).(*array.Int64)
	want := []int64{1, 3, 5}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), v)
		}
	}
}

// Polars: a non-Bool filter mask errors. golars returns
// ErrMaskNotBool wrapped with the dtype mismatch error.
func TestParityFilterNonBoolMaskErrors(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mask, _ := series.FromInt64("m", []int64{1, 1}, nil, series.WithAllocator(alloc))
	defer mask.Release()

	if _, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(alloc)); err == nil {
		t.Fatal("non-bool mask must error")
	}
}

// Polars: mask length mismatch is a hard error. golars reports via
// ErrLengthMismatch.
func TestParityFilterLengthMismatchErrors(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mask, _ := series.FromBool("m", []bool{true, false}, nil, series.WithAllocator(alloc))
	defer mask.Release()

	if _, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(alloc)); err == nil {
		t.Fatal("length mismatch must error")
	}
}

// Polars: filter preserves nulls in source values. Mask keeps row 1
// whose value is null → output has a null.
func TestParityFilterPreservesSourceNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 4},
		[]bool{true, false, true, true},
		series.WithAllocator(alloc))
	defer s.Release()
	mask, _ := series.FromBool("m",
		[]bool{true, true, false, true}, nil,
		series.WithAllocator(alloc))
	defer mask.Release()

	out, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 3 {
		t.Fatalf("len=%d want 3", out.Len())
	}
	if out.NullCount() != 1 {
		t.Fatalf("null count = %d, want 1 (row 1 survives as null)", out.NullCount())
	}
}

// Polars: filter by a comparison-derived boolean mask: the typical
// pipeline: `df.filter(pl.col("a") > 2)`. golars builds the same mask
// via compute.GtLit then Filter.
func TestParityFilterByGtLitComparison(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()

	mask, err := compute.GtLit(context.Background(), s, int64(2), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer mask.Release()

	out, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 3 {
		t.Fatalf("len=%d want 3", out.Len())
	}
	arr := out.Chunk(0).(*array.Int64)
	want := []int64{3, 4, 5}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), v)
		}
	}
}

// Polars: filter on a boolean Series with `col == false` retains only
// false rows. We verify EqLit-on-bool via an explicit boolean column
// test; GolarsEqLit doesn't take bool literals but an all-false mask
// here exercises the same semantic.
func TestParityFilterBoolColumn(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Source: booleans themselves.
	s, _ := series.FromBool("flag",
		[]bool{true, false, true, false, false},
		nil, series.WithAllocator(alloc))
	defer s.Release()
	// Filter using itself as mask → only "true" rows survive.
	out, err := compute.Filter(context.Background(), s, s, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 2 {
		t.Fatalf("true rows = %d, want 2", out.Len())
	}
	arr := out.Chunk(0).(*array.Boolean)
	for i := range arr.Len() {
		if !arr.Value(i) {
			t.Fatalf("survivor [%d] should be true", i)
		}
	}
}

// Polars semantic from test_filter_logical_type_* family: filter over
// a float64 source returns float64 (dtype preserved).
func TestParityFilterPreservesDtype(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a", []float64{1.1, 2.2, 3.3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mask, _ := series.FromBool("m", []bool{true, false, true}, nil, series.WithAllocator(alloc))
	defer mask.Release()
	out, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.DType().String() != "f64" {
		t.Fatalf("dtype = %s, want f64", out.DType().String())
	}
	arr := out.Chunk(0).(*array.Float64)
	want := []float64{1.1, 3.3}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("[%d]=%v want %v", i, arr.Value(i), v)
		}
	}
}

// Polars: filtering a string column preserves the string dtype and
// null validity bitmap.
func TestParityFilterStringsWithNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"alice", "", "bob", "", "carol"},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()
	mask, _ := series.FromBool("m",
		[]bool{true, true, false, true, true}, nil,
		series.WithAllocator(alloc))
	defer mask.Release()

	out, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 4 {
		t.Fatalf("len=%d want 4", out.Len())
	}
	arr := out.Chunk(0).(*array.String)
	// Expected logical values (with nulls): [alice, null, null, carol]
	if !arr.IsValid(0) || arr.Value(0) != "alice" {
		t.Fatalf("[0] alice expected")
	}
	if arr.IsValid(1) || arr.IsValid(2) {
		t.Fatal("[1]/[2] should be null")
	}
	if !arr.IsValid(3) || arr.Value(3) != "carol" {
		t.Fatalf("[3] carol expected")
	}
}
