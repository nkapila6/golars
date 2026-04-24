// Behavioural parity with polars' tests/unit/dataframe/test_describe.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported:
//   - Non-numeric Describe row semantics : polars emits strings like
//     "2"/"1" for count/null_count plus min/max across strings and
//     dates. golars emits numeric stats only for numeric columns and
//     count/null_count rows for non-numerics.
//   - Custom percentiles (percentiles=...): golars' Describe uses
//     fixed 25/50/75 quartiles.
//   - Duration / Date / Time / Categorical dtypes: unsupported.
//   - Describe on an empty-schema DataFrame: golars constructs New()
//     with zero columns from a shared statistic column only.
//   - LazyFrame.describe: not implemented; polars' describe lowers
//     through collect internally.

package dataframe_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_df_describe numeric-column projection.
// Each statistic row lines up across columns; values match a hand-
// computed reference for the numeric column.
func TestParityDescribeNumeric(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a", []float64{1.0, 2.8, 3.0}, nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	got, err := df.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	// Row layout: count, null_count, mean, std, min, 25%, 50%, 75%, max.
	if got.Height() != 9 {
		t.Fatalf("height = %d, want 9", got.Height())
	}
	stat, _ := got.Column("statistic")
	statArr := stat.Chunk(0).(*array.String)
	wantStat := []string{"count", "null_count", "mean", "std", "min", "25%", "50%", "75%", "max"}
	for i, w := range wantStat {
		if statArr.Value(i) != w {
			t.Errorf("statistic[%d] = %q, want %q", i, statArr.Value(i), w)
		}
	}

	col, _ := got.Column("a")
	arr := col.Chunk(0).(*array.Float64)

	// count / null_count
	if arr.Value(0) != 3 || arr.Value(1) != 0 {
		t.Errorf("count,null_count = %v,%v, want 3,0",
			arr.Value(0), arr.Value(1))
	}
	// mean = (1.0 + 2.8 + 3.0) / 3 = 2.2666...
	if got, want := arr.Value(2), 2.2666666666666666; math.Abs(got-want) > 1e-12 {
		t.Errorf("mean = %v, want %v", got, want)
	}
	// min / max
	if arr.Value(4) != 1.0 {
		t.Errorf("min = %v, want 1.0", arr.Value(4))
	}
	if arr.Value(8) != 3.0 {
		t.Errorf("max = %v, want 3.0", arr.Value(8))
	}
}

// Polars: describe with nulls: count reflects non-null count;
// null_count reports nulls; numeric stats exclude nulls.
func TestParityDescribeHandlesNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	b, _ := series.FromInt64("b",
		[]int64{4, 5, 0},
		[]bool{true, true, false},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(b)
	defer df.Release()

	got, err := df.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	col, _ := got.Column("b")
	arr := col.Chunk(0).(*array.Float64)

	if arr.Value(0) != 2 {
		t.Errorf("count = %v, want 2", arr.Value(0))
	}
	if arr.Value(1) != 1 {
		t.Errorf("null_count = %v, want 1", arr.Value(1))
	}
	if arr.Value(2) != 4.5 {
		t.Errorf("mean = %v, want 4.5", arr.Value(2))
	}
	if arr.Value(4) != 4 {
		t.Errorf("min = %v, want 4", arr.Value(4))
	}
	if arr.Value(8) != 5 {
		t.Errorf("max = %v, want 5", arr.Value(8))
	}
}

// Single-value series: std is undefined (polars: null); mean == min == max.
func TestParityDescribeSingleValue(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a", []float64{42.0}, nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	got, err := df.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	col, _ := got.Column("a")
	arr := col.Chunk(0).(*array.Float64)

	if arr.IsValid(3) {
		t.Errorf("std should be null for single-value series, got %v", arr.Value(3))
	}
	if arr.Value(2) != 42.0 || arr.Value(4) != 42.0 || arr.Value(8) != 42.0 {
		t.Errorf("mean=%v min=%v max=%v, all want 42",
			arr.Value(2), arr.Value(4), arr.Value(8))
	}
}

// Polars: describe preserves the "statistic" row name column.
func TestParityDescribeStatisticColumnPresent(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("x", []int64{1, 2, 3}, nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	got, err := df.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if _, err := got.Column("statistic"); err != nil {
		t.Errorf("missing 'statistic' column: %v", err)
	}
	if _, err := got.Column("x"); err != nil {
		t.Errorf("missing 'x' column: %v", err)
	}
}
