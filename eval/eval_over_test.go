package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestOverSumByGroup(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromString("k", []string{"a", "a", "b", "b", "a"}, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{1, 2, 10, 20, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("v").Sum().Over("k").Alias("vs")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("vs")
	arr := col.Chunk(0).(*array.Int64)
	// a: 1+2+3=6; b: 10+20=30.
	want := []int64{6, 6, 30, 30, 6}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestOverCumSumByGroup(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromString("k", []string{"a", "a", "b", "a", "b"}, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{1, 2, 10, 3, 20}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("v").CumSum().Over("k").Alias("cum")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("cum")
	arr := col.Chunk(0).(*array.Float64)
	// Per-group cumsum broadcast back to original row order:
	// a: 1, 3, ?, 6, ?  → rows 0,1,3 = 1,3,6
	// b: 10, ?, 30     → rows 2,4 = 10,30
	// Combined by position: [1, 3, 10, 6, 30]
	want := []float64{1, 3, 10, 6, 30}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}
