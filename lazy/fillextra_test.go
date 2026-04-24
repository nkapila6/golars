package lazy_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestLazyFillNan(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a",
		[]float64{1, math.NaN(), 3},
		nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).FillNan(0).Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("a")
	arr := col.Chunk(0).(*array.Float64)
	want := []float64{1, 0, 3}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestLazyForwardFill(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a",
		[]int64{1, 0, 0, 4},
		[]bool{true, false, false, true},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).ForwardFill(0).Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("a")
	arr := col.Chunk(0).(*array.Int64)
	want := []int64{1, 1, 1, 4}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), v)
		}
	}
	if arr.NullN() != 0 {
		t.Fatalf("expected no nulls, got %d", arr.NullN())
	}
}

func TestExprForwardFill(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a",
		[]int64{1, 0, 0, 4},
		[]bool{true, false, false, true},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("a").ForwardFill(0).Alias("ff")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("ff")
	arr := col.Chunk(0).(*array.Int64)
	want := []int64{1, 1, 1, 4}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestExprFillNan(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a",
		[]float64{1, math.NaN(), 3},
		nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		WithColumns(expr.Col("a").FillNan(-1).Alias("a2")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("a2")
	arr := col.Chunk(0).(*array.Float64)
	if arr.Value(1) != -1 {
		t.Fatalf("NaN should be filled to -1, got %v", arr.Value(1))
	}
}
