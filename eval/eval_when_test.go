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

func TestWhenThenOtherwise(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.When(expr.Col("a").Gt(expr.Lit(int64(2)))).
			Then(expr.Lit(int64(99))).
			Otherwise(expr.Col("a")).
			Alias("r")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("r")
	arr := col.Chunk(0).(*array.Int64)
	want := []int64{1, 2, 99, 99, 99}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %d want %d", i, arr.Value(i), v)
		}
	}
}

func TestWhenThenPromoteMixedDtype(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	// Then branch is int literal, Otherwise is float literal; result
	// should be promoted to float64.
	out, err := lazy.FromDataFrame(df).
		Select(expr.When(expr.Col("a").Gt(expr.Lit(int64(1)))).
			Then(expr.Lit(int64(1))).
			Otherwise(expr.Lit(0.5)).
			Alias("r")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("r")
	arr := col.Chunk(0).(*array.Float64)
	want := []float64{0.5, 1, 1}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}
