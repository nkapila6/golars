package lazy_test

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

func TestLazyReverseAndTail(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{1, 2, 3, 4, 5}, nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(s)
	defer df.Release()

	rev, err := lazy.FromDataFrame(df).Reverse().
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer rev.Release()
	col, _ := rev.Column("x")
	arr := col.Chunk(0).(*array.Int64)
	for i, want := range []int64{5, 4, 3, 2, 1} {
		if arr.Value(i) != want {
			t.Errorf("Reverse[%d] = %d, want %d", i, arr.Value(i), want)
		}
	}

	tail, err := lazy.FromDataFrame(df).Tail(2).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Release()
	tArr := tail.Columns()[0].Chunk(0).(*array.Int64)
	if tArr.Len() != 2 || tArr.Value(0) != 4 || tArr.Value(1) != 5 {
		t.Errorf("Tail = [%d, %d], want [4, 5]", tArr.Value(0), tArr.Value(1))
	}
}

func TestLazyWithColumnSingle(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		WithColumn(expr.Col("a").MulLit(int64(10)).Alias("a10")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if !out.HasColumn("a10") {
		t.Errorf("missing a10 column")
	}
}

func TestLazyExprFunctionsPath(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	src, _ := series.FromFloat64("x", []float64{-4, -1, 0, 1, 4}, nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(src)
	defer df.Release()
	ctx := context.Background()

	out, err := lazy.FromDataFrame(df).
		Select(
			expr.Col("x").Abs().Alias("abs"),
			expr.Col("x").Abs().Sqrt().Alias("sqrt"),
			expr.Col("x").Clip(-2, 2).Alias("clip"),
			expr.Col("x").Round(0).Alias("round"),
			expr.Col("x").Sign().Alias("sign"),
			expr.Col("x").Pow(2).Alias("sq"),
			expr.Col("x").Floor().Alias("floor"),
			expr.Col("x").Ceil().Alias("ceil"),
			expr.Col("x").Reverse().Alias("rev"),
			expr.Col("x").Shift(1).Alias("shifted"),
		).
		Collect(ctx, lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Width() != 10 {
		t.Fatalf("width = %d, want 10", out.Width())
	}

	// Spot-check a couple.
	absC, _ := out.Column("abs")
	absArr := absC.Chunk(0).(*array.Float64)
	for i, want := range []float64{4, 1, 0, 1, 4} {
		if absArr.Value(i) != want {
			t.Errorf("abs[%d] = %v, want %v", i, absArr.Value(i), want)
		}
	}

	clipC, _ := out.Column("clip")
	clipArr := clipC.Chunk(0).(*array.Float64)
	for i, want := range []float64{-2, -1, 0, 1, 2} {
		if clipArr.Value(i) != want {
			t.Errorf("clip[%d] = %v, want %v", i, clipArr.Value(i), want)
		}
	}
}

func TestLazyFillNullExpr(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(s)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("a").FillNull(int64(-1)).Alias("filled")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	col, _ := out.Column("filled")
	arr := col.Chunk(0).(*array.Int64)
	want := []int64{1, -1, 3, -1, 5}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("filled[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

func TestLazySink(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(s)
	defer df.Release()

	var sunk int
	err := lazy.FromDataFrame(df).Sink(context.Background(),
		func(ctx context.Context, d *dataframe.DataFrame) error {
			sunk = d.Height()
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if sunk != 3 {
		t.Errorf("Sink saw %d rows, want 3", sunk)
	}
}
