package lazy_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestLazySumHorizontalAppendsColumn(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{10, 20, 30}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		SumHorizontal("total", "a", "b").
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Width() != 3 {
		t.Fatalf("width: got %d want 3", out.Width())
	}
	col, err := out.Column("total")
	if err != nil {
		t.Fatal(err)
	}
	// All-int64, no-null inputs preserve the Int64 dtype.
	arr := col.Chunk(0).(*array.Int64)
	for i, want := range []int64{11, 22, 33} {
		if arr.Value(i) != want {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), want)
		}
	}
}

func TestLazyMeanHorizontalSchema(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a", []float64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromFloat64("b", []float64{3, 2, 1}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	lf := lazy.FromDataFrame(df).MeanHorizontal("avg")
	sch, err := lf.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sch.Index("avg"); !ok {
		t.Fatalf("expected schema to contain 'avg' column")
	}
	out, err := lf.Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("avg")
	arr := col.Chunk(0).(*array.Float64)
	for i := range 3 {
		if arr.Value(i) != 2 {
			t.Fatalf("mean row %d: got %v want 2", i, arr.Value(i))
		}
	}
}
