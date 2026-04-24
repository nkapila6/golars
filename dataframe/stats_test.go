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

func TestDataFrameCorrMatrix(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a", []float64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	b, _ := series.FromFloat64("b", []float64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	c, _ := series.FromFloat64("c", []float64{4, 3, 2, 1}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	out, err := df.Corr(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Height() != 3 || out.Width() != 4 {
		t.Fatalf("shape: got %dx%d want 3x4", out.Height(), out.Width())
	}
	// Diagonal must be 1.0; a-c correlation is -1.0.
	cols := []string{"a", "b", "c"}
	for i, name := range cols {
		col, _ := out.Column(name)
		arr := col.Chunk(0).(*array.Float64)
		if math.Abs(arr.Value(i)-1) > 1e-9 {
			t.Fatalf("%s[%d] (diagonal) = %v", name, i, arr.Value(i))
		}
	}
	colC, _ := out.Column("c")
	if math.Abs(colC.Chunk(0).(*array.Float64).Value(0)+1) > 1e-9 {
		t.Fatalf("corr(a,c) should be -1")
	}
}
