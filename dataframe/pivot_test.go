package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestDataFrameTranspose(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{3, 4}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := df.Transpose(context.Background(), "col", "row")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	// Expect: col=[a,b], row0=[1,3], row1=[2,4]
	if out.Height() != 2 || out.Width() != 3 {
		t.Fatalf("shape: got %dx%d, want 2x3", out.Height(), out.Width())
	}
	col, _ := out.Column("col")
	if col.Chunk(0).(*array.String).Value(0) != "a" {
		t.Fatalf("header[0] = %q", col.Chunk(0).(*array.String).Value(0))
	}
	r0, _ := out.Column("row0")
	if r0.Chunk(0).(*array.Float64).Value(0) != 1 {
		t.Fatalf("row0[a] = %v, want 1", r0.Chunk(0).(*array.Float64).Value(0))
	}
}

func TestDataFrameUnpivot(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	id, _ := series.FromString("id", []string{"x", "y"}, nil, series.WithAllocator(alloc))
	a, _ := series.FromInt64("a", []int64{1, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{2, 4}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(id, a, b)
	defer df.Release()

	out, err := df.Unpivot(context.Background(), []string{"id"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	// 2 rows * 2 value cols = 4 rows.
	if out.Height() != 4 {
		t.Fatalf("rows: got %d want 4", out.Height())
	}
	// Columns: id, variable, value
	if out.Width() != 3 {
		t.Fatalf("cols: got %d want 3", out.Width())
	}
}
