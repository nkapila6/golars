package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestDataFrameTopK(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 5, 3, 2, 9, 4}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	top, err := df.TopK(context.Background(), 3, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer top.Release()
	col, _ := top.Column("a")
	arr := col.Chunk(0).(*array.Int64)
	want := []int64{9, 5, 4}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("top[%d] = %d, want %d", i, arr.Value(i), v)
		}
	}

	bot, _ := df.BottomK(context.Background(), 2, "a")
	defer bot.Release()
	bcol, _ := bot.Column("a")
	barr := bcol.Chunk(0).(*array.Int64)
	if barr.Value(0) != 1 || barr.Value(1) != 2 {
		t.Fatalf("bottom: got %d,%d, want 1,2", barr.Value(0), barr.Value(1))
	}
}

func TestDataFramePartitionBy(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromString("k", []string{"a", "b", "a", "b", "a"}, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	parts, err := df.PartitionBy(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, p := range parts {
			p.Release()
		}
	}()
	if len(parts) != 2 {
		t.Fatalf("parts: got %d want 2", len(parts))
	}
	// Order preserves first appearance: "a" then "b".
	if parts[0].Height() != 3 || parts[1].Height() != 2 {
		t.Fatalf("heights: got %d,%d want 3,2", parts[0].Height(), parts[1].Height())
	}
}
