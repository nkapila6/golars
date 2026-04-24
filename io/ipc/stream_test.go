package ipc_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestIPCStreamRoundTrip(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{4, 5, 6}, nil, series.WithAllocator(alloc))
	df1, _ := dataframe.New(a, b)
	defer df1.Release()

	c, _ := series.FromInt64("a", []int64{10, 20}, nil, series.WithAllocator(alloc))
	d, _ := series.FromInt64("b", []int64{30, 40}, nil, series.WithAllocator(alloc))
	df2, _ := dataframe.New(c, d)
	defer df2.Release()

	var buf bytes.Buffer
	ctx := context.Background()
	sw, err := ipc.NewStreamWriter(&buf, df1, ipc.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	if err := sw.Write(ctx, df1); err != nil {
		t.Fatal(err)
	}
	if err := sw.Write(ctx, df2); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}

	sr, err := ipc.NewStreamReader(&buf, ipc.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	var seen int
	var totalRows int
	for df, err := range sr.Iter(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if df == nil {
			continue
		}
		totalRows += df.Height()
		seen++
		col, _ := df.Column("a")
		_ = col.Chunk(0).(*array.Int64)
		df.Release()
	}
	if seen != 2 {
		t.Fatalf("seen: got %d want 2", seen)
	}
	if totalRows != 5 {
		t.Fatalf("rows: got %d want 5", totalRows)
	}
}
