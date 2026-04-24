package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func buildSample(t *testing.T, alloc memory.Allocator, n int) *dataframe.DataFrame {
	t.Helper()
	vals := make([]int64, n)
	names := make([]string, n)
	for i := range n {
		vals[i] = int64(i)
		names[i] = string(rune('a' + i%26))
	}
	a, err := series.FromInt64("id", vals, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	b, err := series.FromString("name", names, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(a, b)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func TestSampleBasic(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := buildSample(t, alloc, 100)
	defer df.Release()
	out, err := df.Sample(context.Background(), 10, false, 42)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 10 {
		t.Fatalf("height=%d", out.Height())
	}
	if out.Width() != 2 {
		t.Fatalf("width=%d", out.Width())
	}
}

func TestSampleDeterministicSeed(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := buildSample(t, alloc, 100)
	defer df.Release()
	a, _ := df.Sample(context.Background(), 20, false, 123)
	defer a.Release()
	b, _ := df.Sample(context.Background(), 20, false, 123)
	defer b.Release()
	aIDs := a.ColumnAt(0).Chunk(0).(*array.Int64)
	bIDs := b.ColumnAt(0).Chunk(0).(*array.Int64)
	if aIDs.Len() != bIDs.Len() {
		t.Fatal("seeded samples must have same length")
	}
	for i := range aIDs.Len() {
		if aIDs.Value(i) != bIDs.Value(i) {
			t.Fatalf("seed determinism broken at [%d]: %d vs %d", i, aIDs.Value(i), bIDs.Value(i))
		}
	}
}

func TestSampleNoReplacementAllDistinct(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := buildSample(t, alloc, 50)
	defer df.Release()
	out, err := df.Sample(context.Background(), 30, false, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.ColumnAt(0).Chunk(0).(*array.Int64)
	seen := make(map[int64]struct{}, 30)
	for i := range 30 {
		v := arr.Value(i)
		if _, dup := seen[v]; dup {
			t.Fatalf("no-replacement produced duplicate %d", v)
		}
		seen[v] = struct{}{}
	}
}

func TestSampleExceedsHeight(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := buildSample(t, alloc, 10)
	defer df.Release()
	_, err := df.Sample(context.Background(), 20, false, 1)
	if err == nil {
		t.Fatal("should error when n > height without replacement")
	}
}

func TestSampleWithReplacement(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := buildSample(t, alloc, 10)
	defer df.Release()
	// n > height is fine with replacement.
	out, err := df.Sample(context.Background(), 50, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 50 {
		t.Fatalf("with replacement height=%d", out.Height())
	}
}

func TestSampleZero(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := buildSample(t, alloc, 10)
	defer df.Release()
	out, err := df.Sample(context.Background(), 0, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 0 {
		t.Fatalf("zero sample height=%d", out.Height())
	}
}

func TestSampleNegative(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := buildSample(t, alloc, 10)
	defer df.Release()
	if _, err := df.Sample(context.Background(), -1, false, 0); err == nil {
		t.Fatal("negative n should error")
	}
}
