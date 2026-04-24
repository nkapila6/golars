package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestDataFrameSort(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{3, 1, 4, 1, 5}, nil, series.WithAllocator(mem))
	b, _ := series.FromString("b", []string{"c", "a", "d", "b", "e"}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	got, err := df.Sort(ctx, "a", false, dataframe.WithSortAllocator(mem))
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	defer got.Release()

	aCol, _ := got.Column("a")
	aVals := aCol.Chunk(0).(*array.Int64).Int64Values()
	want := []int64{1, 1, 3, 4, 5}
	for i := range want {
		if aVals[i] != want[i] {
			t.Errorf("a[%d] = %d, want %d", i, aVals[i], want[i])
		}
	}

	// b must follow a's permutation (stable). Original b in row order [c,a,d,b,e]
	// at sorted indices [1,3,0,2,4] becomes [a,b,c,d,e].
	bCol, _ := got.Column("b")
	bArr := bCol.Chunk(0).(*array.String)
	wantB := []string{"a", "b", "c", "d", "e"}
	for i, w := range wantB {
		if g := bArr.Value(i); g != w {
			t.Errorf("b[%d] = %q, want %q", i, g, w)
		}
	}
}

func TestDataFrameSortBy(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// Rows: (1, b), (1, a), (0, c), (0, b)
	// Sort by (a asc, b asc) => (0, b), (0, c), (1, a), (1, b).
	a, _ := series.FromInt64("a", []int64{1, 1, 0, 0}, nil, series.WithAllocator(mem))
	b, _ := series.FromString("b", []string{"b", "a", "c", "b"}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	got, err := df.SortBy(ctx,
		[]string{"a", "b"},
		[]compute.SortOptions{{}, {}},
		dataframe.WithSortAllocator(mem))
	if err != nil {
		t.Fatalf("SortBy: %v", err)
	}
	defer got.Release()

	aCol, _ := got.Column("a")
	aVals := aCol.Chunk(0).(*array.Int64).Int64Values()
	wantA := []int64{0, 0, 1, 1}
	for i := range wantA {
		if aVals[i] != wantA[i] {
			t.Errorf("a[%d] = %d, want %d", i, aVals[i], wantA[i])
		}
	}
	bCol, _ := got.Column("b")
	bArr := bCol.Chunk(0).(*array.String)
	wantB := []string{"b", "c", "a", "b"}
	for i, w := range wantB {
		if g := bArr.Value(i); g != w {
			t.Errorf("b[%d] = %q, want %q", i, g, w)
		}
	}
}
