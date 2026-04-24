package dataframe_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestDataFrameFilter(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(mem))
	b, _ := series.FromString("b", []string{"v", "w", "x", "y", "z"}, nil, series.WithAllocator(mem))
	df, err := dataframe.New(a, b)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer df.Release()

	mask, _ := series.FromBool("m",
		[]bool{true, false, true, true, false},
		nil,
		series.WithAllocator(mem))
	defer mask.Release()

	got, err := df.Filter(ctx, mask, dataframe.WithFilterAllocator(mem))
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer got.Release()

	if got.Height() != 3 {
		t.Errorf("Height = %d, want 3", got.Height())
	}

	aCol, _ := got.Column("a")
	aVals := aCol.Chunk(0).(*array.Int64).Int64Values()
	if len(aVals) != 3 || aVals[0] != 1 || aVals[1] != 3 || aVals[2] != 4 {
		t.Errorf("a values = %v, want [1 3 4]", aVals)
	}

	bCol, _ := got.Column("b")
	bArr := bCol.Chunk(0).(*array.String)
	bVals := []string{bArr.Value(0), bArr.Value(1), bArr.Value(2)}
	if bVals[0] != "v" || bVals[1] != "x" || bVals[2] != "y" {
		t.Errorf("b values = %v, want [v x y]", bVals)
	}
}

func TestDataFrameFilterLengthMismatch(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	defer df.Release()

	mask, _ := series.FromBool("m", []bool{true, false}, nil, series.WithAllocator(mem))
	defer mask.Release()

	_, err := df.Filter(ctx, mask, dataframe.WithFilterAllocator(mem))
	if !errors.Is(err, compute.ErrLengthMismatch) {
		t.Errorf("expected ErrLengthMismatch, got %v", err)
	}
}

func TestDataFrameFilterWrongMaskDType(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	defer df.Release()

	wrong, _ := series.FromInt64("m", []int64{1, 0, 1}, nil, series.WithAllocator(mem))
	defer wrong.Release()

	_, err := df.Filter(ctx, wrong, dataframe.WithFilterAllocator(mem))
	if !errors.Is(err, compute.ErrMaskNotBool) {
		t.Errorf("expected ErrMaskNotBool, got %v", err)
	}
}
