package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestGtLitInt64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("x", []int64{1, 3, 5, 7, 9}, nil, series.WithAllocator(mem))
	defer s.Release()

	mask, err := compute.GtLit(ctx, s, int64(4), compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("GtLit: %v", err)
	}
	defer mask.Release()

	arr := mask.Chunk(0).(*array.Boolean)
	want := []bool{false, false, true, true, true}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("mask[%d] = %v, want %v", i, arr.Value(i), w)
		}
	}
}

func TestGtLitFloat64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromFloat64("x", []float64{0.5, 1.5, 2.5, 3.5}, nil, series.WithAllocator(mem))
	defer s.Release()

	mask, err := compute.GtLit(ctx, s, 2.0, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("GtLit: %v", err)
	}
	defer mask.Release()

	arr := mask.Chunk(0).(*array.Boolean)
	want := []bool{false, false, true, true}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("mask[%d] = %v, want %v", i, arr.Value(i), w)
		}
	}
}

func TestEqLitCoerce(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("x", []int64{1, 2, 3, 2, 4}, nil, series.WithAllocator(mem))
	defer s.Release()

	// int literal gets coerced to int64.
	mask, err := compute.EqLit(ctx, s, 2, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("EqLit: %v", err)
	}
	defer mask.Release()
	arr := mask.Chunk(0).(*array.Boolean)
	want := []bool{false, true, false, true, false}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("mask[%d] = %v, want %v", i, arr.Value(i), w)
		}
	}
}

func TestLtLeGeNeLit(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("x", []int64{1, 2, 3, 4}, nil, series.WithAllocator(mem))
	defer s.Release()

	// < 3: [T, T, F, F]
	lt, _ := compute.LtLit(ctx, s, int64(3), compute.WithAllocator(mem))
	defer lt.Release()
	if arr := lt.Chunk(0).(*array.Boolean); !arr.Value(0) || !arr.Value(1) || arr.Value(2) || arr.Value(3) {
		t.Error("LtLit: wrong result")
	}

	// <= 3: [T, T, T, F]
	le, _ := compute.LeLit(ctx, s, int64(3), compute.WithAllocator(mem))
	defer le.Release()
	if arr := le.Chunk(0).(*array.Boolean); !arr.Value(0) || !arr.Value(1) || !arr.Value(2) || arr.Value(3) {
		t.Error("LeLit: wrong result")
	}

	// >= 3: [F, F, T, T]
	ge, _ := compute.GeLit(ctx, s, int64(3), compute.WithAllocator(mem))
	defer ge.Release()
	if arr := ge.Chunk(0).(*array.Boolean); arr.Value(0) || arr.Value(1) || !arr.Value(2) || !arr.Value(3) {
		t.Error("GeLit: wrong result")
	}

	// != 2: [T, F, T, T]
	ne, _ := compute.NeLit(ctx, s, int64(2), compute.WithAllocator(mem))
	defer ne.Release()
	if arr := ne.Chunk(0).(*array.Boolean); !arr.Value(0) || arr.Value(1) || !arr.Value(2) || !arr.Value(3) {
		t.Error("NeLit: wrong result")
	}
}
