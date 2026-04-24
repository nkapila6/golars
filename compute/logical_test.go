package compute_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestAndKleene(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// truth table including nulls (N denotes null):
	//    a:  T T T F F F N N N
	//    b:  T F N T F N T F N
	//   and: T F N F F F N F N
	a, _ := series.FromBool("a",
		[]bool{true, true, true, false, false, false, false, false, false},
		[]bool{true, true, true, true, true, true, false, false, false},
		series.WithAllocator(mem))
	b, _ := series.FromBool("b",
		[]bool{true, false, false, true, false, false, true, false, false},
		[]bool{true, true, false, true, true, false, true, true, false},
		series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	got, err := compute.And(ctx, a, b, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("And: %v", err)
	}
	defer got.Release()

	// Kleene expected: T F N F F F N F N
	wantVals := []bool{true, false, false, false, false, false, false, false, false}
	wantValid := []bool{true, true, false, true, true, true, false, true, false}
	assertBoolValues(t, got, wantVals, wantValid)
}

func TestOrKleene(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// Or truth table:
	//    a:  T T T F F F N N N
	//    b:  T F N T F N T F N
	//    or: T T T T F N T N N
	a, _ := series.FromBool("a",
		[]bool{true, true, true, false, false, false, false, false, false},
		[]bool{true, true, true, true, true, true, false, false, false},
		series.WithAllocator(mem))
	b, _ := series.FromBool("b",
		[]bool{true, false, false, true, false, false, true, false, false},
		[]bool{true, true, false, true, true, false, true, true, false},
		series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	got, _ := compute.Or(ctx, a, b, compute.WithAllocator(mem))
	defer got.Release()

	wantVals := []bool{true, true, true, true, false, false, true, false, false}
	wantValid := []bool{true, true, true, true, true, false, true, false, false}
	assertBoolValues(t, got, wantVals, wantValid)
}

func TestNot(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromBool("s",
		[]bool{true, false, true},
		[]bool{true, false, true},
		series.WithAllocator(mem))
	defer s.Release()

	got, err := compute.Not(ctx, s, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Not: %v", err)
	}
	defer got.Release()

	assertBoolValues(t, got, []bool{false, false, false}, []bool{true, false, true})
}

func TestIsNullAndIsNotNull(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s, _ := series.FromInt64("s",
		[]int64{1, 2, 3},
		[]bool{true, false, true},
		series.WithAllocator(mem))
	defer s.Release()

	isn, _ := compute.IsNull(ctx, s, compute.WithAllocator(mem))
	defer isn.Release()
	assertBoolValues(t, isn, []bool{false, true, false}, nil)
	if isn.NullCount() != 0 {
		t.Errorf("IsNull result should have no nulls, got %d", isn.NullCount())
	}

	inn, _ := compute.IsNotNull(ctx, s, compute.WithAllocator(mem))
	defer inn.Release()
	assertBoolValues(t, inn, []bool{true, false, true}, nil)
}

func TestIsNullNoNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("s", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	defer s.Release()

	isn, _ := compute.IsNull(context.Background(), s, compute.WithAllocator(mem))
	defer isn.Release()
	assertBoolValues(t, isn, []bool{false, false, false}, nil)
}

func TestNotRejectsNonBool(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("s", []int64{1}, nil, series.WithAllocator(mem))
	defer s.Release()

	_, err := compute.Not(context.Background(), s, compute.WithAllocator(mem))
	if !errors.Is(err, compute.ErrUnsupportedDType) {
		t.Errorf("expected ErrUnsupportedDType, got %v", err)
	}
}
