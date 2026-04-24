package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestColAndEvalBasic(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, expr.Col("a"), df)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	if out.Name() != "a" || out.Len() != 4 {
		t.Errorf("name=%q len=%d, want a,4", out.Name(), out.Len())
	}
}

func TestArithmeticExpression(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", []int64{10, 20, 30, 40}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	e := expr.Col("a").Add(expr.Col("b")).MulLit(int64(2)).Alias("result")
	out, err := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, e, df)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	if out.Name() != "result" {
		t.Errorf("name = %q, want result", out.Name())
	}
	vals := out.Chunk(0).(*array.Int64).Int64Values()
	want := []int64{22, 44, 66, 88}
	for i := range want {
		if vals[i] != want[i] {
			t.Errorf("[%d] = %d, want %d", i, vals[i], want[i])
		}
	}
}

func TestComparisonExpression(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 5, 10, 15}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, _ := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, expr.Col("a").GtLit(int64(5)), df)
	defer out.Release()

	if !out.DType().IsBool() {
		t.Errorf("dtype = %s, want bool", out.DType())
	}
	vals := out.Chunk(0).(*array.Boolean)
	want := []bool{false, false, true, true}
	for i := range want {
		if vals.Value(i) != want[i] {
			t.Errorf("[%d] = %v, want %v", i, vals.Value(i), want[i])
		}
	}
}

func TestIsNullNegate(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a",
		[]int64{1, 0, 3},
		[]bool{true, false, true},
		series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	defer df.Release()

	nonNull, _ := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, expr.Col("a").IsNotNull(), df)
	defer nonNull.Release()
	barr := nonNull.Chunk(0).(*array.Boolean)
	if barr.Value(0) != true || barr.Value(1) != false || barr.Value(2) != true {
		t.Errorf("IsNotNull result wrong")
	}
}

func TestSumAggregation(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, _ := eval.Eval(ctx, eval.EvalContext{Alloc: mem},
		expr.Col("a").Sum().Alias("s"), df)
	defer out.Release()

	if out.Len() != 1 {
		t.Fatalf("Sum should return 1 row, got %d", out.Len())
	}
	v := out.Chunk(0).(*array.Int64).Int64Values()[0]
	if v != 10 {
		t.Errorf("Sum = %d, want 10", v)
	}
}

func TestIntFloatPromotion(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{0.5, 0.5, 0.5}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, _ := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, expr.Col("a").Add(expr.Col("b")), df)
	defer out.Release()

	if !out.DType().IsFloating() {
		t.Errorf("dtype = %s, want floating", out.DType())
	}
	vals := out.Chunk(0).(*array.Float64).Float64Values()
	want := []float64{1.5, 2.5, 3.5}
	for i := range want {
		if vals[i] != want[i] {
			t.Errorf("[%d] = %v, want %v", i, vals[i], want[i])
		}
	}
}
