package lazy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestLazyGroupByAgg(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	k, _ := series.FromString("k", []string{"a", "b", "a", "b", "a"}, nil, series.WithAllocator(mem))
	v, _ := series.FromInt64("v", []int64{1, 10, 2, 20, 3}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		GroupBy("k").
		Agg(expr.Col("v").Sum().Alias("v_sum")).
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	defer out.Release()

	if out.Height() != 2 {
		t.Errorf("Height = %d, want 2", out.Height())
	}
	sumCol, _ := out.Column("v_sum")
	vals := sumCol.Chunk(0).(*array.Int64).Int64Values()
	if vals[0] != 6 || vals[1] != 30 {
		t.Errorf("sums = %v, want [6, 30]", vals)
	}
}

func TestLazyJoin(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	lk, _ := series.FromInt64("id", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	lv, _ := series.FromString("name", []string{"a", "b", "c"}, nil, series.WithAllocator(mem))
	left, _ := dataframe.New(lk, lv)
	defer left.Release()

	rk, _ := series.FromInt64("id", []int64{2, 3}, nil, series.WithAllocator(mem))
	rv, _ := series.FromInt64("qty", []int64{10, 20}, nil, series.WithAllocator(mem))
	right, _ := dataframe.New(rk, rv)
	defer right.Release()

	out, err := lazy.FromDataFrame(left).
		Join(lazy.FromDataFrame(right), []string{"id"}, dataframe.InnerJoin).
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	defer out.Release()

	if out.Height() != 2 {
		t.Errorf("Height = %d, want 2", out.Height())
	}
	if !out.Contains("qty") {
		t.Errorf("output missing qty")
	}
}

func TestLazyGroupByThenFilterExplain(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	k, _ := series.FromString("k", []string{"a", "b", "a"}, nil, series.WithAllocator(mem))
	v, _ := series.FromInt64("v", []int64{1, 10, 2}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	lf := lazy.FromDataFrame(df).
		GroupBy("k").
		Agg(expr.Col("v").Sum().Alias("s")).
		Filter(expr.Col("s").GtLit(int64(5)))

	report, err := lf.Explain()
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !strings.Contains(report, "AGG") || !strings.Contains(report, "FILTER") {
		t.Errorf("explain missing sections:\n%s", report)
	}
}
