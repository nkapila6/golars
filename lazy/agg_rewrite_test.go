package lazy_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestGroupByAggComplexInput exercises the planner rewrite that hoists
// complex agg inputs into a WithColumns stage. Without the rewrite,
// `col("price") * col("qty")).Sum()` would error at Collect with a
// "must be a bare column" message; with it, the query runs and the
// aliased output column matches the manual-hoist variant.
func TestGroupByAggComplexInput(t *testing.T) {
	ctx := context.Background()

	key, _ := series.FromString("k", []string{"a", "a", "b", "b"}, nil)
	price, _ := series.FromFloat64("price", []float64{10, 20, 30, 40}, nil)
	qty, _ := series.FromFloat64("qty", []float64{1, 2, 3, 4}, nil)
	df, err := dataframe.New(key, price, qty)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	// Inline complex agg: price * qty summed per key.
	revenue := expr.Col("price").Mul(expr.Col("qty"))
	out, err := lazy.FromDataFrame(df).
		GroupBy("k").
		Agg(revenue.Sum().Alias("revenue")).
		Sort("k", false).
		Collect(ctx)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	defer out.Release()

	if got, want := out.Width(), 2; got != want {
		t.Fatalf("width = %d, want %d", got, want)
	}
	if out.Schema().Field(1).Name != "revenue" {
		t.Errorf("second column name = %q, want revenue",
			out.Schema().Field(1).Name)
	}
	col, _ := out.Column("revenue")
	arr := col.Chunk(0).(*array.Float64)
	// k=a: 10*1 + 20*2 = 50. k=b: 30*3 + 40*4 = 250.
	if got := arr.Value(0); got != 50 {
		t.Errorf("revenue[0] = %v, want 50", got)
	}
	if got := arr.Value(1); got != 250 {
		t.Errorf("revenue[1] = %v, want 250", got)
	}
}
