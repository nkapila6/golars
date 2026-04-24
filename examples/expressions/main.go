// Column expressions used in select/with_columns.
// Run: go run ./examples/expressions
// Typed variant: go run ./examples/expressions/generic
package main

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	price, _ := series.FromFloat64("price", []float64{10.0, 25.0, 5.5, 42.0}, nil)
	qty, _ := series.FromInt64("qty", []int64{1, 3, 7, 2}, nil)
	df, _ := dataframe.New(price, qty)
	defer df.Release()

	// WithColumns adds derived columns without dropping originals.
	out, err := lazy.FromDataFrame(df).
		WithColumns(
			// total = price * qty (float multiplied by cast int).
			expr.Col("price").Mul(expr.Col("qty").Cast(dtype.Float64())).Alias("total"),
			// discount = qty > 2
			expr.Col("qty").Gt(expr.Lit(int64(2))).Alias("bulk"),
		).
		Collect(ctx)
	if err != nil {
		panic(err)
	}
	defer out.Release()
	fmt.Println(out)
}
