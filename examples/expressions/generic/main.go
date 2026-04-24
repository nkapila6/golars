// Typed-column variant of ./examples/expressions.
// Run: go run ./examples/expressions/generic
package main

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
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

	var (
		p = expr.Float("price")
		q = expr.Int("qty")
	)

	out, err := lazy.FromDataFrame(df).
		WithColumns(
			// total = price * qty; cast qty to float64 via typed helper.
			p.MulCol(q.CastFloat64()).As("total").Expr,
			// bulk = qty > 2; literal inferred as int64 from Gt.
			q.Gt(2).Alias("bulk"),
		).
		Collect(ctx)
	if err != nil {
		panic(err)
	}
	defer out.Release()
	fmt.Println(out)
}
