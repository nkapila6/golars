// Typed-column variant of ./examples/over_window.
// Run: go run ./examples/over_window/generic
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	customer, _ := series.FromString("customer", []string{
		"alice", "alice", "bob", "bob", "bob", "carol",
	}, nil)
	amount, _ := series.FromInt64("amount", []int64{
		50, 20, 100, 75, 25, 10,
	}, nil)
	df, _ := dataframe.New(customer, amount)
	defer df.Release()

	amt := expr.Int("amount")

	out, err := lazy.FromDataFrame(df).
		WithColumns(
			amt.Sum().Over("customer").As("total_by_customer").Expr,
		).
		WithColumns(
			// total_by_customer exists only after the previous stage, so
			// reference it via expr.Float (typed) over its new name.
			amt.CastFloat64().DivCol(expr.Float("total_by_customer")).As("pct_of_total").Expr,
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Release()
	fmt.Println(out)
}
