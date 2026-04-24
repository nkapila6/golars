// Window functions via Expr.Over(keys...).
// Run: go run ./examples/over_window
// Typed variant: go run ./examples/over_window/generic
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	// Orders dataset: each row is one line-item with customer + amount.
	customer, _ := series.FromString("customer", []string{
		"alice", "alice", "bob", "bob", "bob", "carol",
	}, nil)
	amount, _ := series.FromInt64("amount", []int64{
		50, 20, 100, 75, 25, 10,
	}, nil)
	df, _ := dataframe.New(customer, amount)
	defer df.Release()

	// Add two window columns:
	//   total_by_customer: sum(amount) broadcast across the customer's rows
	//   pct_of_total:      amount / total_by_customer
	out, err := lazy.FromDataFrame(df).
		WithColumns(
			expr.Col("amount").Sum().Over("customer").Alias("total_by_customer"),
		).
		WithColumns(
			expr.Col("amount").Cast(dtype.Float64()).Div(expr.Col("total_by_customer").Cast(dtype.Float64())).Alias("pct_of_total"),
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Release()
	fmt.Println(out)
}
