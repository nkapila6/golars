// Build and collect a lazy pipeline: filter -> groupby -> sort.
// Run: go run ./examples/lazy
// Typed variant: go run ./examples/lazy/generic
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

	dept, _ := series.FromString("dept", []string{"eng", "eng", "sales", "sales", "eng", "ops", "ops"}, nil)
	salary, _ := series.FromInt64("salary", []int64{100, 120, 80, 90, 115, 70, 75}, nil)
	df, _ := dataframe.New(dept, salary)
	defer df.Release()

	// Build a lazy pipeline. Nothing runs until Collect.
	plan := lazy.FromDataFrame(df).
		Filter(expr.Col("salary").Gt(expr.Lit(int64(75)))).
		GroupBy("dept").
		Agg(expr.Col("salary").Sum().Alias("total"), expr.Col("salary").Mean().Alias("avg")).
		Sort("total", true)

	fmt.Println("logical plan (indented):")
	fmt.Println(lazy.Explain(plan.Plan()))

	fmt.Println("logical plan (tree):")
	fmt.Println(lazy.ExplainTree(plan.Plan()))

	out, err := plan.Collect(ctx)
	if err != nil {
		panic(err)
	}
	defer out.Release()
	fmt.Println("result:")
	fmt.Println(out)
}
