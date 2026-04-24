// Group by a key column and run per-group aggregations.
// Run: go run ./examples/groupby
// Typed variant: go run ./examples/groupby/generic
package main

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	dept, _ := series.FromString("dept", []string{"eng", "eng", "sales", "sales", "eng", "ops"}, nil)
	salary, _ := series.FromInt64("salary", []int64{100, 120, 80, 90, 115, 70}, nil)
	bonus, _ := series.FromFloat64("bonus", []float64{10, 12, 6, 7, 11, 5}, nil)

	df, _ := dataframe.New(dept, salary, bonus)
	defer df.Release()

	agg, err := df.GroupBy("dept").Agg(ctx, []expr.Expr{
		expr.Col("salary").Sum().Alias("salary_sum"),
		expr.Col("salary").Mean().Alias("salary_mean"),
		expr.Col("bonus").Sum().Alias("bonus_sum"),
		expr.Col("dept").Count().Alias("count"),
	})
	if err != nil {
		panic(err)
	}
	defer agg.Release()
	fmt.Println(agg)
}
