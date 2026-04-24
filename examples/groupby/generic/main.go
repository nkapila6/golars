// Typed-column variant of ./examples/groupby.
// Run: go run ./examples/groupby/generic
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

	var (
		s = expr.Int("salary")
		b = expr.Float("bonus")
		d = expr.Str("dept")
	)

	agg, err := df.GroupBy("dept").Agg(ctx, []expr.Expr{
		s.Sum().As("salary_sum").Expr,
		s.Mean().As("salary_mean").Expr,
		b.Sum().As("bonus_sum").Expr,
		d.Count().As("count").Expr,
	})
	if err != nil {
		panic(err)
	}
	defer agg.Release()
	fmt.Println(agg)
}
