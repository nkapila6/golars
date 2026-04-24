// Conditional expressions with when().then().otherwise().
// Run: go run ./examples/when_then
// Typed variant: go run ./examples/when_then/generic
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

	age, _ := series.FromInt64("age", []int64{12, 17, 18, 25, 64, 71}, nil)
	df, _ := dataframe.New(age)
	defer df.Release()

	// Tag each age as minor / adult / senior.
	out, err := lazy.FromDataFrame(df).
		WithColumns(
			expr.When(expr.Col("age").Lt(expr.Lit(int64(18)))).
				Then(expr.Lit("minor")).
				Otherwise(
					expr.When(expr.Col("age").Ge(expr.Lit(int64(65)))).
						Then(expr.Lit("senior")).
						Otherwise(expr.Lit("adult")),
				).
				Alias("bucket"),
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Release()
	fmt.Println(out)
}
