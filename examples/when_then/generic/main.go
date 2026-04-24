// Typed-column variant of ./examples/when_then.
// Run: go run ./examples/when_then/generic
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

	a := expr.Int("age")

	// LitOf infers T from the argument so the branch-value types are
	// checked at build time. The outer When takes an untyped Expr
	// because conditional branches are themselves Exprs.
	out, err := lazy.FromDataFrame(df).
		WithColumns(
			expr.When(a.Lt(18)).
				Then(expr.LitOf("minor")).
				Otherwise(
					expr.When(a.Ge(65)).
						Then(expr.LitOf("senior")).
						Otherwise(expr.LitOf("adult")),
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
