// Typed-column variant of ./examples/rolling.
// Run: go run ./examples/rolling/generic
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

	prices := make([]float64, 30)
	for i := range prices {
		prices[i] = 100 + float64(i)*1.5 + float64(i%5)*3.0
	}
	p, _ := series.FromFloat64("price", prices, nil)
	df, _ := dataframe.New(p)
	defer df.Release()

	price := expr.Float("price")

	out, err := lazy.FromDataFrame(df).
		Select(
			price.Expr,
			price.RollingMean(7, 1).As("ma7").Expr,
			price.RollingStd(7, 2).As("vol7").Expr,
			price.RollingMin(7, 1).As("lo7").Expr,
			price.RollingMax(7, 1).As("hi7").Expr,
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Release()
	fmt.Println(out.Head(10))
	fmt.Println("(first 10 rows of 30)")
}
