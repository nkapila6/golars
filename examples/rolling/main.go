// Rolling sum / mean / std on a single-column time series.
// Run: go run ./examples/rolling
// Typed variant: go run ./examples/rolling/generic
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

	// 30 daily price observations.
	prices := make([]float64, 30)
	for i := range prices {
		prices[i] = 100 + float64(i)*1.5 + float64(i%5)*3.0
	}
	p, _ := series.FromFloat64("price", prices, nil)
	df, _ := dataframe.New(p)
	defer df.Release()

	// 7-day moving average + 7-day rolling std.
	out, err := lazy.FromDataFrame(df).
		Select(
			expr.Col("price"),
			expr.Col("price").RollingMean(7, 1).Alias("ma7"),
			expr.Col("price").RollingStd(7, 2).Alias("vol7"),
			expr.Col("price").RollingMin(7, 1).Alias("lo7"),
			expr.Col("price").RollingMax(7, 1).Alias("hi7"),
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Release()
	fmt.Println(out.Head(10))
	fmt.Println("(first 10 rows of 30)")
}
