// FillNull, ForwardFill, BackwardFill, FillNan.
// Run: go run ./examples/fill_strategies
package main

import (
	"context"
	"fmt"
	"log"
	"math"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	// int64 with explicit null mask.
	vals, _ := series.FromInt64("price",
		[]int64{100, 0, 0, 0, 110, 0, 120},
		[]bool{true, false, false, false, true, false, true})
	df, _ := dataframe.New(vals)
	defer df.Release()

	// Forward-fill nulls - a common time-series cleanup.
	ff, err := lazy.FromDataFrame(df).ForwardFill(0).Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer ff.Release()
	fmt.Println("--- ForwardFill ---")
	fmt.Println(ff)

	// Limit to two consecutive fills after each value.
	ff2, _ := lazy.FromDataFrame(df).ForwardFill(2).Collect(ctx)
	defer ff2.Release()
	fmt.Println("--- ForwardFill(limit=2) ---")
	fmt.Println(ff2)

	// FillNan for floats.
	f, _ := series.FromFloat64("x",
		[]float64{1, math.NaN(), 3, math.NaN(), 5},
		nil)
	fdf, _ := dataframe.New(f)
	defer fdf.Release()
	nanClean, _ := lazy.FromDataFrame(fdf).FillNan(-1).Collect(ctx)
	defer nanClean.Release()
	fmt.Println("--- FillNan(-1) ---")
	fmt.Println(nanClean)
}
