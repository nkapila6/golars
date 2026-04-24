// Attach a profiler to a lazy plan and print per-node timings.
// Run: go run ./examples/profiler
// Typed variant: go run ./examples/profiler/generic
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

	// Synthetic dataset.
	const n = 50_000
	keys := make([]int64, n)
	vals := make([]int64, n)
	for i := range keys {
		keys[i] = int64(i % 16)
		vals[i] = int64(i)
	}
	k, _ := series.FromInt64("k", keys, nil)
	v, _ := series.FromInt64("v", vals, nil)
	df, _ := dataframe.New(k, v)
	defer df.Release()

	p := lazy.NewProfiler()
	out, err := lazy.FromDataFrame(df).
		Filter(expr.Col("v").Gt(expr.Lit(int64(10)))).
		GroupBy("k").
		Agg(expr.Col("v").Sum().Alias("total")).
		Sort("total", true).
		Limit(5).
		Collect(ctx, lazy.WithProfiler(p))
	if err != nil {
		panic(err)
	}
	defer out.Release()
	fmt.Println(out)
	fmt.Println()
	fmt.Print(p.Report())
}
