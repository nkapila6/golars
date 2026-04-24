// Typed-column variant of ./examples/profiler.
// Run: go run ./examples/profiler/generic
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

	val := expr.Int("v")

	p := lazy.NewProfiler()
	out, err := lazy.FromDataFrame(df).
		Filter(val.Gt(10)).
		GroupBy("k").
		Agg(val.Sum().As("total").Expr).
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
