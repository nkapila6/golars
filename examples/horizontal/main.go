// Row-wise aggregates: SumHorizontal, MeanHorizontal, MinHorizontal.
// Run: go run ./examples/horizontal
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	q1, _ := series.FromInt64("q1", []int64{10, 20, 30}, nil)
	q2, _ := series.FromInt64("q2", []int64{15, 25, 35}, nil)
	q3, _ := series.FromInt64("q3", []int64{20, 30, 40}, nil)
	q4, _ := series.FromInt64("q4", []int64{25, 35, 45}, nil)
	df, _ := dataframe.New(q1, q2, q3, q4)
	defer df.Release()

	total, _ := df.SumHorizontal(ctx, dataframe.IgnoreNulls)
	defer total.Release()
	mean, _ := df.MeanHorizontal(ctx, dataframe.IgnoreNulls)
	defer mean.Release()

	out, _ := dataframe.New(q1.Clone(), q2.Clone(), q3.Clone(), q4.Clone(),
		total.Rename("total"), mean.Rename("avg"))
	defer out.Release()
	fmt.Println(out)

	// Frame-level collapse (every numeric column → one row of sums).
	sums, err := df.SumAll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer sums.Release()
	fmt.Println("per-column totals:")
	fmt.Println(sums)
}
