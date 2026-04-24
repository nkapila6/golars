// Build a DataFrame from slices, then take the head, filter, and sort.
// Run: go run ./examples/basic
package main

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	names, _ := series.FromString("name", []string{"ada", "brian", "carl", "dana", "ed"}, nil)
	ages, _ := series.FromInt64("age", []int64{27, 34, 19, 42, 28}, nil)
	scores, _ := series.FromFloat64("score", []float64{9.1, 7.3, 6.4, 8.8, 5.5}, nil)

	df, err := dataframe.New(names, ages, scores)
	if err != nil {
		panic(err)
	}
	defer df.Release()
	fmt.Println("full frame:")
	fmt.Println(df)

	head := df.Head(3)
	defer head.Release()
	fmt.Println("head(3):")
	fmt.Println(head)

	sorted, err := df.SortBy(ctx, []string{"age"}, []compute.SortOptions{{}})
	if err != nil {
		panic(err)
	}
	defer sorted.Release()
	fmt.Println("sorted by age:")
	fmt.Println(sorted)

	// Filter: rows where score > 7. GtLit compares directly against a
	// scalar so we don't pay the cost of broadcasting the threshold into
	// a full-length Series (mirrors polars' `pl.col("score") > 7`).
	mask, _ := compute.GtLit(ctx, scores, 7.0)
	defer mask.Release()
	ageOnly, _ := df.Filter(ctx, mask)
	defer ageOnly.Release()
	fmt.Println("rows where score > 7:")
	fmt.Println(ageOnly)
}
