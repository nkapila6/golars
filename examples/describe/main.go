// DataFrame.Describe: summary statistics for every column.
// Run: go run ./examples/describe
package main

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	names, _ := series.FromString("name", []string{"ada", "brian", "carl", "dana", "ed"}, nil)
	ages, _ := series.FromInt64("age", []int64{27, 34, 19, 42, 28}, nil)
	scores, _ := series.FromFloat64("score",
		[]float64{9.1, 7.3, 6.4, 8.8, 5.5}, nil)
	df, _ := dataframe.New(names, ages, scores)
	defer df.Release()

	fmt.Println("Original:")
	fmt.Println(df)

	stats, err := df.Describe(ctx)
	if err != nil {
		panic(err)
	}
	defer stats.Release()
	fmt.Println("Describe:")
	fmt.Println(stats)
}
