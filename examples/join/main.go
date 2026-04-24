// Inner and left joins across two DataFrames.
// Run: go run ./examples/join
package main

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	id, _ := series.FromInt64("id", []int64{1, 2, 3, 4}, nil)
	name, _ := series.FromString("name", []string{"ada", "brian", "carl", "dana"}, nil)
	people, _ := dataframe.New(id, name)
	defer people.Release()

	fid, _ := series.FromInt64("id", []int64{1, 2, 4, 5}, nil)
	score, _ := series.FromFloat64("score", []float64{9.1, 7.3, 8.8, 5.5}, nil)
	scores, _ := dataframe.New(fid, score)
	defer scores.Release()

	inner, _ := people.Join(ctx, scores, []string{"id"}, dataframe.InnerJoin)
	defer inner.Release()
	fmt.Println("inner join on id:")
	fmt.Println(inner)

	left, _ := people.Join(ctx, scores, []string{"id"}, dataframe.LeftJoin)
	defer left.Release()
	fmt.Println("left join on id:")
	fmt.Println(left)
}
