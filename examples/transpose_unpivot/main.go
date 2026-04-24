// Transpose and Unpivot (melt).
// Run: go run ./examples/transpose_unpivot
package main

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	// Per-week sales for two products.
	id, _ := series.FromString("id", []string{"A", "B"}, nil)
	wk1, _ := series.FromInt64("wk1", []int64{10, 20}, nil)
	wk2, _ := series.FromInt64("wk2", []int64{15, 25}, nil)
	wk3, _ := series.FromInt64("wk3", []int64{12, 22}, nil)
	df, _ := dataframe.New(id, wk1, wk2, wk3)
	defer df.Release()

	// Transpose: each column becomes a row. Transpose requires
	// numeric/bool columns, so drop the id column first.
	numeric := df.Drop("id")
	defer numeric.Release()
	t, err := numeric.Transpose(ctx, "week", "value")
	if err != nil {
		panic(err)
	}
	defer t.Release()
	fmt.Println("--- Transpose ---")
	fmt.Println(t)

	// Unpivot (melt): wide → long.
	long, _ := df.Unpivot(ctx, []string{"id"}, nil)
	defer long.Release()
	fmt.Println("--- Unpivot (melt) ---")
	fmt.Println(long)
}
