// TopK / BottomK / Pipe - nicer alternatives to Sort+Head.
// Run: go run ./examples/topk_pipe
package main

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	sym, _ := series.FromString("symbol", []string{"AAA", "BBB", "CCC", "DDD", "EEE"}, nil)
	vol, _ := series.FromInt64("volume", []int64{300, 100, 500, 200, 400}, nil)
	df, _ := dataframe.New(sym, vol)
	defer df.Release()

	top3, _ := df.TopK(ctx, 3, "volume")
	defer top3.Release()
	fmt.Println("--- TopK(3, volume) ---")
	fmt.Println(top3)

	bottom2, _ := df.BottomK(ctx, 2, "volume")
	defer bottom2.Release()
	fmt.Println("--- BottomK(2, volume) ---")
	fmt.Println(bottom2)

	// Pipe chains arbitrary transformations into the method chain.
	// Here we TopK twice to keep just the #2 row.
	out, _ := df.Pipe(func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		top2, err := d.TopK(ctx, 2, "volume")
		if err != nil {
			return nil, err
		}
		defer top2.Release()
		return top2.BottomK(ctx, 1, "volume")
	})
	defer out.Release()
	fmt.Println("--- second-highest via Pipe ---")
	fmt.Println(out)
	// Silence "imported and not used" when expr is only referenced in
	// a future addition.
	_ = expr.Col
}
