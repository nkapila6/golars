// Coalesce + ConcatStr + IntRange - polars-style constructors.
// Run: go run ./examples/coalesce_concat
// Typed variant: go run ./examples/coalesce_concat/generic
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

	primary, _ := series.FromString("primary",
		[]string{"ada", "", "", "dan"},
		[]bool{true, false, false, true})
	backup, _ := series.FromString("backup",
		[]string{"", "brian", "", "don"},
		[]bool{false, true, false, true})
	fallback, _ := series.FromString("fallback",
		[]string{"guest", "guest", "guest", "guest"}, nil)
	df, _ := dataframe.New(primary, backup, fallback)
	defer df.Release()

	// Pick the first non-null name per row.
	out, err := lazy.FromDataFrame(df).
		Select(
			expr.Coalesce(
				expr.Col("primary"),
				expr.Col("backup"),
				expr.Col("fallback"),
			).Alias("name"),
			// Row-wise concat: "ada|brian|guest".
			expr.ConcatStr("|",
				expr.Col("primary"),
				expr.Col("backup"),
				expr.Col("fallback"),
			).Alias("combined"),
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Release()
	fmt.Println(out)

	// A 0..10 range as a standalone Series.
	rangeDF, _ := lazy.FromDataFrame(df).
		Select(expr.IntRange(0, 5, 1).Alias("idx")).
		Collect(ctx)
	defer rangeDF.Release()
	fmt.Println(rangeDF)
}
