// Typed-column variant of ./examples/coalesce_concat.
// Coalesce and ConcatStr take variadic Exprs whose dtypes must match
// at runtime, so the typed handles are projected to untyped Exprs
// before being passed in.
//
// Run: go run ./examples/coalesce_concat/generic
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

	var (
		p = expr.Str("primary")
		b = expr.Str("backup")
		f = expr.Str("fallback")
	)

	out, err := lazy.FromDataFrame(df).
		Select(
			expr.Coalesce(p.Expr, b.Expr, f.Expr).Alias("name"),
			expr.ConcatStr("|", p.Expr, b.Expr, f.Expr).Alias("combined"),
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Release()
	fmt.Println(out)

	rangeDF, _ := lazy.FromDataFrame(df).
		Select(expr.IntRange(0, 5, 1).Alias("idx")).
		Collect(ctx)
	defer rangeDF.Release()
	fmt.Println(rangeDF)
}
