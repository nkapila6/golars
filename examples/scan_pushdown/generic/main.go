// Typed-column variant of ./examples/scan_pushdown.
// Run: go run ./examples/scan_pushdown/generic
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	path := filepath.Join(os.TempDir(), "golars-scan-pushdown.csv")
	sym, _ := series.FromString("symbol", []string{"AAA", "BBB", "CCC", "AAA", "BBB"}, nil)
	region, _ := series.FromString("region", []string{"us", "eu", "us", "us", "asia"}, nil)
	price, _ := series.FromFloat64("price", []float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
	vol, _ := series.FromInt64("volume", []int64{100, 200, 300, 50, 400}, nil)
	src, _ := dataframe.New(sym, region, price, vol)
	if err := iocsv.WriteFile(ctx, path, src); err != nil {
		log.Fatal(err)
	}
	src.Release()

	reg := expr.Str("region")

	lf := iocsv.Scan(path).
		Filter(reg.Eq("us")).
		Select(expr.Str("symbol").Expr, expr.Float("price").Expr)

	plan, _ := lf.Explain()
	fmt.Println(plan)

	out, err := lf.Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Release()
	fmt.Println(out)
}
