// Lazy scan with predicate + projection pushdown.
// Run: go run ./examples/scan_pushdown
// Typed variant: go run ./examples/scan_pushdown/generic
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

	// Seed a tiny CSV file so the example is self-contained.
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

	// Scan the file lazily; optimiser pushes projection and predicate
	// into the scan so only the two requested columns are read and
	// non-us rows are discarded before they hit memory.
	lf := iocsv.Scan(path).
		Filter(expr.Col("region").Eq(expr.Lit("us"))).
		Select(expr.Col("symbol"), expr.Col("price"))

	plan, _ := lf.Explain()
	fmt.Println(plan)

	out, err := lf.Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Release()
	fmt.Println(out)
}
