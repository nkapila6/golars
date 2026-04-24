// Command gen writes a tiny synthetic lineitem.parquet with just
// enough columns and row count for local Q1/Q6 development.
//
// This is not a substitute for tpchgen-cli - referential integrity
// with orders/customer isn't enforced, and the value distributions
// are uniform noise. Good for "does the query compile and run",
// not for "is this faster than polars".
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	var (
		rows = flag.Int("rows", 100_000, "rows in lineitem.parquet")
		out  = flag.String("out", "/tmp/pdsh", "output directory")
		seed = flag.Int64("seed", 1, "PCG seed")
	)
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail("mkdir: %v", err)
	}
	r := rand.New(rand.NewPCG(uint64(*seed), 0x9e3779b97f4a7c15))
	n := *rows

	retFlags := []string{"A", "N", "R"}
	lineStatuses := []string{"O", "F"}

	returnflag := make([]string, n)
	linestatus := make([]string, n)
	qty := make([]float64, n)
	price := make([]float64, n)
	disc := make([]float64, n)
	tax := make([]float64, n)
	for i := 0; i < n; i++ {
		returnflag[i] = retFlags[r.IntN(len(retFlags))]
		linestatus[i] = lineStatuses[r.IntN(len(lineStatuses))]
		qty[i] = float64(1 + r.IntN(50))
		price[i] = 900 + r.Float64()*90_000 // rough TPC-H-ish range
		disc[i] = float64(r.IntN(11)) / 100 // 0.00..0.10
		tax[i] = float64(r.IntN(9)) / 100   // 0.00..0.08
	}

	cols := make([]*series.Series, 0, 6)
	mustBuild := func(fn func() (*series.Series, error)) {
		s, err := fn()
		if err != nil {
			fail("build column: %v", err)
		}
		cols = append(cols, s)
	}
	mustBuild(func() (*series.Series, error) { return series.FromString("l_returnflag", returnflag, nil) })
	mustBuild(func() (*series.Series, error) { return series.FromString("l_linestatus", linestatus, nil) })
	mustBuild(func() (*series.Series, error) { return series.FromFloat64("l_quantity", qty, nil) })
	mustBuild(func() (*series.Series, error) { return series.FromFloat64("l_extendedprice", price, nil) })
	mustBuild(func() (*series.Series, error) { return series.FromFloat64("l_discount", disc, nil) })
	mustBuild(func() (*series.Series, error) { return series.FromFloat64("l_tax", tax, nil) })

	df, err := dataframe.New(cols...)
	if err != nil {
		fail("dataframe: %v", err)
	}
	defer df.Release()

	path := filepath.Join(*out, "lineitem.parquet")
	if err := parquet.WriteFile(context.Background(), path, df); err != nil {
		fail("write: %v", err)
	}
	fmt.Printf("wrote %s (%d rows × %d cols)\n", path, df.Height(), df.Width())
}

func fail(f string, args ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", args...)
	os.Exit(1)
}
