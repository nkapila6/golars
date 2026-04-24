package golars_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Gaurav-Gosain/golars"
)

// TestTopLevelFacade sanity-checks that the one-line-import ergonomic
// actually compiles and runs: ReadCSV + Lazy pipeline + Collect.
func TestTopLevelFacade(t *testing.T) {
	// Write a small CSV fixture.
	dir := t.TempDir()
	path := filepath.Join(dir, "trades.csv")
	content := "symbol,qty,price\nAAPL,10,150.0\nMSFT,5,300.0\nAAPL,7,155.0\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	df, err := golars.ReadCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if df.Height() != 3 || df.Width() != 3 {
		t.Fatalf("shape = (%d,%d), want (3,3)", df.Height(), df.Width())
	}

	out, err := golars.Lazy(df).
		WithColumns(golars.Col("qty").Mul(golars.Col("price")).Alias("total")).
		GroupBy("symbol").
		Agg(golars.Sum("total")).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 || out.Width() != 2 {
		t.Fatalf("agg shape = (%d,%d), want (2,2)", out.Height(), out.Width())
	}
}

// TestMathPipeline exercises the new Expr.Abs / .Sqrt / .Round
// wiring via the lazy executor.
func TestMathPipeline(t *testing.T) {
	df, _ := golars.FromMap(map[string]any{
		"x": []float64{-4, -1, 0, 1, 16},
	}, []string{"x"})
	defer df.Release()

	out, err := golars.Lazy(df).
		Select(
			golars.Col("x").Abs().Alias("ax"),
			golars.Col("x").Abs().Sqrt().Alias("sx"),
		).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Width() != 2 {
		t.Fatalf("width = %d, want 2", out.Width())
	}
}
