package golars_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars"
)

func TestSelectExpr(t *testing.T) {
	ctx := context.Background()
	a, _ := golars.FromInt64("a", []int64{1, 2, 3}, nil)
	b, _ := golars.FromInt64("b", []int64{10, 20, 30}, nil)
	df, _ := golars.NewDataFrame(a, b)
	defer df.Release()

	out, err := golars.SelectExpr(ctx, df,
		golars.Col("a").Add(golars.Col("b")).Alias("sum"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Width() != 1 || out.Height() != 3 {
		t.Fatalf("shape = %dx%d want 1x3", out.Width(), out.Height())
	}
}

func TestWithColumnsExpr(t *testing.T) {
	ctx := context.Background()
	a, _ := golars.FromInt64("a", []int64{1, 2, 3}, nil)
	df, _ := golars.NewDataFrame(a)
	defer df.Release()

	out, err := golars.WithColumnsExpr(ctx, df,
		golars.Col("a").MulLit(int64(2)).Alias("a2"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Width() != 2 {
		t.Fatalf("width = %d want 2", out.Width())
	}
}
