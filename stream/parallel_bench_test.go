package stream_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
	"github.com/Gaurav-Gosain/golars/stream"
)

func buildBenchSource(n int) *dataframe.DataFrame {
	a := make([]int64, n)
	b := make([]int64, n)
	for i := range a {
		a[i] = int64(i)
		b[i] = int64(i * 3)
	}
	sa, _ := series.FromInt64("a", a, nil)
	sb, _ := series.FromInt64("b", b, nil)
	df, _ := dataframe.New(sa, sb)
	return df
}

func BenchmarkSerialVsParallelFilter(b *testing.B) {
	ctx := context.Background()
	const n = 1 << 20

	src := buildBenchSource(n)
	defer src.Release()

	for _, workers := range []int{1, 2, 4, 8} {
		name := fmt.Sprintf("workers=%d", workers)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n) * 16)
			for b.Loop() {
				cfg := stream.DefaultConfig()
				cfg.MorselRows = 16 * 1024
				cfg.ChannelBuffer = 4
				stages := []stream.Stage{
					stream.ParallelFilterStage(cfg,
						expr.Col("a").GtLit(int64(n/2)), workers),
					stream.ParallelProjectStage(cfg,
						[]expr.Expr{expr.Col("a").Add(expr.Col("b")).Alias("s")},
						workers),
				}
				p := stream.New(cfg, stream.DataFrameSource(src, cfg),
					stages, stream.CollectSink(cfg))
				out, err := p.Run(ctx)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}
