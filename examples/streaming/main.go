// Run a pipeline through the streaming/morsel engine.
// Run: go run ./examples/streaming
// Typed variant: go run ./examples/streaming/generic
package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
	"github.com/Gaurav-Gosain/golars/stream"
)

func main() {
	ctx := context.Background()
	alloc := memory.DefaultAllocator

	// Build a 100K-row source.
	const n = 100_000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i)
	}
	s, _ := series.FromInt64("a", vals, nil)
	df, _ := dataframe.New(s)
	defer df.Release()

	cfg := stream.Config{MorselRows: 4096, ChannelBuffer: 4, Allocator: alloc}

	// filter(a > 50000) → project(b = a * 2)
	pipeline := stream.New(cfg,
		stream.DataFrameSource(df, cfg),
		[]stream.Stage{
			stream.FilterStage(cfg, expr.Col("a").GtLit(int64(50_000))),
			stream.ProjectStage(cfg, []expr.Expr{
				expr.Col("a").MulLit(int64(2)).Alias("b"),
			}),
		},
		stream.CollectSink(cfg),
	)

	out, err := pipeline.Run(ctx)
	if err != nil {
		panic(err)
	}
	defer out.Release()
	fmt.Printf("streamed %d rows through filter→project\n", out.Height())
	fmt.Println(out)
}
