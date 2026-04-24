package stream_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
	"github.com/Gaurav-Gosain/golars/stream"
)

func buildSource(t *testing.T, mem interface {
	Allocate(size int) []byte
	Reallocate(size int, b []byte) []byte
	Free(b []byte)
}, n int) *dataframe.DataFrame {
	t.Helper()
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i)
	}
	a, _ := series.FromInt64("a", vals, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	return df
}

func TestPipelineFilterProjectCollect(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	const n = 1 << 12
	src := buildSource(t, mem, n)
	defer src.Release()

	cfg := stream.Config{
		MorselRows:    256,
		ChannelBuffer: 4,
		Allocator:     mem,
	}

	pipeline := stream.New(cfg,
		stream.DataFrameSource(src, cfg),
		[]stream.Stage{
			stream.FilterStage(cfg, expr.Col("a").GtLit(int64(n/2))),
			stream.ProjectStage(cfg, []expr.Expr{
				expr.Col("a").MulLit(int64(2)).Alias("b"),
			}),
		},
		stream.CollectSink(cfg),
	)

	out, err := pipeline.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	defer out.Release()

	// Expected rows: a in (n/2, n), i.e. n/2 rows. b = 2*a.
	wantRows := n/2 - 1
	if out.Width() != 1 {
		t.Errorf("Width = %d, want 1", out.Width())
	}
	if out.Height() != wantRows {
		t.Errorf("Height = %d, want %d", out.Height(), wantRows)
	}
	b, _ := out.Column("b")
	if !b.DType().IsInteger() {
		t.Errorf("b dtype = %s, want integer", b.DType())
	}

	// Spot-check first and last.
	chunks := b.Chunks()
	var all []int64
	for _, ch := range chunks {
		all = append(all, ch.(*array.Int64).Int64Values()...)
	}
	if len(all) > 0 {
		if all[0] != int64(2*(n/2+1)) {
			t.Errorf("first b = %d, want %d", all[0], 2*(n/2+1))
		}
		if all[len(all)-1] != int64(2*(n-1)) {
			t.Errorf("last b = %d, want %d", all[len(all)-1], 2*(n-1))
		}
	}
}

func TestPipelineCancellation(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	src := buildSource(t, mem, 1<<14)
	defer src.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	cfg := stream.Config{MorselRows: 256, ChannelBuffer: 1, Allocator: mem}
	pipeline := stream.New(cfg,
		stream.DataFrameSource(src, cfg),
		[]stream.Stage{
			stream.FilterStage(cfg, expr.Col("a").GtLit(int64(0))),
		},
		stream.CollectSink(cfg),
	)

	_, err := pipeline.Run(ctx)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled, got %v", err)
	}
}

func TestPipelineMorselBoundariesDoNotAffectResult(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	const n = 10000
	src := buildSource(t, mem, n)
	defer src.Release()

	// Two pipelines, same inputs, different morsel sizes; results must be
	// identical.
	run := func(rows int) *dataframe.DataFrame {
		cfg := stream.Config{MorselRows: rows, ChannelBuffer: 4, Allocator: mem}
		p := stream.New(cfg,
			stream.DataFrameSource(src, cfg),
			[]stream.Stage{
				stream.FilterStage(cfg, expr.Col("a").GtLit(int64(1000)).And(expr.Col("a").LtLit(int64(9000)))),
			},
			stream.CollectSink(cfg),
		)
		out, err := p.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		return out
	}

	a := run(128)
	defer a.Release()
	b := run(1024)
	defer b.Release()
	c := run(50_000) // single morsel
	defer c.Release()

	if a.Height() != b.Height() || a.Height() != c.Height() {
		t.Errorf("heights differ: %d, %d, %d", a.Height(), b.Height(), c.Height())
	}
}

func BenchmarkStreamingFilterProject(b *testing.B) {
	ctx := context.Background()
	const n = 1 << 18
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i)
	}
	src, _ := dataframe.New(mustSeries("a", vals))
	defer src.Release()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	for b.Loop() {
		cfg := stream.DefaultConfig()
		cfg.MorselRows = 64 * 1024
		p := stream.New(cfg,
			stream.DataFrameSource(src, cfg),
			[]stream.Stage{
				stream.FilterStage(cfg, expr.Col("a").GtLit(int64(0))),
				stream.ProjectStage(cfg, []expr.Expr{expr.Col("a").MulLit(int64(3)).Alias("b")}),
			},
			stream.CollectSink(cfg),
		)
		out, err := p.Run(ctx)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func mustSeries(name string, v []int64) *series.Series {
	s, err := series.FromInt64(name, v, nil)
	if err != nil {
		panic(err)
	}
	return s
}
