package stream_test

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
	"github.com/Gaurav-Gosain/golars/stream"
)

func TestParallelFilterMatchesSerial(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	const n = 20_000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i)
	}
	s, _ := series.FromInt64("a", vals, nil, series.WithAllocator(mem))
	src, _ := dataframe.New(s)
	defer src.Release()

	cfg := stream.Config{MorselRows: 500, ChannelBuffer: 4, Allocator: mem}

	run := func(stage stream.Stage) *dataframe.DataFrame {
		p := stream.New(cfg,
			stream.DataFrameSource(src, cfg),
			[]stream.Stage{stage},
			stream.CollectSink(cfg),
		)
		out, err := p.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	serial := run(stream.FilterStage(cfg, expr.Col("a").GtLit(int64(5_000))))
	defer serial.Release()
	par := run(stream.ParallelFilterStage(cfg, expr.Col("a").GtLit(int64(5_000)), 4))
	defer par.Release()

	if serial.Height() != par.Height() {
		t.Errorf("heights differ: serial=%d parallel=%d",
			serial.Height(), par.Height())
	}

	// Compare row by row to confirm order preservation.
	ser, _ := serial.Column("a")
	parC, _ := par.Column("a")

	flatten := func(s *series.Series) []int64 {
		var out []int64
		for _, c := range s.Chunks() {
			out = append(out, int64ChunkValues(c)...)
		}
		return out
	}
	s1 := flatten(ser)
	s2 := flatten(parC)
	if len(s1) != len(s2) {
		t.Fatalf("flattened lengths differ: %d vs %d", len(s1), len(s2))
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			t.Errorf("row %d: serial=%d parallel=%d", i, s1[i], s2[i])
			return
		}
	}
}

func TestParallelProjectMatchesSerial(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	const n = 5_000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i)
	}
	s, _ := series.FromInt64("a", vals, nil, series.WithAllocator(mem))
	src, _ := dataframe.New(s)
	defer src.Release()

	cfg := stream.Config{MorselRows: 256, ChannelBuffer: 2, Allocator: mem}

	p := stream.New(cfg,
		stream.DataFrameSource(src, cfg),
		[]stream.Stage{
			stream.ParallelProjectStage(cfg,
				[]expr.Expr{expr.Col("a").MulLit(int64(2)).Alias("b")},
				runtime.GOMAXPROCS(0)),
		},
		stream.CollectSink(cfg),
	)
	out, err := p.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Height() != n {
		t.Fatalf("Height = %d, want %d", out.Height(), n)
	}
	b, _ := out.Column("b")
	for c := 0; c < b.NumChunks(); c++ {
		values := int64ChunkValues(b.Chunk(c))
		offset := 0
		for ci := 0; ci < c; ci++ {
			offset += b.Chunk(ci).Len()
		}
		for i, v := range values {
			want := int64((offset + i) * 2)
			if v != want {
				t.Errorf("b[%d] = %d, want %d", offset+i, v, want)
				return
			}
		}
	}
}

func TestParallelFilterCancellation(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	const n = 50_000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i)
	}
	s, _ := series.FromInt64("a", vals, nil, series.WithAllocator(mem))
	src, _ := dataframe.New(s)
	defer src.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := stream.Config{MorselRows: 100, ChannelBuffer: 1, Allocator: mem}
	p := stream.New(cfg,
		stream.DataFrameSource(src, cfg),
		[]stream.Stage{
			stream.ParallelFilterStage(cfg, expr.Col("a").GtLit(int64(0)), 4),
		},
		stream.CollectSink(cfg),
	)
	_, err := p.Run(ctx)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled, got %v", err)
	}
}

func int64ChunkValues(a interface {
	Len() int
	IsValid(int) bool
}) []int64 {
	type int64Vals interface {
		Int64Values() []int64
	}
	if v, ok := a.(int64Vals); ok {
		raw := v.Int64Values()
		out := make([]int64, len(raw))
		copy(out, raw)
		return out
	}
	return nil
}
