// Package stream provides the morsel-driven streaming executor.
//
// The executor chops input DataFrames into bounded morsels (default 64K
// rows) and pipes them through operator stages over buffered channels. Each
// stage runs in its own goroutine; buffered channels between stages provide
// back-pressure. Cancellation is observed on context.
//
// The streaming executor is an alternative backend for the lazy engine. It
// is useful for pipelines that do not fit comfortably in memory, or where
// the overlap between I/O and compute matters (e.g. reading parquet while
// filtering/aggregating).
package stream

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// DefaultMorselRows is the default upper bound on rows per morsel. Chosen to
// amortize per-morsel scheduling cost while keeping memory bounded.
const DefaultMorselRows = 64 * 1024

// Morsel is the unit of data moving between stages: a small DataFrame with a
// bounded row count. A Morsel owns its underlying Series; consumers Release
// when done.
type Morsel struct {
	DF *dataframe.DataFrame
}

// Release drops the Morsel's reference to its data.
func (m Morsel) Release() {
	if m.DF != nil {
		m.DF.Release()
	}
}

// Source yields morsels on the output channel and returns when the input is
// exhausted or when ctx is cancelled. Implementations must close out on
// return and must not close out more than once.
type Source func(ctx context.Context, out chan<- Morsel) error

// Stage reads morsels from in, transforms them, and writes to out. It
// closes out on return. Dropped morsels must be Released.
type Stage func(ctx context.Context, in <-chan Morsel, out chan<- Morsel) error

// Sink consumes morsels and returns a single collected DataFrame.
type Sink func(ctx context.Context, in <-chan Morsel) (*dataframe.DataFrame, error)

// Config tunes the pipeline.
type Config struct {
	// MorselRows caps rows per morsel; zero uses DefaultMorselRows.
	MorselRows int
	// ChannelBuffer is the buffer size of inter-stage channels. A small
	// buffer propagates back-pressure quickly; a large one decouples stages
	// at the cost of memory.
	ChannelBuffer int
	// Allocator is used for output morsels and operator scratch.
	Allocator memory.Allocator
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		MorselRows:    DefaultMorselRows,
		ChannelBuffer: 4,
		Allocator:     memory.DefaultAllocator,
	}
}

// Pipeline connects a Source, zero or more Stages, and a Sink.
type Pipeline struct {
	cfg    Config
	source Source
	stages []Stage
	sink   Sink
}

// New creates a Pipeline. Stages are invoked in the order given.
func New(cfg Config, source Source, stages []Stage, sink Sink) *Pipeline {
	if cfg.MorselRows <= 0 {
		cfg.MorselRows = DefaultMorselRows
	}
	if cfg.ChannelBuffer <= 0 {
		cfg.ChannelBuffer = 4
	}
	if cfg.Allocator == nil {
		cfg.Allocator = memory.DefaultAllocator
	}
	return &Pipeline{cfg: cfg, source: source, stages: stages, sink: sink}
}

// Run executes the pipeline end-to-end. Returns the Sink's collected
// DataFrame or the first error observed by any stage.
func (p *Pipeline) Run(ctx context.Context) (*dataframe.DataFrame, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2+len(p.stages))

	// Build channels: source -> stage0 -> stage1 -> ... -> sink.
	chans := make([]chan Morsel, len(p.stages)+1)
	for i := range chans {
		chans[i] = make(chan Morsel, p.cfg.ChannelBuffer)
	}

	// Launch source.
	go func() {
		defer close(chans[0])
		if err := p.source(ctx, chans[0]); err != nil {
			errCh <- fmt.Errorf("source: %w", err)
			cancel()
			return
		}
		errCh <- nil
	}()

	// Launch stages.
	for i, s := range p.stages {
		in, out := chans[i], chans[i+1]
		stage := s
		idx := i
		go func() {
			defer close(out)
			if err := stage(ctx, in, out); err != nil {
				errCh <- fmt.Errorf("stage %d: %w", idx, err)
				cancel()
				return
			}
			errCh <- nil
		}()
	}

	// Run sink on the caller's goroutine (simpler lifetime).
	result, sinkErr := p.sink(ctx, chans[len(chans)-1])

	// Collect non-sink goroutine errors.
	var firstErr error
	nGoroutines := 1 + len(p.stages)
	for range nGoroutines {
		if e := <-errCh; e != nil && firstErr == nil {
			firstErr = e
		}
	}
	if firstErr != nil {
		if result != nil {
			result.Release()
		}
		return nil, firstErr
	}
	if sinkErr != nil {
		if result != nil {
			result.Release()
		}
		return nil, fmt.Errorf("sink: %w", sinkErr)
	}
	return result, nil
}

// DataFrameSource chops df into morsels of at most cfg.MorselRows rows.
// The source retains the input df's buffers through slice; callers own df.
func DataFrameSource(df *dataframe.DataFrame, cfg Config) Source {
	rows := cfg.MorselRows
	if rows <= 0 {
		rows = DefaultMorselRows
	}
	return func(ctx context.Context, out chan<- Morsel) error {
		height := df.Height()
		for start := 0; start < height; start += rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			length := rows
			if start+length > height {
				length = height - start
			}
			sl, err := df.Slice(start, length)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				sl.Release()
				return ctx.Err()
			case out <- Morsel{DF: sl}:
			}
		}
		return nil
	}
}

// CollectSink concatenates every incoming morsel into a single DataFrame.
func CollectSink(cfg Config) Sink {
	return func(ctx context.Context, in <-chan Morsel) (*dataframe.DataFrame, error) {
		var batches []*dataframe.DataFrame
		cleanup := func() {
			for _, b := range batches {
				b.Release()
			}
		}
		for {
			select {
			case <-ctx.Done():
				cleanup()
				return nil, ctx.Err()
			case m, ok := <-in:
				if !ok {
					return concatDataFrames(batches, cfg.Allocator)
				}
				batches = append(batches, m.DF)
			}
		}
	}
}

// concatDataFrames glues a slice of DataFrames into a single DataFrame by
// concatenating each column's chunks. The input DataFrames are consumed.
func concatDataFrames(batches []*dataframe.DataFrame, mem memory.Allocator) (*dataframe.DataFrame, error) {
	if len(batches) == 0 {
		empty, _ := dataframe.New()
		return empty, nil
	}
	if len(batches) == 1 {
		return batches[0], nil
	}

	sch := batches[0].Schema()
	cols := make([]*series.Series, sch.Len())
	cleanup := func(upTo int) {
		for _, c := range cols[:upTo] {
			if c != nil {
				c.Release()
			}
		}
		for _, b := range batches {
			b.Release()
		}
	}
	for ci, f := range sch.Fields() {
		var arrowChunks []arrow.Array
		for _, b := range batches {
			col, err := b.Column(f.Name)
			if err != nil {
				cleanup(ci)
				return nil, err
			}
			for _, c := range col.Chunks() {
				c.Retain()
				arrowChunks = append(arrowChunks, c)
			}
		}
		concatenated, err := array.Concatenate(arrowChunks, mem)
		for _, c := range arrowChunks {
			c.Release()
		}
		if err != nil {
			cleanup(ci)
			return nil, err
		}
		s, err := series.New(f.Name, concatenated)
		if err != nil {
			cleanup(ci)
			return nil, err
		}
		cols[ci] = s
	}
	for _, b := range batches {
		b.Release()
	}
	return dataframe.New(cols...)
}
