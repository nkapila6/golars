package ipc

import (
	"context"
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowipc "github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// StreamWriter emits an Arrow IPC stream one DataFrame (record batch)
// at a time. Use it when the full frame doesn't fit in memory or you
// want to pipe batches to another process. Caller must Close at end.
type StreamWriter struct {
	writer *arrowipc.Writer
	schema *arrow.Schema
	cfg    config
}

// NewStreamWriter wraps w into a streaming IPC writer. The schema is
// derived from schemaFrame's columns; subsequent Write calls must
// produce frames with the same schema.
func NewStreamWriter(w io.Writer, schemaFrame *dataframe.DataFrame, opts ...Option) (*StreamWriter, error) {
	cfg := resolve(opts)
	sch := schemaFrame.Schema().ToArrow()
	writer := arrowipc.NewWriter(w,
		arrowipc.WithSchema(sch),
		arrowipc.WithAllocator(cfg.alloc),
	)
	return &StreamWriter{writer: writer, schema: sch, cfg: cfg}, nil
}

// Write appends df as a new record batch.
func (sw *StreamWriter) Write(ctx context.Context, df *dataframe.DataFrame) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cols := make([]arrow.Array, df.Width())
	releaseAll := func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}
	for i := range df.Width() {
		s := df.ColumnAt(i)
		arr, err := concatChunks(s, sw.cfg.alloc)
		if err != nil {
			releaseAll()
			return fmt.Errorf("ipc.StreamWriter: column %q: %w", s.Name(), err)
		}
		cols[i] = arr
	}
	rec := array.NewRecordBatch(sw.schema, cols, int64(df.Height()))
	defer rec.Release()
	releaseAll()
	if err := sw.writer.Write(rec); err != nil {
		return fmt.Errorf("ipc.StreamWriter: write: %w", err)
	}
	return nil
}

// Close flushes the footer and closes the underlying writer.
func (sw *StreamWriter) Close() error { return sw.writer.Close() }

// StreamReader yields record batches as DataFrames. Callers must
// Release each yielded DataFrame. Iteration stops on EOF or error
// (check the error companion at end).
type StreamReader struct {
	reader *arrowipc.Reader
	cfg    config
	err    error
}

// NewStreamReader wraps r into a streaming IPC reader.
func NewStreamReader(r io.Reader, opts ...Option) (*StreamReader, error) {
	cfg := resolve(opts)
	reader, err := arrowipc.NewReader(r, arrowipc.WithAllocator(cfg.alloc))
	if err != nil {
		return nil, fmt.Errorf("ipc.StreamReader: %w", err)
	}
	return &StreamReader{reader: reader, cfg: cfg}, nil
}

// Iter returns an iter.Seq2 over (*DataFrame, error) pairs. The same
// DataFrame handle is valid only for the current iteration; call
// Release before advancing the loop.
func (sr *StreamReader) Iter(ctx context.Context) iter.Seq2[*dataframe.DataFrame, error] {
	return func(yield func(*dataframe.DataFrame, error) bool) {
		for sr.reader.Next() {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}
			rec := sr.reader.RecordBatch()
			df, err := recordToDataFrame(rec)
			if !yield(df, err) {
				return
			}
		}
		if err := sr.reader.Err(); err != nil {
			sr.err = err
			yield(nil, err)
		}
	}
}

// Schema returns the stream's arrow schema.
func (sr *StreamReader) Schema() *arrow.Schema { return sr.reader.Schema() }

// Close releases the underlying arrow reader.
func (sr *StreamReader) Close() { sr.reader.Release() }

// WriteStreamFile opens path for writing and writes every frame in
// frames as a separate record batch. All frames must share a schema.
func WriteStreamFile(ctx context.Context, path string, frames []*dataframe.DataFrame, opts ...Option) error {
	if len(frames) == 0 {
		return fmt.Errorf("ipc.WriteStreamFile: at least one frame required")
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("ipc.WriteStreamFile: create %q: %w", path, err)
	}
	sw, err := NewStreamWriter(f, frames[0], opts...)
	if err != nil {
		f.Close()
		return err
	}
	for _, d := range frames {
		if err := sw.Write(ctx, d); err != nil {
			sw.Close()
			f.Close()
			return err
		}
	}
	if err := sw.Close(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// recordToDataFrame builds a DataFrame from a single arrow.RecordBatch
// (retains columns; caller owns the result).
func recordToDataFrame(rec arrow.RecordBatch) (*dataframe.DataFrame, error) {
	// Wrap each column as a single-chunk Series via the existing
	// IPC Read path's helpers.
	sch := rec.Schema()
	numCols := sch.NumFields()
	chunks := make([][]arrow.Array, numCols)
	for i := range numCols {
		col := rec.Column(i)
		col.Retain()
		chunks[i] = []arrow.Array{col}
	}
	return buildDataFrameFromChunks(sch, chunks)
}
