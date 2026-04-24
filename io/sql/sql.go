package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// DefaultBatchSize caps the row count of a streaming Reader's batch.
// 8192 rows of a 10-column int64 table ≈ 640 KiB, comfortably L2-fit
// and a natural boundary for downstream SIMD kernels that prefer
// aligned power-of-two chunks.
const DefaultBatchSize = 8192

// Option configures SQL reads. Pass WithAllocator to override the
// arrow allocator used for returned columns, WithBatchSize to tune
// streaming batch size.
type Option func(*config)

type config struct {
	alloc     memory.Allocator
	batchSize int
}

func resolve(opts []Option) config {
	c := config{alloc: memory.DefaultAllocator, batchSize: DefaultBatchSize}
	for _, o := range opts {
		o(&c)
	}
	if c.batchSize <= 0 {
		c.batchSize = DefaultBatchSize
	}
	return c
}

// WithAllocator sets the arrow allocator used for result columns.
func WithAllocator(a memory.Allocator) Option { return func(c *config) { c.alloc = a } }

// WithBatchSize controls the maximum rows in a streaming batch. No
// effect on the eager ReadSQL function. Min 1; zero falls back to
// DefaultBatchSize.
func WithBatchSize(n int) Option { return func(c *config) { c.batchSize = n } }

// ReadSQL executes query on db and returns a DataFrame with all rows.
// Column dtypes are inferred from the driver's reported type names
// (INTEGER → i64, REAL/DOUBLE/NUMERIC → f64, BOOL → bool, timestamp →
// i64 unix-micros, unknown → str). Null values are preserved via the
// arrow validity bitmap.
//
// The returned DataFrame owns its buffers; call Release when done.
// For large result sets that don't fit in memory, use NewReader.
func ReadSQL(ctx context.Context, db *sql.DB, query string, args ...any) (*dataframe.DataFrame, error) {
	return ReadSQLWith(ctx, db, query, nil, args...)
}

// ReadSQLWith is ReadSQL with options.
func ReadSQLWith(ctx context.Context, db *sql.DB, query string, opts []Option, args ...any) (*dataframe.DataFrame, error) {
	r, err := NewReader(ctx, db, query, opts, args...)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.ReadAll()
}

// ReadRows reads from an existing *sql.Rows. The caller is responsible
// for closing rows.
func ReadRows(rows *sql.Rows, opts ...Option) (*dataframe.DataFrame, error) {
	r, err := newReaderFromRows(rows, false, resolve(opts))
	if err != nil {
		return nil, err
	}
	return r.ReadAll()
}

// Reader streams SQL rows as DataFrame batches. Designed for the
// arrow RecordReader idiom:
//
//	r, _ := sql.NewReader(ctx, db, "SELECT * FROM big_table", nil)
//	defer r.Close()
//	for r.Next() {
//		df := r.DataFrame()
//		process(df)
//		df.Release()
//	}
//	if err := r.Err(); err != nil { ... }
//
// The DataFrame returned by each Next-tick has at most batchSize rows.
// Column schema is stable across batches.
type Reader struct {
	rows     *sql.Rows
	cfg      config
	colProto []*sql.ColumnType
	colKinds []colKind
	colNames []string
	ownsRows bool

	cur *dataframe.DataFrame
	err error
}

// NewReader opens a streaming reader against db.
func NewReader(ctx context.Context, db *sql.DB, query string, opts []Option, args ...any) (*Reader, error) {
	if db == nil {
		return nil, errors.New("sql: nil db")
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sql: query: %w", err)
	}
	return newReaderFromRows(rows, true, resolve(opts))
}

// NewReaderFromRows wraps an existing *sql.Rows; caller retains
// responsibility for closing rows.
func NewReaderFromRows(rows *sql.Rows, opts ...Option) (*Reader, error) {
	return newReaderFromRows(rows, false, resolve(opts))
}

func newReaderFromRows(rows *sql.Rows, owns bool, cfg config) (*Reader, error) {
	cts, err := rows.ColumnTypes()
	if err != nil {
		if owns {
			rows.Close()
		}
		return nil, fmt.Errorf("sql: column types: %w", err)
	}
	kinds := make([]colKind, len(cts))
	names := make([]string, len(cts))
	for i, ct := range cts {
		kinds[i] = typeNameKind(ct.DatabaseTypeName())
		names[i] = ct.Name()
	}
	return &Reader{
		rows:     rows,
		cfg:      cfg,
		colProto: cts,
		colKinds: kinds,
		colNames: names,
		ownsRows: owns,
	}, nil
}

// ColumnNames returns the result column names in order.
func (r *Reader) ColumnNames() []string {
	out := make([]string, len(r.colNames))
	copy(out, r.colNames)
	return out
}

// Next reads up to batchSize rows and materializes a DataFrame. Returns
// false on end-of-stream or error; inspect Err() to distinguish.
func (r *Reader) Next() bool {
	if r.err != nil {
		return false
	}
	// Release previously-returned batch's reference from the reader's
	// side (caller may still hold their own). This avoids silently
	// leaking arrow buffers if the caller forgets to Release a batch
	// between Next() ticks.
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	cols := make([]*column, len(r.colKinds))
	for i := range cols {
		cols[i] = &column{name: r.colNames[i], kind: r.colKinds[i]}
	}
	scanArgs := make([]any, len(cols))

	rowsRead := 0
	for rowsRead < r.cfg.batchSize && r.rows.Next() {
		for i := range cols {
			scanArgs[i] = cols[i].scanTarget()
		}
		if err := r.rows.Scan(scanArgs...); err != nil {
			r.err = fmt.Errorf("sql: scan: %w", err)
			return false
		}
		for i, t := range scanArgs {
			cols[i].appendFrom(t)
		}
		rowsRead++
	}
	if err := r.rows.Err(); err != nil {
		r.err = fmt.Errorf("sql: iter: %w", err)
		return false
	}
	if rowsRead == 0 {
		return false
	}
	df, err := buildDataFrame(cols, r.cfg.alloc)
	if err != nil {
		r.err = err
		return false
	}
	r.cur = df
	return true
}

// DataFrame returns the current batch. Valid only between Next() and
// the next Next() / Close(). The returned DataFrame's buffers are
// retained by the caller (they stay alive even after the Reader
// advances), matching arrow RecordReader semantics: call Release when
// done.
func (r *Reader) DataFrame() *dataframe.DataFrame {
	if r.cur == nil {
		return nil
	}
	// Caller gets its own refcount bump.
	return r.cur.Clone()
}

// Err returns the first error encountered during Next(), or nil.
func (r *Reader) Err() error { return r.err }

// Close releases the current batch (if any) and closes the underlying
// rows cursor when the Reader owns it. Safe to call more than once.
func (r *Reader) Close() error {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.ownsRows && r.rows != nil {
		err := r.rows.Close()
		r.rows = nil
		return err
	}
	return nil
}

// ReadAll drains the Reader and concatenates every batch into a
// single DataFrame. Equivalent to ReadSQL but reuses the streaming
// path, which keeps per-batch memory bounded.
func (r *Reader) ReadAll() (*dataframe.DataFrame, error) {
	var batches []*dataframe.DataFrame
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	for r.Next() {
		batches = append(batches, r.DataFrame())
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return buildEmptyDataFrame(r.colNames, r.colKinds, r.cfg.alloc)
	}
	if len(batches) == 1 {
		out := batches[0]
		batches[0] = nil
		batches = batches[:0]
		return out, nil
	}
	out, err := dataframe.Concat(batches...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func buildDataFrame(cols []*column, mem memory.Allocator) (*dataframe.DataFrame, error) {
	out := make([]*series.Series, len(cols))
	for i, c := range cols {
		s, err := c.build(mem)
		if err != nil {
			for _, p := range out[:i] {
				if p != nil {
					p.Release()
				}
			}
			return nil, err
		}
		out[i] = s
	}
	return dataframe.New(out...)
}

// buildEmptyDataFrame produces a zero-row DataFrame that still carries
// the full schema. Used when ReadAll finds no rows.
func buildEmptyDataFrame(names []string, kinds []colKind, mem memory.Allocator) (*dataframe.DataFrame, error) {
	out := make([]*series.Series, len(names))
	for i, name := range names {
		c := &column{name: name, kind: kinds[i]}
		s, err := c.build(mem)
		if err != nil {
			for _, p := range out[:i] {
				if p != nil {
					p.Release()
				}
			}
			return nil, err
		}
		out[i] = s
	}
	return dataframe.New(out...)
}

// column buffers one column's values during row scan and emits a
// typed Series on finish.
type column struct {
	name  string
	kind  colKind
	i64   []int64
	f64   []float64
	b     []bool
	s     []string
	valid []bool
}

type colKind int

const (
	kindInt colKind = iota
	kindFloat
	kindBool
	kindString
	kindTime
)

// typeNameKind maps a driver-reported database type name to a golars
// kind. Drivers are inconsistent about case and spelling so we accept
// the common variants; anything unrecognized falls back to string.
func typeNameKind(dbType string) colKind {
	switch dbType {
	case "INTEGER", "INT", "INT2", "INT4", "INT8", "BIGINT", "SMALLINT",
		"TINYINT", "MEDIUMINT", "SERIAL", "BIGSERIAL":
		return kindInt
	case "REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT4", "FLOAT8",
		"DECIMAL", "NUMERIC":
		return kindFloat
	case "BOOL", "BOOLEAN":
		return kindBool
	case "TIMESTAMP", "TIMESTAMPTZ", "DATETIME", "DATE", "TIME":
		return kindTime
	}
	return kindString
}

func (c *column) scanTarget() any {
	switch c.kind {
	case kindInt:
		return &sql.NullInt64{}
	case kindFloat:
		return &sql.NullFloat64{}
	case kindBool:
		return &sql.NullBool{}
	case kindTime:
		return &sql.NullTime{}
	}
	return &sql.NullString{}
}

func (c *column) appendFrom(t any) {
	switch v := t.(type) {
	case *sql.NullInt64:
		c.i64 = append(c.i64, v.Int64)
		c.valid = append(c.valid, v.Valid)
	case *sql.NullFloat64:
		c.f64 = append(c.f64, v.Float64)
		c.valid = append(c.valid, v.Valid)
	case *sql.NullBool:
		c.b = append(c.b, v.Bool)
		c.valid = append(c.valid, v.Valid)
	case *sql.NullString:
		c.s = append(c.s, v.String)
		c.valid = append(c.valid, v.Valid)
	case *sql.NullTime:
		var us int64
		if v.Valid {
			us = v.Time.UnixMicro()
		}
		c.i64 = append(c.i64, us)
		c.valid = append(c.valid, v.Valid)
	}
}

func (c *column) build(mem memory.Allocator) (*series.Series, error) {
	valid := c.valid
	allValid := true
	for _, v := range valid {
		if !v {
			allValid = false
			break
		}
	}
	if allValid {
		valid = nil
	}
	switch c.kind {
	case kindInt, kindTime:
		return series.FromInt64(c.name, c.i64, valid, series.WithAllocator(mem))
	case kindFloat:
		return series.FromFloat64(c.name, c.f64, valid, series.WithAllocator(mem))
	case kindBool:
		return series.FromBool(c.name, c.b, valid, series.WithAllocator(mem))
	}
	return series.FromString(c.name, c.s, valid, series.WithAllocator(mem))
}
