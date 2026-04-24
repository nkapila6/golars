package dataframe

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// ToArrow returns the DataFrame as an arrow.RecordBatch. Each column
// emits its first chunk. The returned RecordBatch shares memory with
// the DataFrame: Retain if you need it to outlive this DataFrame.
// Callers that want a multi-batch view should use ToArrowTable.
func (df *DataFrame) ToArrow() arrow.RecordBatch {
	arrays := make([]arrow.Array, len(df.cols))
	for i, c := range df.cols {
		a := c.Chunk(0)
		a.Retain()
		arrays[i] = a
	}
	sch := df.sch.ToArrow()
	rec := array.NewRecordBatch(sch, arrays, int64(df.height))
	// NewRecord increments refcounts on arrays; drop the refs we held.
	for _, a := range arrays {
		a.Release()
	}
	return rec
}

// ToArrowTable returns the DataFrame as an arrow.Table (a
// multi-chunk, multi-column view). This is the format most
// cross-language Arrow IPC tools expect.
func (df *DataFrame) ToArrowTable() arrow.Table {
	sch := df.sch.ToArrow()
	cols := make([]arrow.Column, len(df.cols))
	colPtrs := make([]*arrow.Column, len(df.cols))
	for i, c := range df.cols {
		chunked := c.ToArrowChunked()
		col := arrow.NewColumn(sch.Field(i), chunked)
		chunked.Release()
		cols[i] = *col
		colPtrs[i] = col
	}
	t := array.NewTable(sch, cols, int64(df.height))
	// NewTable retains each column; release ours.
	for _, col := range colPtrs {
		col.Release()
	}
	return t
}

// FromArrowTable constructs a DataFrame from an arrow.Table. Each
// column is wrapped as a single Series; the table's chunk layout is
// preserved. The returned DataFrame retains references: the caller
// can Release their table handle independently.
func FromArrowTable(t arrow.Table) (*DataFrame, error) {
	n := int(t.NumCols())
	cols := make([]*series.Series, n)
	for i := range n {
		col := t.Column(i)
		s := series.FromArrowChunked(col.Name(), col.Data())
		cols[i] = s
	}
	return New(cols...)
}
