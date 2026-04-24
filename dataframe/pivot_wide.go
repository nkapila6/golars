package dataframe

import (
	"context"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// PivotAgg names the reduction polars' DataFrame.pivot applies when
// multiple rows map to the same (index, column) cell. "first" keeps
// the first encountered value; "sum"/"mean"/"min"/"max"/"count"
// aggregate across the group. Unknown values default to "first".
type PivotAgg string

const (
	PivotFirst PivotAgg = "first"
	PivotSum   PivotAgg = "sum"
	PivotMean  PivotAgg = "mean"
	PivotMin   PivotAgg = "min"
	PivotMax   PivotAgg = "max"
	PivotCount PivotAgg = "count"
)

// pivotCell accumulates the values mapping to one (index, column) tuple.
type pivotCell struct {
	total     float64
	count     int64
	first     float64
	firstSet  bool
	minV      float64
	maxV      float64
	minMaxSet bool
}

func (c *pivotCell) observe(v float64) {
	if !c.firstSet {
		c.first = v
		c.firstSet = true
	}
	c.total += v
	c.count++
	if !c.minMaxSet {
		c.minV, c.maxV = v, v
		c.minMaxSet = true
		return
	}
	if v < c.minV {
		c.minV = v
	}
	if v > c.maxV {
		c.maxV = v
	}
}

func (c *pivotCell) reduce(agg PivotAgg) (float64, bool) {
	if c.count == 0 {
		if agg == PivotCount {
			return 0, true
		}
		return 0, false
	}
	switch agg {
	case PivotSum:
		return c.total, true
	case PivotMean:
		return c.total / float64(c.count), true
	case PivotMin:
		return c.minV, true
	case PivotMax:
		return c.maxV, true
	case PivotCount:
		return float64(c.count), true
	}
	return c.first, true
}

// Pivot reshapes a long-form DataFrame into wide form. index names
// the column(s) kept as row identifiers; columns supplies the column
// whose distinct values become new output columns; values supplies
// the column whose cells populate the new columns. agg selects the
// reduction when multiple rows share an (index, column) tuple.
//
// Mirrors polars' DataFrame.pivot(index=, on=, values=, aggregate_function=).
func (df *DataFrame) Pivot(
	_ context.Context,
	index []string,
	columns string,
	values string,
	agg PivotAgg,
) (*DataFrame, error) {
	if len(index) == 0 {
		return nil, fmt.Errorf("dataframe.Pivot: at least one index column required")
	}
	idxCols := make([]*series.Series, len(index))
	for i, n := range index {
		c, err := df.Column(n)
		if err != nil {
			return nil, err
		}
		idxCols[i] = c
	}
	onCol, err := df.Column(columns)
	if err != nil {
		return nil, err
	}
	valCol, err := df.Column(values)
	if err != nil {
		return nil, err
	}

	height := df.Height()
	idxMap := map[string]int{}
	var idxOrder []string
	idxFirstRow := []int{}

	colMap := map[string]int{}
	var colOrder []string

	// Cells laid out as a 2D slice indexed by (idxSlot, colSlot). We
	// grow both dimensions as new keys are seen.
	cells := [][]*pivotCell{}

	ensureCell := func(idxSlot, colSlot int) *pivotCell {
		for len(cells) <= idxSlot {
			cells = append(cells, nil)
		}
		row := cells[idxSlot]
		for len(row) <= colSlot {
			row = append(row, nil)
		}
		cells[idxSlot] = row
		if row[colSlot] == nil {
			row[colSlot] = &pivotCell{}
		}
		return row[colSlot]
	}

	for r := range height {
		key := idxMapKey(idxCols, r)
		idxSlot, ok := idxMap[key]
		if !ok {
			idxSlot = len(idxOrder)
			idxMap[key] = idxSlot
			idxOrder = append(idxOrder, key)
			idxFirstRow = append(idxFirstRow, r)
		}
		onKey := cellString(onCol.Chunk(0), r)
		colSlot, ok := colMap[onKey]
		if !ok {
			colSlot = len(colOrder)
			colMap[onKey] = colSlot
			colOrder = append(colOrder, onKey)
		}
		if !valCol.Chunk(0).IsValid(r) {
			continue
		}
		v, ok := floatFromChunk(valCol.Chunk(0), r)
		if !ok {
			continue
		}
		c := ensureCell(idxSlot, colSlot)
		c.observe(v)
	}

	// Sort column labels alphabetically for a deterministic output.
	stableCols := make([]string, len(colOrder))
	copy(stableCols, colOrder)
	sort.Strings(stableCols)

	rows := len(idxOrder)
	outCols := make([]*series.Series, 0, len(index)+len(stableCols))
	for i, name := range index {
		s, err := gatherIndexColumn(idxCols[i], idxFirstRow, name)
		if err != nil {
			for _, p := range outCols {
				p.Release()
			}
			return nil, err
		}
		outCols = append(outCols, s)
	}
	for _, onValue := range stableCols {
		oldSlot := colMap[onValue]
		vals := make([]float64, rows)
		valid := make([]bool, rows)
		for idxSlot, row := range cells {
			if oldSlot >= len(row) || row[oldSlot] == nil {
				continue
			}
			if v, ok := row[oldSlot].reduce(agg); ok {
				vals[idxSlot] = v
				valid[idxSlot] = true
			}
		}
		s, err := series.FromFloat64(onValue, vals, valid)
		if err != nil {
			for _, p := range outCols {
				p.Release()
			}
			return nil, err
		}
		outCols = append(outCols, s)
	}
	return New(outCols...)
}

// floatFromChunk extracts a numeric value as float64. Returns false
// when the dtype isn't numeric.
func floatFromChunk(chunk any, i int) (float64, bool) {
	switch a := chunk.(type) {
	case *array.Float64:
		return a.Value(i), true
	case *array.Float32:
		return float64(a.Value(i)), true
	case *array.Int64:
		return float64(a.Value(i)), true
	case *array.Int32:
		return float64(a.Value(i)), true
	case *array.Boolean:
		if a.Value(i) {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// cellString returns the string representation of chunk[i] for use as
// a pivot-column header. Numeric cells become their decimal form.
func cellString(chunk any, i int) string {
	switch a := chunk.(type) {
	case *array.String:
		return a.Value(i)
	case *array.Int64:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Int32:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Float64:
		return fmt.Sprintf("%g", a.Value(i))
	case *array.Float32:
		return fmt.Sprintf("%g", a.Value(i))
	case *array.Boolean:
		if a.Value(i) {
			return "true"
		}
		return "false"
	}
	return ""
}

// idxMapKey formats a row's index-column tuple into a stable string key.
func idxMapKey(cols []*series.Series, row int) string {
	buf := make([]byte, 0, 32)
	for i, c := range cols {
		if i > 0 {
			buf = append(buf, 0x1f)
		}
		buf = append(buf, formatCell(c.Chunk(0), row)...)
	}
	return string(buf)
}

// gatherIndexColumn copies one row per unique tuple from src into a
// fresh Series. Uses the source's native dtype for strings; other
// dtypes collapse to float64.
func gatherIndexColumn(src *series.Series, rowFor []int, name string) (*series.Series, error) {
	chunk := src.Chunk(0)
	n := len(rowFor)
	if a, ok := chunk.(*array.String); ok {
		out := make([]string, n)
		valid := make([]bool, n)
		for i, r := range rowFor {
			if a.IsValid(r) {
				out[i] = a.Value(r)
				valid[i] = true
			}
		}
		return series.FromString(name, out, valid)
	}
	out := make([]float64, n)
	valid := make([]bool, n)
	for i, r := range rowFor {
		if !chunk.IsValid(r) {
			continue
		}
		if v, ok := floatFromChunk(chunk, r); ok {
			out[i] = v
			valid[i] = true
		}
	}
	return series.FromFloat64(name, out, valid)
}
