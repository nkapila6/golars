package dataframe

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// Transpose returns a DataFrame that is the transpose of df. All
// columns must share a numeric dtype; the output has one column per
// row of the input. `headerCol` names the first output column that
// carries the original column names; `colPrefix` is used to name the
// resulting columns ("0", "1", ... when colPrefix is empty).
//
// Mirrors polars' DataFrame.transpose(include_header=False,
// header_name=headerCol, column_names=None) for numeric input.
func (df *DataFrame) Transpose(_ context.Context, headerCol, colPrefix string) (*DataFrame, error) {
	if df.Width() == 0 {
		return New()
	}
	if headerCol == "" {
		headerCol = "column"
	}
	// Verify all input columns are numeric or boolean; otherwise polars
	// falls back to Object dtype which we don't support yet.
	for _, c := range df.cols {
		if !c.DType().IsNumeric() && !c.DType().IsBool() {
			return nil, fmt.Errorf("dataframe.Transpose: column %q dtype %s not supported (need numeric/bool)",
				c.Name(), c.DType())
		}
	}
	height := df.Height()
	width := df.Width()
	// Output: one header column + `height` data columns. Each data
	// column has `width` rows (one per input column).
	headerVals := make([]string, width)
	for i, c := range df.cols {
		headerVals[i] = c.Name()
	}
	header, err := series.FromString(headerCol, headerVals, nil)
	if err != nil {
		return nil, err
	}
	out := []*series.Series{header}
	for row := range height {
		vals := make([]float64, width)
		valid := make([]bool, width)
		for ci, c := range df.cols {
			if v, ok := floatCell(c.Chunk(0), row); ok {
				vals[ci] = v
				valid[ci] = true
			}
		}
		name := strconv.Itoa(row)
		if colPrefix != "" {
			name = colPrefix + name
		}
		s, err := series.FromFloat64(name, vals, valid)
		if err != nil {
			for _, prev := range out {
				prev.Release()
			}
			return nil, err
		}
		out = append(out, s)
	}
	return New(out...)
}

// Unpivot reshapes df from wide to long form. idVars stay as-is; each
// other column becomes two rows of a long frame: a "variable" column
// holding the original column name and a "value" column holding the
// cell value. Mirrors polars' DataFrame.unpivot (melt in pandas).
func (df *DataFrame) Unpivot(_ context.Context, idVars []string, valueVars []string) (*DataFrame, error) {
	if len(valueVars) == 0 {
		// Default: every non-id column is a value column.
		seen := make(map[string]struct{}, len(idVars))
		for _, v := range idVars {
			seen[v] = struct{}{}
		}
		for _, c := range df.cols {
			if _, ok := seen[c.Name()]; !ok {
				valueVars = append(valueVars, c.Name())
			}
		}
	}
	if len(valueVars) == 0 {
		return nil, fmt.Errorf("dataframe.Unpivot: no value columns")
	}
	// Resolve columns.
	idCols := make([]*series.Series, len(idVars))
	for i, v := range idVars {
		c, err := df.Column(v)
		if err != nil {
			return nil, err
		}
		idCols[i] = c
	}
	valCols := make([]*series.Series, len(valueVars))
	for i, v := range valueVars {
		c, err := df.Column(v)
		if err != nil {
			return nil, err
		}
		valCols[i] = c
	}
	height := df.Height()
	out := len(valueVars) * height
	// Build the id columns (replicated), the "variable" column, and
	// the "value" column (float64 to accept mixed numeric types).
	idOut := make([][]float64, len(idVars))
	idOutValid := make([][]bool, len(idVars))
	idIsStr := make([]bool, len(idVars))
	idStrOut := make([][]string, len(idVars))
	idStrValid := make([][]bool, len(idVars))
	for i, c := range idCols {
		if _, isStr := c.Chunk(0).(*array.String); isStr {
			idIsStr[i] = true
			idStrOut[i] = make([]string, out)
			idStrValid[i] = make([]bool, out)
		} else {
			idOut[i] = make([]float64, out)
			idOutValid[i] = make([]bool, out)
		}
	}
	variable := make([]string, out)
	value := make([]float64, out)
	valid := make([]bool, out)
	row := 0
	for _, c := range valCols {
		for r := range height {
			variable[row] = c.Name()
			if v, ok := floatCell(c.Chunk(0), r); ok {
				value[row] = v
				valid[row] = true
			}
			for i, ic := range idCols {
				if idIsStr[i] {
					sa := ic.Chunk(0).(*array.String)
					if sa.IsValid(r) {
						idStrOut[i][row] = sa.Value(r)
						idStrValid[i][row] = true
					}
				} else {
					if v, ok := floatCell(ic.Chunk(0), r); ok {
						idOut[i][row] = v
						idOutValid[i][row] = true
					}
				}
			}
			row++
		}
	}
	// Compose the output frame: idVars, variable, value.
	outCols := make([]*series.Series, 0, len(idVars)+2)
	for i, v := range idVars {
		if idIsStr[i] {
			s, err := series.FromString(v, idStrOut[i], idStrValid[i])
			if err != nil {
				for _, p := range outCols {
					p.Release()
				}
				return nil, err
			}
			outCols = append(outCols, s)
		} else {
			s, err := series.FromFloat64(v, idOut[i], idOutValid[i])
			if err != nil {
				for _, p := range outCols {
					p.Release()
				}
				return nil, err
			}
			outCols = append(outCols, s)
		}
	}
	varCol, err := series.FromString("variable", variable, nil)
	if err != nil {
		for _, p := range outCols {
			p.Release()
		}
		return nil, err
	}
	outCols = append(outCols, varCol)
	valCol, err := series.FromFloat64("value", value, valid)
	if err != nil {
		for _, p := range outCols {
			p.Release()
		}
		return nil, err
	}
	outCols = append(outCols, valCol)
	return New(outCols...)
}

func floatCell(chunk any, i int) (float64, bool) {
	switch a := chunk.(type) {
	case *array.Float64:
		if !a.IsValid(i) {
			return 0, false
		}
		return a.Value(i), true
	case *array.Float32:
		if !a.IsValid(i) {
			return 0, false
		}
		return float64(a.Value(i)), true
	case *array.Int64:
		if !a.IsValid(i) {
			return 0, false
		}
		return float64(a.Value(i)), true
	case *array.Int32:
		if !a.IsValid(i) {
			return 0, false
		}
		return float64(a.Value(i)), true
	case *array.Boolean:
		if !a.IsValid(i) {
			return 0, false
		}
		if a.Value(i) {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}
