package eval

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalOver implements `expr.over(keys...)`: evaluate the inner
// expression per group defined by keys and broadcast the result back
// to the original row positions.
//
// Shape rules mirror polars:
//   - scalar inner (length 1)   -> broadcast across every row of the group.
//   - length-height inner       -> gather the subset for the group.
//
// Non-matching shapes return an error.
func evalOver(ctx context.Context, ec EvalContext, n expr.OverNode, df *dataframe.DataFrame) (*series.Series, error) {
	if len(n.Keys) == 0 {
		// Without keys, over() degenerates to the inner expression.
		return evalNode(ctx, ec, n.Inner, df)
	}
	// Fast path: when the inner is a plain scalar aggregation
	// (sum/mean/min/max/count on a single column), avoid the
	// per-group materialise-and-eval loop entirely. We groupby
	// the keys, run the agg once, then hash-join the result back.
	if fast, err := tryScalarAggOver(ctx, ec, n, df); fast != nil || err != nil {
		return fast, err
	}
	height := df.Height()
	// Build per-row group key and a per-group row list.
	keyCols := make([]*series.Series, len(n.Keys))
	for i, k := range n.Keys {
		c, err := df.Column(k)
		if err != nil {
			return nil, err
		}
		keyCols[i] = c
	}
	groups := map[string][]int{}
	order := []string{}
	for r := range height {
		key := overKey(keyCols, r)
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], r)
	}
	// Compose the output buffer; use float64 to accept mixed numeric
	// intermediate dtypes. String and bool inners hit a separate path.
	out := make([]outValue, height)
	var outDType string
	for _, key := range order {
		rows := groups[key]
		sub, err := gatherGroupFrame(ctx, df, rows)
		if err != nil {
			return nil, err
		}
		res, err := evalNode(ctx, ec, n.Inner, sub)
		sub.Release()
		if err != nil {
			return nil, err
		}
		resLen := res.Len()
		switch resLen {
		case 1:
			broadcastToOut(res.Chunk(0), rows, out, &outDType)
		default:
			if resLen != len(rows) {
				res.Release()
				return nil, fmt.Errorf("eval: over() inner produced len %d for %d rows", resLen, len(rows))
			}
			scatterToOut(res.Chunk(0), rows, out, &outDType)
		}
		res.Release()
	}
	// Materialise based on dominant dtype.
	switch outDType {
	case "float":
		vals := make([]float64, height)
		valid := make([]bool, height)
		for i := range out {
			if out[i].kind == 1 {
				vals[i] = out[i].f
				valid[i] = true
			}
		}
		return series.FromFloat64(n.String(), vals, valid, seriesAllocOpt(ec))
	case "string":
		vals := make([]string, height)
		valid := make([]bool, height)
		for i := range out {
			if out[i].kind == 2 {
				vals[i] = out[i].s
				valid[i] = true
			}
		}
		return series.FromString(n.String(), vals, valid, seriesAllocOpt(ec))
	case "bool":
		vals := make([]bool, height)
		valid := make([]bool, height)
		for i := range out {
			if out[i].kind == 3 {
				vals[i] = out[i].b
				valid[i] = true
			}
		}
		return series.FromBool(n.String(), vals, valid, seriesAllocOpt(ec))
	}
	// All-null fallback (empty input).
	return series.FromFloat64(n.String(), make([]float64, height), make([]bool, height), seriesAllocOpt(ec))
}

// overKey formats a row's group tuple into a stable string.
func overKey(cols []*series.Series, row int) string {
	buf := make([]byte, 0, 32)
	for i, c := range cols {
		if i > 0 {
			buf = append(buf, 0x1f)
		}
		buf = appendCell(buf, c.Chunk(0), row)
	}
	return string(buf)
}

func appendCell(buf []byte, chunk any, i int) []byte {
	switch a := chunk.(type) {
	case *array.Int64:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		return strconv.AppendInt(buf, a.Value(i), 10)
	case *array.Int32:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		return strconv.AppendInt(buf, int64(a.Value(i)), 10)
	case *array.Float64:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		return strconv.AppendFloat(buf, a.Value(i), 'g', -1, 64)
	case *array.String:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		return append(buf, a.Value(i)...)
	case *array.Boolean:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		if a.Value(i) {
			return append(buf, '1')
		}
		return append(buf, '0')
	}
	return buf
}

// gatherGroupFrame builds a sub-DataFrame containing only rows in the
// given indices, one column at a time. Uses compute.Take per column.
func gatherGroupFrame(ctx context.Context, df *dataframe.DataFrame, rows []int) (*dataframe.DataFrame, error) {
	names := df.Schema().Names()
	cols := make([]*series.Series, 0, len(names))
	for _, name := range names {
		c, err := df.Column(name)
		if err != nil {
			for _, p := range cols {
				p.Release()
			}
			return nil, err
		}
		taken, err := compute.Take(ctx, c, rows)
		if err != nil {
			for _, p := range cols {
				p.Release()
			}
			return nil, err
		}
		cols = append(cols, taken)
	}
	return dataframe.New(cols...)
}

type outValue struct {
	f    float64
	s    string
	b    bool
	kind byte // 0=null, 1=float, 2=str, 3=bool
}

func broadcastToOut(chunk any, rows []int, out []outValue, dominantDT *string) {
	for _, r := range rows {
		writeOut(chunk, 0, r, out, dominantDT)
	}
}

func scatterToOut(chunk any, rows []int, out []outValue, dominantDT *string) {
	for i, r := range rows {
		writeOut(chunk, i, r, out, dominantDT)
	}
}

func writeOut(chunk any, src int, dst int, out []outValue, dominantDT *string) {
	switch a := chunk.(type) {
	case *array.Float64:
		if *dominantDT == "" {
			*dominantDT = "float"
		}
		if !a.IsValid(src) {
			return
		}
		out[dst] = outValue{f: a.Value(src), kind: 1}
	case *array.Float32:
		if *dominantDT == "" {
			*dominantDT = "float"
		}
		if !a.IsValid(src) {
			return
		}
		out[dst] = outValue{f: float64(a.Value(src)), kind: 1}
	case *array.Int64:
		if *dominantDT == "" {
			*dominantDT = "float"
		}
		if !a.IsValid(src) {
			return
		}
		out[dst] = outValue{f: float64(a.Value(src)), kind: 1}
	case *array.Int32:
		if *dominantDT == "" {
			*dominantDT = "float"
		}
		if !a.IsValid(src) {
			return
		}
		out[dst] = outValue{f: float64(a.Value(src)), kind: 1}
	case *array.Boolean:
		if *dominantDT == "" {
			*dominantDT = "bool"
		}
		if !a.IsValid(src) {
			return
		}
		out[dst] = outValue{b: a.Value(src), kind: 3}
	case *array.String:
		if *dominantDT == "" {
			*dominantDT = "string"
		}
		if !a.IsValid(src) {
			return
		}
		out[dst] = outValue{s: a.Value(src), kind: 2}
	}
}

// seriesAllocOpt returns the allocator option matching ec.
func seriesAllocOpt(ec EvalContext) series.Option {
	return seriesAlloc(ec)
}
