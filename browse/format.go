package browse

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// cellFunc stringifies cell i of a single column. Closed over the
// concrete arrow array so reads don't pay interface dispatch.
type cellFunc func(int) string

// makeCellFn returns a cellFunc for an arrow array. Unknown types
// fall back to "?".
func makeCellFn(chunk any) cellFunc {
	const null = "∅"
	switch a := chunk.(type) {
	case *array.String:
		return func(i int) string {
			if !a.IsValid(i) {
				return null
			}
			return a.Value(i)
		}
	case *array.Int64:
		return func(i int) string {
			if !a.IsValid(i) {
				return null
			}
			return strconv.FormatInt(a.Value(i), 10)
		}
	case *array.Int32:
		return func(i int) string {
			if !a.IsValid(i) {
				return null
			}
			return strconv.FormatInt(int64(a.Value(i)), 10)
		}
	case *array.Uint64:
		return func(i int) string {
			if !a.IsValid(i) {
				return null
			}
			return strconv.FormatUint(a.Value(i), 10)
		}
	case *array.Uint32:
		return func(i int) string {
			if !a.IsValid(i) {
				return null
			}
			return strconv.FormatUint(uint64(a.Value(i)), 10)
		}
	case *array.Float64:
		return func(i int) string {
			if !a.IsValid(i) {
				return null
			}
			return strconv.FormatFloat(a.Value(i), 'g', -1, 64)
		}
	case *array.Float32:
		return func(i int) string {
			if !a.IsValid(i) {
				return null
			}
			return strconv.FormatFloat(float64(a.Value(i)), 'g', -1, 32)
		}
	case *array.Boolean:
		return func(i int) string {
			if !a.IsValid(i) {
				return null
			}
			if a.Value(i) {
				return "true"
			}
			return "false"
		}
	}
	return func(int) string { return "?" }
}

// summaryString returns a one-line summary for the column at visIdx:
// numeric -> min / max / mean / sum; string -> approximate distinct
// count; boolean -> len / null count.
func (m *model) summaryString(visIdx int) string {
	vis := m.visibleCols()
	if visIdx < 0 || visIdx >= len(vis) {
		return ""
	}
	ci := vis[visIdx]
	cv := m.cols[ci]
	col := m.df.Columns()[cv.orig]
	switch cv.dtype {
	case "i64", "i32", "u64", "u32", "f64", "f32":
		sum, err := col.Sum()
		if err != nil {
			return ""
		}
		mn, _ := col.Min()
		mx, _ := col.Max()
		mean, _ := col.Mean()
		return fmt.Sprintf("sum=%g  min=%g  max=%g  mean=%g", sum, mn, mx, mean)
	case "bool":
		return fmt.Sprintf("len=%d  nulls=%d", col.Len(), col.NullCount())
	case "str":
		return fmt.Sprintf("distinct≈%d  nulls=%d",
			approxDistinct(m.cellFn[cv.orig], col.Len()), col.NullCount())
	}
	return ""
}

// approxDistinct samples up to 4096 values. Cheap enough to recompute
// on every cursor move.
func approxDistinct(fn cellFunc, n int) int {
	const sampleCap = 4096
	seen := map[string]struct{}{}
	step := 1
	if n > sampleCap {
		step = n / sampleCap
	}
	for i := 0; i < n; i += step {
		seen[fn(i)] = struct{}{}
	}
	return len(seen)
}

// padRight pads s with spaces to width w.
func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	pad := make([]byte, w-len(s))
	for i := range pad {
		pad[i] = ' '
	}
	return s + string(pad)
}

// truncate shortens s to width w, replacing the last rune with an
// ellipsis when it doesn't fit.
func truncate(s string, w int) string {
	if len(s) <= w {
		return s
	}
	if w <= 1 {
		return s[:w]
	}
	return s[:w-1] + "…"
}
