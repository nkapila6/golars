package series

import (
	"github.com/apache/arrow-go/v18/arrow/array"
)

// SortedOrder controls what IsSorted considers a "sorted" series.
type SortedOrder int

const (
	// SortedAscending accepts non-decreasing sequences (ties allowed).
	SortedAscending SortedOrder = iota
	// SortedDescending accepts non-increasing sequences (ties allowed).
	SortedDescending
	// SortedStrictAscending requires strict ordering (no ties).
	SortedStrictAscending
	// SortedStrictDescending requires strict reverse ordering (no ties).
	SortedStrictDescending
)

// IsSorted reports whether s is sorted in the requested order. Nulls
// are treated as "last" (polars' default null placement in sort), so
// any null inside an otherwise-sorted prefix makes the result false.
// Mirrors polars' Series.is_sorted(descending=..., strict=...).
//
// Returns (false, error) only for unsupported dtypes; for supported
// dtypes IsSorted never errors.
func (s *Series) IsSorted(order SortedOrder) (bool, error) {
	chunk := s.Chunk(0)
	n := chunk.Len()
	if n <= 1 {
		return true, nil
	}
	// Any internal null breaks the ordering property (we require nulls
	// only at the end; consecutive trailing nulls are fine).
	desc := order == SortedDescending || order == SortedStrictDescending
	strict := order == SortedStrictAscending || order == SortedStrictDescending

	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		return isSortedPrimitive(raw, a, n, desc, strict,
			func(x, y int64) int {
				if x < y {
					return -1
				}
				if x > y {
					return 1
				}
				return 0
			}), nil
	case *array.Int32:
		raw := a.Int32Values()
		return isSortedPrimitive(raw, a, n, desc, strict,
			func(x, y int32) int {
				if x < y {
					return -1
				}
				if x > y {
					return 1
				}
				return 0
			}), nil
	case *array.Float64:
		raw := a.Float64Values()
		return isSortedPrimitive(raw, a, n, desc, strict,
			func(x, y float64) int {
				// NaN is considered larger than any real value (polars
				// convention for sort).
				xNaN, yNaN := x != x, y != y
				if xNaN && yNaN {
					return 0
				}
				if xNaN {
					return 1
				}
				if yNaN {
					return -1
				}
				if x < y {
					return -1
				}
				if x > y {
					return 1
				}
				return 0
			}), nil
	case *array.Boolean:
		// Specialised because Boolean has no backing values slice.
		lastIdx := -1
		seenNull := false
		for i := range n {
			if a.IsNull(i) {
				seenNull = true
				continue
			}
			if seenNull {
				return false, nil
			}
			if lastIdx >= 0 {
				x, y := a.Value(lastIdx), a.Value(i)
				if cmp := boolCmp(x, y); !validSortStep(cmp, desc, strict) {
					return false, nil
				}
			}
			lastIdx = i
		}
		return true, nil
	case *array.String:
		lastIdx := -1
		seenNull := false
		for i := range n {
			if a.IsNull(i) {
				seenNull = true
				continue
			}
			if seenNull {
				return false, nil
			}
			if lastIdx >= 0 {
				x, y := a.Value(lastIdx), a.Value(i)
				var c int
				if x < y {
					c = -1
				} else if x > y {
					c = 1
				}
				if !validSortStep(c, desc, strict) {
					return false, nil
				}
			}
			lastIdx = i
		}
		return true, nil
	}
	return false, errUniqueUnsupported(s.DType())
}

// isSortedPrimitive scans a primitive buffer. Nulls are accepted only
// when they all sit at the tail (polars' nulls_last default): a valid
// value appearing after a null makes the series not sorted.
func isSortedPrimitive[T any](
	raw []T, chunk interface{ IsValid(int) bool },
	n int, desc, strict bool, cmp func(a, b T) int,
) bool {
	prevIdx := -1
	seenNull := false
	for i := range n {
		if !chunk.IsValid(i) {
			seenNull = true
			continue
		}
		if seenNull {
			return false
		}
		if prevIdx >= 0 {
			c := cmp(raw[prevIdx], raw[i])
			if !validSortStep(c, desc, strict) {
				return false
			}
		}
		prevIdx = i
	}
	return true
}

// validSortStep reports whether a single element-to-element compare
// value (cmp(prev, curr)) is compatible with the requested order.
func validSortStep(c int, desc, strict bool) bool {
	switch {
	case desc && strict:
		return c > 0
	case desc && !strict:
		return c >= 0
	case !desc && strict:
		return c < 0
	default: // ascending, non-strict
		return c <= 0
	}
}

func boolCmp(x, y bool) int {
	switch {
	case x == y:
		return 0
	case !x && y:
		return -1
	default:
		return 1
	}
}
