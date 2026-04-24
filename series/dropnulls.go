package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// DropNulls returns a new Series with every null position removed.
// Preserves order. If the input has no nulls, a clone is returned.
// Mirrors polars Series.drop_nulls().
func (s *Series) DropNulls(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	if chunk.NullN() == 0 {
		return s.Clone(), nil
	}
	n := chunk.Len()
	keep := n - chunk.NullN()

	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]int64, 0, keep)
		for i := range n {
			if a.IsValid(i) {
				out = append(out, raw[i])
			}
		}
		return FromInt64(s.Name(), out, nil, WithAllocator(cfg.alloc))
	case *array.Int32:
		raw := a.Int32Values()
		out := make([]int32, 0, keep)
		for i := range n {
			if a.IsValid(i) {
				out = append(out, raw[i])
			}
		}
		return FromInt32(s.Name(), out, nil, WithAllocator(cfg.alloc))
	case *array.Float64:
		raw := a.Float64Values()
		out := make([]float64, 0, keep)
		for i := range n {
			if a.IsValid(i) {
				out = append(out, raw[i])
			}
		}
		return FromFloat64(s.Name(), out, nil, WithAllocator(cfg.alloc))
	case *array.Boolean:
		out := make([]bool, 0, keep)
		for i := range n {
			if a.IsValid(i) {
				out = append(out, a.Value(i))
			}
		}
		return FromBool(s.Name(), out, nil, WithAllocator(cfg.alloc))
	case *array.String:
		out := make([]string, 0, keep)
		for i := range n {
			if a.IsValid(i) {
				out = append(out, a.Value(i))
			}
		}
		return FromString(s.Name(), out, nil, WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series: DropNulls unsupported for dtype %s", s.DType())
}
