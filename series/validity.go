package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// IsNaN returns a boolean Series with true at every NaN (float dtypes
// only). Null inputs produce null outputs. Non-float dtypes return an
// all-false mask with the same null pattern. Mirrors polars' is_nan.
func (s *Series) IsNaN(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	valid := validFromChunk(chunk)
	out := make([]bool, n)
	switch a := chunk.(type) {
	case *array.Float64:
		for i, v := range a.Float64Values() {
			out[i] = math.IsNaN(v)
		}
	case *array.Float32:
		for i, v := range a.Float32Values() {
			out[i] = math.IsNaN(float64(v))
		}
	case *array.Int64, *array.Int32:
		// int dtypes cannot be NaN; output stays all-false.
	default:
		return nil, fmt.Errorf("series: IsNaN unsupported for dtype %s", s.DType())
	}
	return FromBool(s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// IsFinite returns a boolean mask of finite (non-NaN, non-±Inf) floats.
// Non-float numeric dtypes are always finite.
func (s *Series) IsFinite(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	valid := validFromChunk(chunk)
	out := make([]bool, n)
	switch a := chunk.(type) {
	case *array.Float64:
		for i, v := range a.Float64Values() {
			out[i] = !math.IsNaN(v) && !math.IsInf(v, 0)
		}
	case *array.Float32:
		for i, v := range a.Float32Values() {
			f := float64(v)
			out[i] = !math.IsNaN(f) && !math.IsInf(f, 0)
		}
	case *array.Int64, *array.Int32:
		for i := range out {
			out[i] = true
		}
	default:
		return nil, fmt.Errorf("series: IsFinite unsupported for dtype %s", s.DType())
	}
	return FromBool(s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// IsInfinite returns a boolean mask of ±Inf floats. Non-float numeric
// dtypes always return false.
func (s *Series) IsInfinite(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	valid := validFromChunk(chunk)
	out := make([]bool, n)
	switch a := chunk.(type) {
	case *array.Float64:
		for i, v := range a.Float64Values() {
			out[i] = math.IsInf(v, 0)
		}
	case *array.Float32:
		for i, v := range a.Float32Values() {
			out[i] = math.IsInf(float64(v), 0)
		}
	case *array.Int64, *array.Int32:
		// always false
	default:
		return nil, fmt.Errorf("series: IsInfinite unsupported for dtype %s", s.DType())
	}
	return FromBool(s.Name(), out, valid, WithAllocator(cfg.alloc))
}
