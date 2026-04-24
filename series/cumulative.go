package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// CumSum returns a new Series where out[i] = sum of s[0..i] (inclusive),
// skipping nulls as if they were zero. Mirrors polars'
// Series.cum_sum(). If the source has any nulls, the output at those
// positions is also null: a null cannot be "summed into" a running
// total without a reset rule, and polars preserves nulls here.
//
// Only numeric dtypes (int64, int32, float64) are supported; other
// dtypes return an error.
func (s *Series) CumSum(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		if a.NullN() == 0 {
			return BuildInt64Direct(s.Name(), n, cfg.alloc, func(out []int64) {
				var acc int64
				for i, v := range raw {
					acc += v
					out[i] = acc
				}
			})
		}
		out := make([]int64, n)
		valid := make([]bool, n)
		var acc int64
		for i := range n {
			if a.IsValid(i) {
				acc += raw[i]
				out[i] = acc
				valid[i] = true
			}
		}
		return FromInt64(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Float64:
		raw := a.Float64Values()
		if a.NullN() == 0 {
			return BuildFloat64Direct(s.Name(), n, cfg.alloc, func(out []float64) {
				var acc float64
				for i, v := range raw {
					acc += v
					out[i] = acc
				}
			})
		}
		out := make([]float64, n)
		valid := make([]bool, n)
		var acc float64
		for i := range n {
			if a.IsValid(i) {
				acc += raw[i]
				out[i] = acc
				valid[i] = true
			}
		}
		return FromFloat64(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Int32:
		raw := a.Int32Values()
		out := make([]int32, n)
		var acc int32
		if a.NullN() == 0 {
			for i, v := range raw {
				acc += v
				out[i] = acc
			}
			return FromInt32(s.Name(), out, nil, WithAllocator(cfg.alloc))
		}
		valid := make([]bool, n)
		for i := range n {
			if a.IsValid(i) {
				acc += raw[i]
				out[i] = acc
				valid[i] = true
			}
		}
		return FromInt32(s.Name(), out, valid, WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series: CumSum unsupported for dtype %s", s.DType())
}

// Diff returns the elementwise difference s[i] - s[i-periods]. The
// first periods positions are null. Negative periods diff against a
// future row (tail becomes null). Mirrors polars Series.diff().
//
// Only numeric dtypes are supported.
func (s *Series) Diff(periods int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	if periods == 0 {
		return nil, fmt.Errorf("series: Diff periods must be non-zero")
	}
	if periods >= n || -periods >= n {
		return nullSeries(s.Name(), s.DType(), n, cfg.alloc)
	}
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]int64, n)
		valid := make([]bool, n)
		if periods > 0 {
			for i := periods; i < n; i++ {
				if a.IsValid(i) && a.IsValid(i-periods) {
					out[i] = raw[i] - raw[i-periods]
					valid[i] = true
				}
			}
		} else {
			off := -periods
			for i := 0; i < n-off; i++ {
				if a.IsValid(i) && a.IsValid(i+off) {
					out[i] = raw[i] - raw[i+off]
					valid[i] = true
				}
			}
		}
		return FromInt64(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Float64:
		raw := a.Float64Values()
		out := make([]float64, n)
		valid := make([]bool, n)
		if periods > 0 {
			for i := periods; i < n; i++ {
				if a.IsValid(i) && a.IsValid(i-periods) {
					out[i] = raw[i] - raw[i-periods]
					valid[i] = true
				}
			}
		} else {
			off := -periods
			for i := 0; i < n-off; i++ {
				if a.IsValid(i) && a.IsValid(i+off) {
					out[i] = raw[i] - raw[i+off]
					valid[i] = true
				}
			}
		}
		return FromFloat64(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Int32:
		raw := a.Int32Values()
		out := make([]int32, n)
		valid := make([]bool, n)
		if periods > 0 {
			for i := periods; i < n; i++ {
				if a.IsValid(i) && a.IsValid(i-periods) {
					out[i] = raw[i] - raw[i-periods]
					valid[i] = true
				}
			}
		} else {
			off := -periods
			for i := 0; i < n-off; i++ {
				if a.IsValid(i) && a.IsValid(i+off) {
					out[i] = raw[i] - raw[i+off]
					valid[i] = true
				}
			}
		}
		return FromInt32(s.Name(), out, valid, WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series: Diff unsupported for dtype %s", s.DType())
}
