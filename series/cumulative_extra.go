package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// CumMin returns a running minimum: out[i] = min(src[0..=i]).
// Nulls are skipped (out stays at the previous running min when the
// current row is null), matching polars. Empty input → empty output.
func (s *Series) CumMin(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]int64, n)
		valid := make([]bool, n)
		cur := int64(math.MaxInt64)
		seen := false
		for i := range n {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			v := raw[i]
			if !seen || v < cur {
				cur = v
			}
			seen = true
			out[i] = cur
			valid[i] = true
		}
		return FromInt64(s.Name(), out, validOrNil(valid), WithAllocator(cfg.alloc))
	case *array.Float64:
		raw := a.Float64Values()
		out := make([]float64, n)
		valid := make([]bool, n)
		cur := math.Inf(1)
		seen := false
		for i := range n {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			v := raw[i]
			if !seen || (!math.IsNaN(v) && v < cur) {
				cur = v
			}
			seen = true
			out[i] = cur
			valid[i] = true
		}
		return FromFloat64(s.Name(), out, validOrNil(valid), WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series: CumMin unsupported for dtype %s", s.DType())
}

// CumMax is the symmetric counterpart of CumMin.
func (s *Series) CumMax(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]int64, n)
		valid := make([]bool, n)
		cur := int64(math.MinInt64)
		seen := false
		for i := range n {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			v := raw[i]
			if !seen || v > cur {
				cur = v
			}
			seen = true
			out[i] = cur
			valid[i] = true
		}
		return FromInt64(s.Name(), out, validOrNil(valid), WithAllocator(cfg.alloc))
	case *array.Float64:
		raw := a.Float64Values()
		out := make([]float64, n)
		valid := make([]bool, n)
		cur := math.Inf(-1)
		seen := false
		for i := range n {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			v := raw[i]
			if !seen || (!math.IsNaN(v) && v > cur) {
				cur = v
			}
			seen = true
			out[i] = cur
			valid[i] = true
		}
		return FromFloat64(s.Name(), out, validOrNil(valid), WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series: CumMax unsupported for dtype %s", s.DType())
}

// CumProd returns a running product. Polars widens to the input's
// numeric dtype; we emit float64 for simplicity and overflow safety.
func (s *Series) CumProd(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]float64, n)
	valid := make([]bool, n)
	cur := 1.0
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		for i := range n {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			cur *= float64(raw[i])
			out[i] = cur
			valid[i] = true
		}
	case *array.Float64:
		raw := a.Float64Values()
		for i := range n {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			cur *= raw[i]
			out[i] = cur
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("series: CumProd unsupported for dtype %s", s.DType())
	}
	return FromFloat64(s.Name(), out, validOrNil(valid), WithAllocator(cfg.alloc))
}

// CumCount returns a running count of non-null values. Output is an
// int64 Series with no nulls.
func (s *Series) CumCount(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]int64, n)
	count := int64(0)
	for i := range n {
		if chunk.NullN() == 0 || chunk.IsValid(i) {
			count++
		}
		out[i] = count
	}
	return FromInt64(s.Name(), out, nil, WithAllocator(cfg.alloc))
}

// PctChange returns the percent change between each row and the
// previous row (or between row i and row i-periods for non-default
// periods). Output is float64; leading rows are null.
func (s *Series) PctChange(periods int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]float64, n)
	valid := make([]bool, n)
	toFloat := func(i int) (float64, bool) {
		switch a := chunk.(type) {
		case *array.Int64:
			if a.NullN() > 0 && !a.IsValid(i) {
				return 0, false
			}
			return float64(a.Value(i)), true
		case *array.Float64:
			if a.NullN() > 0 && !a.IsValid(i) {
				return 0, false
			}
			return a.Value(i), true
		}
		return 0, false
	}
	for i := range n {
		j := i - periods
		if j < 0 || j >= n {
			continue
		}
		cur, okC := toFloat(i)
		prev, okP := toFloat(j)
		if !okC || !okP || prev == 0 {
			continue
		}
		out[i] = cur/prev - 1
		valid[i] = true
	}
	return FromFloat64(s.Name(), out, validOrNil(valid), WithAllocator(cfg.alloc))
}

// Mode returns the most frequent value(s). Ties produce multiple
// rows. Nulls are counted only if all non-null values would tie with
// the null count. Empty input returns an empty Series of the same
// dtype.
func (s *Series) Mode(opts ...Option) (*Series, error) {
	u, err := s.Unique(opts...)
	if err != nil {
		return nil, err
	}
	defer u.Release()
	// Build a frequency table by scanning the original.
	chunk := s.Chunk(0)
	switch a := chunk.(type) {
	case *array.Int64:
		counts := map[int64]int{}
		for i := range a.Len() {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			counts[a.Value(i)]++
		}
		return modeTopInt64(s.Name(), counts, opts)
	case *array.Float64:
		counts := map[float64]int{}
		for i := range a.Len() {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			counts[a.Value(i)]++
		}
		return modeTopFloat64(s.Name(), counts, opts)
	case *array.String:
		counts := map[string]int{}
		for i := range a.Len() {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			counts[a.Value(i)]++
		}
		return modeTopString(s.Name(), counts, opts)
	}
	return nil, fmt.Errorf("series: Mode unsupported for dtype %s", s.DType())
}

// modeTop* pick every key that ties for the max count.
func modeTopInt64(name string, counts map[int64]int, opts []Option) (*Series, error) {
	maxN := 0
	for _, c := range counts {
		if c > maxN {
			maxN = c
		}
	}
	var out []int64
	for k, c := range counts {
		if c == maxN {
			out = append(out, k)
		}
	}
	return FromInt64(name, out, nil, opts...)
}

func modeTopFloat64(name string, counts map[float64]int, opts []Option) (*Series, error) {
	maxN := 0
	for _, c := range counts {
		if c > maxN {
			maxN = c
		}
	}
	var out []float64
	for k, c := range counts {
		if c == maxN {
			out = append(out, k)
		}
	}
	return FromFloat64(name, out, nil, opts...)
}

func modeTopString(name string, counts map[string]int, opts []Option) (*Series, error) {
	maxN := 0
	for _, c := range counts {
		if c > maxN {
			maxN = c
		}
	}
	var out []string
	for k, c := range counts {
		if c == maxN {
			out = append(out, k)
		}
	}
	return FromString(name, out, nil, opts...)
}

// validOrNil returns valid if any slot is false, otherwise nil (the
// conventional "no nulls" marker accepted by From* constructors).
func validOrNil(valid []bool) []bool {
	for _, v := range valid {
		if !v {
			return valid
		}
	}
	return nil
}
