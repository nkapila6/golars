package series

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// ArgTrue returns a UInt64 Series of positions where s is true. s
// must be a boolean Series. Mirrors polars' arg_true.
func (s *Series) ArgTrue() ([]int64, error) {
	chunk, ok := s.Chunk(0).(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("series: ArgTrue requires bool, got %s", s.DType())
	}
	n := chunk.Len()
	out := make([]int64, 0, n/2)
	for i := range n {
		if chunk.IsValid(i) && chunk.Value(i) {
			out = append(out, int64(i))
		}
	}
	return out, nil
}

// ArgUnique returns the indices of the first occurrence of each
// distinct value in s. Order preserves first-seen. Mirrors polars'
// arg_unique.
func (s *Series) ArgUnique() ([]int64, error) {
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]int64, 0, n)
	switch a := chunk.(type) {
	case *array.Int64:
		seen := map[int64]struct{}{}
		for i, v := range a.Int64Values() {
			if a.NullN() > 0 && a.IsNull(i) {
				continue
			}
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, int64(i))
		}
	case *array.Float64:
		// Use raw bit representation so NaN entries participate.
		seen := map[uint64]struct{}{}
		for i, v := range a.Float64Values() {
			if a.NullN() > 0 && a.IsNull(i) {
				continue
			}
			key := floatBits(v)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, int64(i))
		}
	case *array.Int32:
		seen := map[int32]struct{}{}
		for i, v := range a.Int32Values() {
			if a.NullN() > 0 && a.IsNull(i) {
				continue
			}
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, int64(i))
		}
	case *array.String:
		seen := map[string]struct{}{}
		for i := range n {
			if a.IsNull(i) {
				continue
			}
			v := a.Value(i)
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, int64(i))
		}
	case *array.Boolean:
		sawT, sawF := false, false
		for i := 0; i < n && !(sawT && sawF); i++ {
			if !a.IsValid(i) {
				continue
			}
			v := a.Value(i)
			if v && !sawT {
				sawT = true
				out = append(out, int64(i))
			} else if !v && !sawF {
				sawF = true
				out = append(out, int64(i))
			}
		}
	default:
		return nil, fmt.Errorf("series: ArgUnique unsupported for dtype %s", s.DType())
	}
	return out, nil
}

// PeakMax returns a boolean Series whose i-th position is true when
// s[i] is strictly greater than both neighbours. Edges are always
// false. Mirrors polars' peak_max.
func (s *Series) PeakMax(opts ...Option) (*Series, error) {
	return s.peakDirectional(true, opts)
}

// PeakMin returns a boolean Series whose i-th position is true when
// s[i] is strictly less than both neighbours.
func (s *Series) PeakMin(opts ...Option) (*Series, error) {
	return s.peakDirectional(false, opts)
}

func (s *Series) peakDirectional(isMax bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]bool, n)
	if n < 3 {
		return FromBool(s.Name(), out, nil, WithAllocator(cfg.alloc))
	}
	switch a := chunk.(type) {
	case *array.Float64:
		v := a.Float64Values()
		for i := 1; i < n-1; i++ {
			if !a.IsValid(i) || !a.IsValid(i-1) || !a.IsValid(i+1) {
				continue
			}
			if isMax {
				out[i] = v[i] > v[i-1] && v[i] > v[i+1]
			} else {
				out[i] = v[i] < v[i-1] && v[i] < v[i+1]
			}
		}
	case *array.Int64:
		v := a.Int64Values()
		for i := 1; i < n-1; i++ {
			if !a.IsValid(i) || !a.IsValid(i-1) || !a.IsValid(i+1) {
				continue
			}
			if isMax {
				out[i] = v[i] > v[i-1] && v[i] > v[i+1]
			} else {
				out[i] = v[i] < v[i-1] && v[i] < v[i+1]
			}
		}
	case *array.Int32:
		v := a.Int32Values()
		for i := 1; i < n-1; i++ {
			if !a.IsValid(i) || !a.IsValid(i-1) || !a.IsValid(i+1) {
				continue
			}
			if isMax {
				out[i] = v[i] > v[i-1] && v[i] > v[i+1]
			} else {
				out[i] = v[i] < v[i-1] && v[i] < v[i+1]
			}
		}
	default:
		return nil, fmt.Errorf("series: PeakMax/PeakMin unsupported for dtype %s", s.DType())
	}
	return FromBool(s.Name(), out, nil, WithAllocator(cfg.alloc))
}

// ApproxNUnique estimates the distinct-value count using HyperLogLog-
// style bucketing over a 64-bit fingerprint. For small series this is
// exact; for larger series it's an O(n) approximation with <2% error
// at k=14 registers. Mirrors polars' approx_n_unique semantics.
func (s *Series) ApproxNUnique() (int, error) {
	const (
		k = 14
		m = 1 << k
	)
	registers := make([]uint8, m)
	chunk := s.Chunk(0)
	n := chunk.Len()
	process := func(h uint64) {
		bucket := h >> (64 - k)
		// count leading zeros in the remaining 64-k bits, +1.
		remaining := h << k
		var lz uint8
		if remaining == 0 {
			lz = 64 - k + 1
		} else {
			lz = uint8(bits.LeadingZeros64(remaining)) + 1
		}
		if lz > registers[bucket] {
			registers[bucket] = lz
		}
	}
	switch a := chunk.(type) {
	case *array.Int64:
		for i, v := range a.Int64Values() {
			if a.NullN() > 0 && a.IsNull(i) {
				continue
			}
			process(mix64(uint64(v)))
		}
	case *array.Int32:
		for i, v := range a.Int32Values() {
			if a.NullN() > 0 && a.IsNull(i) {
				continue
			}
			process(mix64(uint64(uint32(v))))
		}
	case *array.Float64:
		for i, v := range a.Float64Values() {
			if a.NullN() > 0 && a.IsNull(i) {
				continue
			}
			process(mix64(floatBits(v)))
		}
	case *array.String:
		for i := range n {
			if a.IsNull(i) {
				continue
			}
			process(fnv64a(a.Value(i)))
		}
	default:
		return 0, fmt.Errorf("series: ApproxNUnique unsupported for dtype %s", s.DType())
	}
	return hllEstimate(registers, m), nil
}

// floatBits returns the raw IEEE-754 bit pattern so NaN and -0 hash
// distinctly, matching polars' treatment of floats as identifiers.
func floatBits(v float64) uint64 { return math.Float64bits(v) }

// mix64 is a splitmix-style hash for 64-bit integers.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func fnv64a(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

func hllEstimate(regs []uint8, m int) int {
	var sum float64
	zeros := 0
	for _, r := range regs {
		sum += 1.0 / float64(uint64(1)<<r)
		if r == 0 {
			zeros++
		}
	}
	alpha := 0.7213 / (1 + 1.079/float64(m))
	est := alpha * float64(m) * float64(m) / sum
	// Small-range linear counting correction.
	if est <= 2.5*float64(m) && zeros != 0 {
		est = float64(m) * math.Log(float64(m)/float64(zeros))
	}
	if est < 0 {
		return 0
	}
	return int(est + 0.5)
}
