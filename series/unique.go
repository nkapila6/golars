package series

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/intmap"
)

// Unique returns a new Series containing the distinct non-null values of s
// in first-occurrence order. Nulls are collapsed into a single null entry
// appended to the end if present. Mirrors polars' Series.unique(maintain_order=true).
//
// For int64 and float64 inputs an open-addressing int64 hash backs the
// dedup so the operation is O(n). Other dtypes fall back to a map.
func (s *Series) Unique(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	switch a := chunk.(type) {
	case *array.Int64:
		return uniqueInt64(s.Name(), a, cfg.alloc)
	case *array.Int32:
		return uniqueInt32(s.Name(), a, cfg.alloc)
	case *array.Float64:
		return uniqueFloat64(s.Name(), a, cfg.alloc)
	case *array.Boolean:
		return uniqueBool(s.Name(), a, cfg.alloc)
	case *array.String:
		return uniqueString(s.Name(), a, cfg.alloc)
	}
	return nil, errUniqueUnsupported(s.DType())
}

func uniqueInt64(name string, arr *array.Int64, mem memory.Allocator) (*Series, error) {
	n := arr.Len()
	seen := intmap.New(max(n/4, 16))
	defer seen.Release()
	out := make([]int64, 0, n/4)
	hasNull := false
	raw := arr.Int64Values()
	for i := range n {
		if arr.IsNull(i) {
			hasNull = true
			continue
		}
		v := raw[i]
		if _, inserted := seen.InsertOrGet(v, int32(len(out))); inserted {
			out = append(out, v)
		}
	}
	if hasNull {
		valid := make([]bool, len(out)+1)
		for i := range out[:len(valid)-1] {
			valid[i] = true
		}
		outVals := append(out, 0)
		return FromInt64(name, outVals, valid, WithAllocator(mem))
	}
	return FromInt64(name, out, nil, WithAllocator(mem))
}

func uniqueInt32(name string, arr *array.Int32, mem memory.Allocator) (*Series, error) {
	n := arr.Len()
	seen := intmap.New(max(n/4, 16))
	defer seen.Release()
	out := make([]int32, 0, n/4)
	hasNull := false
	raw := arr.Int32Values()
	for i := range n {
		if arr.IsNull(i) {
			hasNull = true
			continue
		}
		v := raw[i]
		if _, inserted := seen.InsertOrGet(int64(v), int32(len(out))); inserted {
			out = append(out, v)
		}
	}
	if hasNull {
		valid := make([]bool, len(out)+1)
		for i := range out[:len(valid)-1] {
			valid[i] = true
		}
		outVals := append(out, 0)
		return FromInt32(name, outVals, valid, WithAllocator(mem))
	}
	return FromInt32(name, out, nil, WithAllocator(mem))
}

func uniqueFloat64(name string, arr *array.Float64, mem memory.Allocator) (*Series, error) {
	// Use a bit-reinterpret so +0 and -0 collapse and NaN handling is
	// consistent (all NaN payloads become the same canonical NaN bucket).
	n := arr.Len()
	seen := intmap.New(max(n/4, 16))
	defer seen.Release()
	out := make([]float64, 0, n/4)
	hasNull := false
	raw := arr.Float64Values()
	nanAdded := false
	for i := range n {
		if arr.IsNull(i) {
			hasNull = true
			continue
		}
		v := raw[i]
		if v != v {
			if !nanAdded {
				out = append(out, math.NaN())
				nanAdded = true
			}
			continue
		}
		// Canonicalise -0 to +0 so they dedup.
		if v == 0 {
			v = 0
		}
		bits := int64(math.Float64bits(v))
		if _, inserted := seen.InsertOrGet(bits, int32(len(out))); inserted {
			out = append(out, v)
		}
	}
	if hasNull {
		valid := make([]bool, len(out)+1)
		for i := range out[:len(valid)-1] {
			valid[i] = true
		}
		outVals := append(out, 0)
		return FromFloat64(name, outVals, valid, WithAllocator(mem))
	}
	return FromFloat64(name, out, nil, WithAllocator(mem))
}

func uniqueBool(name string, arr *array.Boolean, mem memory.Allocator) (*Series, error) {
	n := arr.Len()
	seenTrue := false
	seenFalse := false
	hasNull := false
	var order []bool
	for i := range n {
		if arr.IsNull(i) {
			hasNull = true
			continue
		}
		v := arr.Value(i)
		if v && !seenTrue {
			seenTrue = true
			order = append(order, true)
		} else if !v && !seenFalse {
			seenFalse = true
			order = append(order, false)
		}
		if seenTrue && seenFalse && hasNull {
			break
		}
	}
	if hasNull {
		valid := make([]bool, len(order)+1)
		for i := range order {
			valid[i] = true
		}
		outVals := append(order, false)
		return FromBool(name, outVals, valid, WithAllocator(mem))
	}
	return FromBool(name, order, nil, WithAllocator(mem))
}

func uniqueString(name string, arr *array.String, mem memory.Allocator) (*Series, error) {
	n := arr.Len()
	seen := make(map[string]struct{}, n/4)
	out := make([]string, 0, n/4)
	hasNull := false
	for i := range n {
		if arr.IsNull(i) {
			hasNull = true
			continue
		}
		v := arr.Value(i)
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	if hasNull {
		valid := make([]bool, len(out)+1)
		for i := range out {
			valid[i] = true
		}
		outVals := append(out, "")
		return FromString(name, outVals, valid, WithAllocator(mem))
	}
	return FromString(name, out, nil, WithAllocator(mem))
}

func errUniqueUnsupported(dt interface{ String() string }) error {
	return &uniqueError{dt: dt.String()}
}

type uniqueError struct{ dt string }

func (e *uniqueError) Error() string { return "series: Unique unsupported for dtype " + e.dt }

// NUnique returns the number of distinct non-null values.
func (s *Series) NUnique() (int, error) {
	u, err := s.Unique()
	if err != nil {
		return 0, err
	}
	defer u.Release()
	// Subtract one if the unique series has a null entry (polars' NUnique
	// excludes nulls).
	n := u.Len() - u.NullCount()
	return n, nil
}
