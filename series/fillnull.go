package series

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/mempool"
)


// FillNullInt64Accel is the SIMD acceleration hook for the int64
// FillNull kernel. compute.init registers an AVX2/NEON implementation
// when the build tags and runtime feature detection agree. Set to nil
// means "no acceleration available" - the pure-Go single-pass blend
// below is used.
//
// The registered function should write src[i] to out[i] for every
// position where condBits bit i is set, and lit elsewhere, returning
// the number of elements handled (always a multiple of 8 for AVX2,
// multiple of 2 for NEON). Caller handles the scalar tail.
var FillNullInt64Accel func(condBits []byte, src []int64, lit int64, out []int64) int

// FillNullFloat64Accel is the float64 counterpart. float64 fills blend
// identically to int64 at the bit level, so the same kernel handles
// both once the values are reinterpreted.
var FillNullFloat64Accel func(condBits []byte, src []float64, lit float64, out []float64) int

// fillNullInt64SinglePass writes src[i] to out[i] when valid, v when
// null, in a single pass over both buffers. Offset=0 fast path.
//
// Inner loop decodes 8 rows per bitmap byte: for each bit we select
// src[i] or v with a branchless mask derived from the bit. Go's
// compiler turns `(src[i] & mask) | (v & ^mask)` into a small blend
// on both amd64 and arm64; no temporaries, no memcpy in front.
//
// We avoid the memcpy+patch approach because at ~30% nulls it pays
// for an 8MB copy we immediately overwrite for a third of positions.
// Single-pass is one read of src, one read of validity, one write of
// out - the minimum traffic possible.
func fillNullInt64SinglePass(out, src []int64, v int64, bits []byte, offset, n int) {
	if n == 0 {
		return
	}
	if offset == 0 {
		i := 0
		for ; i+8 <= n; i += 8 {
			b := bits[i>>3]
			switch b {
			case 0xFF:
				// All valid - plain copy.
				copy(out[i:i+8], src[i:i+8])
			case 0x00:
				// All null - broadcast fill.
				out[i+0], out[i+1], out[i+2], out[i+3] = v, v, v, v
				out[i+4], out[i+5], out[i+6], out[i+7] = v, v, v, v
			default:
				// Mixed. True-branchless blend: (src & mask) | (v & ^mask)
				// where mask is all-1s for valid, all-0s for null. Go's
				// compiler emits plain AND/OR/SUB here - no cmov, no
				// branches - so random null patterns don't mispredict.
				_ = src[i+7]
				_ = out[i+7]
				m0 := -int64(b & 1)
				m1 := -int64((b >> 1) & 1)
				m2 := -int64((b >> 2) & 1)
				m3 := -int64((b >> 3) & 1)
				m4 := -int64((b >> 4) & 1)
				m5 := -int64((b >> 5) & 1)
				m6 := -int64((b >> 6) & 1)
				m7 := -int64((b >> 7) & 1)
				out[i+0] = (src[i+0] & m0) | (v &^ m0)
				out[i+1] = (src[i+1] & m1) | (v &^ m1)
				out[i+2] = (src[i+2] & m2) | (v &^ m2)
				out[i+3] = (src[i+3] & m3) | (v &^ m3)
				out[i+4] = (src[i+4] & m4) | (v &^ m4)
				out[i+5] = (src[i+5] & m5) | (v &^ m5)
				out[i+6] = (src[i+6] & m6) | (v &^ m6)
				out[i+7] = (src[i+7] & m7) | (v &^ m7)
			}
		}
		for ; i < n; i++ {
			if bits[i>>3]&(1<<uint(i&7)) != 0 {
				out[i] = src[i]
			} else {
				out[i] = v
			}
		}
		return
	}
	// Offset != 0 general case.
	for i := 0; i < n; i++ {
		abs := offset + i
		if bits[abs>>3]&(1<<uint(abs&7)) != 0 {
			out[i] = src[i]
		} else {
			out[i] = v
		}
	}
}

// fillNullFloat64SinglePass is the float64 mirror. Float64 has no
// bitwise ops so we alias both buffers as []uint64 and blend there;
// IEEE bit layout means `(srcU & m) | (vU &^ m)` reconstructs a
// float64 identical to the result of a conditional assignment, with
// no branches and no extra loads vs the int64 version.
func fillNullFloat64SinglePass(out, src []float64, v float64, bits []byte, offset, n int) {
	if n == 0 {
		return
	}
	srcU := unsafe.Slice((*uint64)(unsafe.Pointer(&src[0])), len(src))
	outU := unsafe.Slice((*uint64)(unsafe.Pointer(&out[0])), len(out))
	vU := math.Float64bits(v)
	if offset == 0 {
		i := 0
		for ; i+8 <= n; i += 8 {
			b := bits[i>>3]
			switch b {
			case 0xFF:
				copy(outU[i:i+8], srcU[i:i+8])
			case 0x00:
				outU[i+0], outU[i+1], outU[i+2], outU[i+3] = vU, vU, vU, vU
				outU[i+4], outU[i+5], outU[i+6], outU[i+7] = vU, vU, vU, vU
			default:
				_ = srcU[i+7]
				_ = outU[i+7]
				m0 := -uint64(b & 1)
				m1 := -uint64((b >> 1) & 1)
				m2 := -uint64((b >> 2) & 1)
				m3 := -uint64((b >> 3) & 1)
				m4 := -uint64((b >> 4) & 1)
				m5 := -uint64((b >> 5) & 1)
				m6 := -uint64((b >> 6) & 1)
				m7 := -uint64((b >> 7) & 1)
				outU[i+0] = (srcU[i+0] & m0) | (vU &^ m0)
				outU[i+1] = (srcU[i+1] & m1) | (vU &^ m1)
				outU[i+2] = (srcU[i+2] & m2) | (vU &^ m2)
				outU[i+3] = (srcU[i+3] & m3) | (vU &^ m3)
				outU[i+4] = (srcU[i+4] & m4) | (vU &^ m4)
				outU[i+5] = (srcU[i+5] & m5) | (vU &^ m5)
				outU[i+6] = (srcU[i+6] & m6) | (vU &^ m6)
				outU[i+7] = (srcU[i+7] & m7) | (vU &^ m7)
			}
		}
		for ; i < n; i++ {
			if bits[i>>3]&(1<<uint(i&7)) != 0 {
				outU[i] = srcU[i]
			} else {
				outU[i] = vU
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		abs := offset + i
		if bits[abs>>3]&(1<<uint(abs&7)) != 0 {
			outU[i] = srcU[i]
		} else {
			outU[i] = vU
		}
	}
}

// FillNull returns a new Series where every null is replaced with the
// given value. The value's Go type must match the Series dtype; mixing
// dtypes returns an error rather than silently coercing. Mirrors polars
// Series.fill_null(value).
//
// If the source has no nulls, a clone is returned: the method is always
// safe to call and always returns a fully-valid Series.
func (s *Series) FillNull(value any, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	// Route the hot output buffer through the shared pool when the
	// caller didn't supply a leak-checking allocator. Saves the
	// mallocgc of an n×8-byte buffer on each call; at 1M rows that
	// was ~60% of the total wall time for this kernel.
	alloc := mempool.Pooling(cfg.alloc)
	chunk := s.Chunk(0)
	n := chunk.Len()

	switch a := chunk.(type) {
	case *array.Int64:
		v, ok := toInt64(value)
		if !ok {
			return nil, fillTypeErr(s, "int64", value)
		}
		if a.NullN() == 0 {
			return s.Clone(), nil
		}
		raw := a.Int64Values()
		bits := a.NullBitmapBytes()
		off := a.Offset()
		return BuildInt64Direct(s.Name(), n, alloc, func(out []int64) {
			// SIMD acceleration requires offset == 0 because the
			// kernel treats bits as starting at bit 0 of the bitmap
			// buffer. Sliced views with offset > 0 fall back to the
			// pure-Go path that honours offset.
			if off == 0 && FillNullInt64Accel != nil {
				done := FillNullInt64Accel(bits, raw, v, out)
				// Tail: scalar single-pass handles the remainder.
				if done < n {
					fillNullInt64SinglePass(out[done:], raw[done:], v, bits[done>>3:], 0, n-done)
				}
				return
			}
			fillNullInt64SinglePass(out, raw, v, bits, off, n)
		})
	case *array.Float64:
		v, ok := toFloat64(value)
		if !ok {
			return nil, fillTypeErr(s, "float64", value)
		}
		if a.NullN() == 0 {
			return s.Clone(), nil
		}
		raw := a.Float64Values()
		bits := a.NullBitmapBytes()
		off := a.Offset()
		return BuildFloat64Direct(s.Name(), n, alloc, func(out []float64) {
			if off == 0 && FillNullFloat64Accel != nil {
				done := FillNullFloat64Accel(bits, raw, v, out)
				if done < n {
					fillNullFloat64SinglePass(out[done:], raw[done:], v, bits[done>>3:], 0, n-done)
				}
				return
			}
			fillNullFloat64SinglePass(out, raw, v, bits, off, n)
		})
	case *array.Int32:
		v, ok := toInt32(value)
		if !ok {
			return nil, fillTypeErr(s, "int32", value)
		}
		if a.NullN() == 0 {
			return s.Clone(), nil
		}
		raw := a.Int32Values()
		out := make([]int32, n)
		copy(out, raw)
		for i := range n {
			if a.IsNull(i) {
				out[i] = v
			}
		}
		return FromInt32(s.Name(), out, nil, WithAllocator(alloc))
	case *array.Boolean:
		v, ok := value.(bool)
		if !ok {
			return nil, fillTypeErr(s, "bool", value)
		}
		if a.NullN() == 0 {
			return s.Clone(), nil
		}
		out := make([]bool, n)
		for i := range n {
			if a.IsNull(i) {
				out[i] = v
			} else {
				out[i] = a.Value(i)
			}
		}
		return FromBool(s.Name(), out, nil, WithAllocator(alloc))
	case *array.String:
		v, ok := value.(string)
		if !ok {
			return nil, fillTypeErr(s, "string", value)
		}
		if a.NullN() == 0 {
			return s.Clone(), nil
		}
		out := make([]string, n)
		for i := range n {
			if a.IsNull(i) {
				out[i] = v
			} else {
				out[i] = a.Value(i)
			}
		}
		return FromString(s.Name(), out, nil, WithAllocator(alloc))
	}
	return nil, fmt.Errorf("series: FillNull unsupported for dtype %s", s.DType())
}

func fillTypeErr(s *Series, expect string, got any) error {
	return fmt.Errorf("series: FillNull on %s wants %s value, got %T",
		s.DType(), expect, got)
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	return 0, false
}

func toInt32(v any) (int32, bool) {
	switch x := v.(type) {
	case int32:
		return x, true
	case int:
		return int32(x), true
	case int64:
		return int32(x), true
	}
	return 0, false
}
