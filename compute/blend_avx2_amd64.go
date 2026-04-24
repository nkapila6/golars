//go:build goexperiment.simd && amd64

package compute

import "unsafe"

// simdBlendInt64AVX2 blends aVals and bVals into out using condBits as
// the select mask. Processes 8 int64s per iteration via two AVX2
// vpblendvb instructions. Returns the number of elements written
// (always a multiple of 8 ≤ n).
//
// Scalar tail handled by the caller.
func simdBlendInt64AVX2(condBits []byte, aVals, bVals, out []int64) int

// unsafeF64toI64 reinterprets a []float64 as []int64 with the same
// pointer and length. Used by the blend kernel: since blending is
// purely a bit-copy, the float representation doesn't matter.
func unsafeF64toI64(v []float64) []int64 {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(&v[0])), len(v))
}
