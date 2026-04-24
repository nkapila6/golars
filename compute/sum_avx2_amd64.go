//go:build amd64 && !noasm

package compute

import "golang.org/x/sys/cpu"

// simdSumInt64AVX2Prefetch is the hand-rolled AVX2 int64 sum with
// software prefetch 4 cache lines ahead. Implementation in
// sum_avx2_amd64.s. Caller must ensure AVX2 is available; otherwise
// fall back to simdSumInt64.
func simdSumInt64AVX2Prefetch(a []int64) int64

// simdSumFloat64AVX2Prefetch is the float64 analogue.
func simdSumFloat64AVX2Prefetch(a []float64) float64

// hasAVX2Prefetch reports whether the CPU supports the instructions
// used by the AVX2 prefetch-unrolled kernels. AVX2 has been standard
// since Haswell (2013); we still gate so -tags noasm or very old CPUs
// don't panic on unsupported opcodes.
var hasAVX2Prefetch = cpu.X86.HasAVX2
