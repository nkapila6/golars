//go:build arm64 && !noasm

package compute

// simdCastInt64ToFloat64NT converts int64 values to float64 using
// NEON's SCVTF.2D, 2 lanes per instruction with 4-way unroll.
// Scalar tail handles any trailing ragged element.
//
// Overrides the scalar fallback in add_nt_fallback.go on arm64; the
// build tags in that file exclude arm64 when -tags noasm is absent.
func simdCastInt64ToFloat64NT(out []float64, src []int64)

// simdCastInt64ToFloat64AVX2 is a no-op on arm64 (NEON). castInt64ToFloat64
// only calls it when hasCastI64ToF64AVX2 is true, which is false on arm64.
func simdCastInt64ToFloat64AVX2(out []float64, src []int64) {
	simdCastInt64ToFloat64NT(out, src)
}

// hasCastI64ToF64AVX2 is false on arm64 so the gate in castInt64ToFloat64
// never picks the AVX2 path; NEON builds keep the scalar fallback loop for
// small N (consistently ~15 GB/s on Apple Silicon).
const hasCastI64ToF64AVX2 = false

// castNTThreshold is the minimum size at which castInt64ToFloat64
// routes through the NEON kernel. On ARM64 the kernel has no NT-store
// magic; it just has a tight SCVTF loop that beats Go's autovec from
// 128K upward (smaller than that, the scalar parallel fallback wins
// because the autovec chunks are already L1-resident).
const castNTThreshold = 128 * 1024
