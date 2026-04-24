//go:build (!amd64 || noasm) && (!arm64 || noasm)

package compute

// simdCastInt64ToFloat64NT is the generic scalar fallback. The amd64
// asm version lives in add_nt_amd64.s; the arm64 NEON version lives
// in cast_arm64.s. Every other platform lands here.
func simdCastInt64ToFloat64NT(out []float64, src []int64) {
	for i := range out {
		out[i] = float64(src[i])
	}
}

// simdCastInt64ToFloat64AVX2 is a no-op stub on non-amd64/noasm builds.
// The hot path in cast.go always checks the platform before calling.
func simdCastInt64ToFloat64AVX2(out []float64, src []int64) {
	for i := range out {
		out[i] = float64(src[i])
	}
}

// castNTThreshold is effectively unreachable for the fallback: the
// scalar kernel has no bandwidth advantage over the autovec parallel
// path, so never switch. 1GiB exceeds the largest array sizes golars
// is expected to handle in a single chunk.
const castNTThreshold = 1 << 30

// hasCastI64ToF64AVX2 is false on fallback builds: castInt64ToFloat64
// never takes the AVX2 branch.
const hasCastI64ToF64AVX2 = false
