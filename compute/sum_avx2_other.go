//go:build !amd64 || noasm

package compute

// Stubs for the AVX2-prefetch reduction kernels on platforms where the
// asm isn't compiled (arm64, noasm builds, non-amd64). hasAVX2Prefetch
// is always false so the kernel functions are never actually called;
// they exist only to satisfy the type checker in agg.go's dispatch.

const hasAVX2Prefetch = false

func simdSumInt64AVX2Prefetch(a []int64) int64 {
	panic("unreachable: hasAVX2Prefetch is false on this platform")
}

func simdSumFloat64AVX2Prefetch(a []float64) float64 {
	panic("unreachable: hasAVX2Prefetch is false on this platform")
}
