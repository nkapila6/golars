//go:build !amd64 || noasm

// The AArch64 codegen in Go's compiler auto-vectorizes these simple
// loops well enough that hand-rolled NEON asm came in 0.85x slower on
// M3 Pro at memory-bound sizes. The scalar fallback is the fastest
// path on arm64 today; revisit if the Go compiler regresses or a
// NEON-specific win (e.g. prefetch hints) materialises.

package compute

// simdAddInt64NT is a scalar fallback where the AVX2 asm isn't available.
func simdAddInt64NT(out, a, b []int64) {
	for i := range out {
		out[i] = a[i] + b[i]
	}
}

func simdAddFloat64NT(out, a, b []float64) {
	for i := range out {
		out[i] = a[i] + b[i]
	}
}

func simdMulInt64NT(out, a, b []int64) {
	for i := range out {
		out[i] = a[i] * b[i]
	}
}

