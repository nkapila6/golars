//go:build goexperiment.simd && amd64

package compute

import (
	"math/rand/v2"
	"testing"
)

// buildFillNullInputs constructs a random int64 slice and a 30%-null
// bitmap of the given length. Used as input for FillNull benchmarks.
func buildFillNullInputs(n int) (src []int64, bits []byte) {
	r := rand.New(rand.NewPCG(7, 8))
	src = make([]int64, n)
	bits = make([]byte, (n+7)/8)
	for i := range n {
		src[i] = int64(i)
		if r.Float64() >= 0.3 {
			bits[i>>3] |= 1 << uint(i&7)
		}
	}
	return
}

func BenchmarkSimdFillNullInt64AVX2_1M(b *testing.B) {
	n := 1 << 20
	src, bits := buildFillNullInputs(n)
	out := make([]int64, n)
	b.ReportAllocs()
	b.SetBytes(int64(n * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		simdFillNullInt64AVX2(bits, src, -1, out)
	}
}

func BenchmarkSimdFillNullInt64AVX2_262k(b *testing.B) {
	n := 262_144
	src, bits := buildFillNullInputs(n)
	out := make([]int64, n)
	b.ReportAllocs()
	b.SetBytes(int64(n * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		simdFillNullInt64AVX2(bits, src, -1, out)
	}
}
