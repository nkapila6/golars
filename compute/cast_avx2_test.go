//go:build amd64 && !noasm

package compute

import (
	"math/rand/v2"
	"testing"
)

// Correctness: the AVX2 cast kernel must produce identical float64
// values to the reference scalar loop for every size including those
// that straddle the 8-unroll, 4-unroll, and 1-scalar tails.
func TestCastInt64ToFloat64AVX2Correctness(t *testing.T) {
	if !hasCastI64ToF64AVX2 {
		t.Skip("requires AVX2")
	}
	r := rand.New(rand.NewPCG(42, 43))
	sizes := []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 63, 64, 65,
		127, 128, 129, 255, 256, 1023, 1024, 1025, 16_384, 131_071}
	for _, n := range sizes {
		src := make([]int64, n)
		for i := range src {
			src[i] = r.Int64N(1 << 40)
			if r.IntN(4) == 0 {
				src[i] = -src[i]
			}
		}
		got := make([]float64, n)
		simdCastInt64ToFloat64AVX2(got, src)
		for i, s := range src {
			want := float64(s)
			if got[i] != want {
				t.Fatalf("n=%d i=%d: got %v, want %v", n, i, got[i], want)
			}
		}
	}
}
