//go:build goexperiment.simd && amd64

package compute

import (
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSimdFillNullInt64AVX2(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	sizes := []int{16, 64, 128, 1_000, 65_536, 131_072, 262_144}
	const fill int64 = -7
	for _, n := range sizes {
		src := make([]int64, n)
		for i := range src {
			src[i] = int64(i) * 3
		}
		// Random validity bitmap at ~30% null rate.
		nBytes := (n + 7) / 8
		bits := make([]byte, nBytes)
		for i := range n {
			if r.Float64() >= 0.3 {
				bits[i>>3] |= 1 << uint(i&7)
			}
		}
		out := make([]int64, n)
		done := simdFillNullInt64AVX2(bits, src, fill, out)
		if done%8 != 0 {
			t.Errorf("n=%d: done=%d is not a multiple of 8", n, done)
		}
		// Verify the first `done` elements.
		for i := range done {
			want := fill
			if bits[i>>3]&(1<<uint(i&7)) != 0 {
				want = src[i]
			}
			if out[i] != want {
				t.Fatalf("n=%d i=%d: out=%d, want=%d", n, i, out[i], want)
			}
		}
	}
	_ = memory.DefaultAllocator
}

// TestSimdFillNullInt64AVX2Edges exercises misaligned tails and tiny
// inputs to make sure the done-return boundary is correct.
func TestSimdFillNullInt64AVX2Edges(t *testing.T) {
	cases := []struct {
		n       int
		expDone int
	}{
		{0, 0},
		{1, 0},
		{7, 0},
		{8, 8},
		{15, 8},
		{16, 16},
		{17, 16},
		{23, 16},
		{24, 24},
	}
	for _, tc := range cases {
		src := make([]int64, tc.n)
		for i := range src {
			src[i] = int64(i + 1)
		}
		bits := make([]byte, (tc.n+7)/8)
		for i := range bits {
			bits[i] = 0xAA // alternating
		}
		out := make([]int64, tc.n)
		got := simdFillNullInt64AVX2(bits, src, -42, out)
		if got != tc.expDone {
			t.Errorf("n=%d: done=%d, want %d", tc.n, got, tc.expDone)
		}
	}
}
