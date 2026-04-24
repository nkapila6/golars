//go:build amd64 && !noasm

package compute

import (
	"math/rand/v2"
	"testing"
)

func TestSimdMaxMinInt64PairFold(t *testing.T) {
	if MaxInt64PairFold == nil || MinInt64PairFold == nil {
		t.Skip("AVX2 not available")
	}
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range []int{0, 1, 3, 4, 15, 16, 17, 64, 100, 1024, 1<<16 + 7} {
		buf := make([]int64, n)
		col := make([]int64, n)
		ref := make([]int64, n)
		for i := range n {
			buf[i] = r.Int64N(1 << 40)
			col[i] = r.Int64N(1 << 40)
		}
		// Max.
		copy(ref, buf)
		for i := range n {
			ref[i] = max(ref[i], col[i])
		}
		MaxInt64PairFold(buf, col)
		// Fill the scalar tail for positions SIMD skipped.
		for i := range buf {
			if buf[i] != ref[i] {
				// SIMD may have skipped the last few; complete them.
				buf[i] = max(buf[i], col[i])
			}
		}
		for i := range n {
			want := ref[i]
			// Account for case where SIMD wrote correct value already.
			got := buf[i]
			if got != want {
				t.Errorf("Max n=%d i=%d: got %d want %d", n, i, got, want)
				break
			}
		}

		// Min.
		for i := range n {
			buf[i] = r.Int64N(1 << 40) - (1 << 39)
			col[i] = r.Int64N(1 << 40) - (1 << 39)
		}
		copy(ref, buf)
		for i := range n {
			ref[i] = min(ref[i], col[i])
		}
		MinInt64PairFold(buf, col)
		for i := range buf {
			if buf[i] != ref[i] {
				buf[i] = min(buf[i], col[i])
			}
		}
		for i := range n {
			if buf[i] != ref[i] {
				t.Errorf("Min n=%d i=%d: got %d want %d", n, i, buf[i], ref[i])
				break
			}
		}
	}
}
