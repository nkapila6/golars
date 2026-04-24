//go:build amd64 && !noasm

package compute

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// TestScatterPrefetchCorrectness: the 2-way PREFETCHT0-tagged asm
// scatter must produce identical bucket-ordered output to the
// reference bucket-sort for a range of sizes. The kernel is the
// production scatter used by the serial float/int64 radix paths, so
// we sweep sizes that straddle the 2-way unroll tail.
func TestScatterPrefetchCorrectness(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 7, 64, 127, 128, 1023, 1024, 16384, 65537} {
		r := rand.New(rand.NewPCG(42, 43))
		src := make([]uint64, n)
		for i := range src {
			src[i] = r.Uint64()
		}
		var ref [256][]uint64
		for _, v := range src {
			ref[v&0xFF] = append(ref[v&0xFF], v)
		}
		want := make([]uint64, 0, n)
		for b := range 256 {
			want = append(want, ref[b]...)
		}

		got := make([]uint64, n)
		counts := make([]int, 256)
		for _, v := range src {
			counts[v&0xFF]++
		}
		scatterUint64_8_prefetch2(src, got, counts, 0)
		if !slices.Equal(got, want) {
			t.Fatalf("n=%d 2-way mismatch", n)
		}
	}
}
