//go:build amd64 && !noasm

package compute

import (
	"math/rand/v2"
	"testing"
)

// goScatter2 replicates the Go 2-way scatter loop for head-to-head
// correctness and bench comparison against the asm kernels.
func goScatter2(src, dst []uint64, off *[2048]int, shift, mask uint64) int {
	n := len(src)
	i := 0
	for ; i+1 < n; i += 2 {
		v0 := src[i]
		v1 := src[i+1]
		d0 := int(v0 >> shift & mask)
		d1 := int(v1 >> shift & mask)
		p0 := off[d0]
		off[d0] = p0 + 1
		p1 := off[d1]
		off[d1] = p1 + 1
		dst[p0] = v0
		dst[p1] = v1
	}
	return i
}

// buildScatterInput prepares input/output slices + offset array to
// match the state entering a single pass of the LSD radix.
func buildScatterInput(n int, shift, mask uint64) (src, dst []uint64, off, ref [2048]int) {
	r := rand.New(rand.NewPCG(42, 43))
	src = make([]uint64, n)
	for i := range src {
		src[i] = r.Uint64()
	}
	// Histogram the source so `off` is the prefix sum, matching
	// what radixSortUint64FromFloatParallel would set up.
	var hist [2048]int
	for _, v := range src {
		hist[(v>>shift)&mask]++
	}
	total := 0
	for d := 0; d < 2048; d++ {
		off[d] = total
		ref[d] = total
		total += hist[d]
	}
	dst = make([]uint64, n)
	return
}

func TestScatterAsmMatchesGo(t *testing.T) {
	const n = 65537
	const shift = 11
	const mask = 0x7FF
	// Run Go, asm no-prefetch, asm with-prefetch, compare outputs.
	srcGo, dstGo, offGo, _ := buildScatterInput(n, shift, mask)
	goScatter2(srcGo, dstGo, &offGo, shift, mask)

	srcA, dstA, offA, _ := buildScatterInput(n, shift, mask)
	simdRadixScatter2AVX2(srcA, dstA, &offA, shift, mask)

	srcP, dstP, offP, _ := buildScatterInput(n, shift, mask)
	simdRadixScatter2PrefetchAVX2(srcP, dstP, &offP, shift, mask)

	for i := 0; i+1 < n; i++ {
		if dstGo[i] != dstA[i] {
			t.Fatalf("asm diverges from Go at %d: %x vs %x", i, dstA[i], dstGo[i])
		}
		if dstGo[i] != dstP[i] {
			t.Fatalf("asm-prefetch diverges from Go at %d: %x vs %x", i, dstP[i], dstGo[i])
		}
	}
}

func benchScatter(b *testing.B, n int, fn func(src, dst []uint64, off *[2048]int, shift, mask uint64) int) {
	const shift = 11
	const mask = 0x7FF
	src, dst, offBase, _ := buildScatterInput(n, shift, mask)
	b.ReportAllocs()
	b.SetBytes(int64(n * 8))
	b.ResetTimer()
	for range b.N {
		off := offBase
		fn(src, dst, &off, shift, mask)
	}
}

func BenchmarkScatterGo_1M(b *testing.B)         { benchScatter(b, 1<<20, goScatter2) }
func BenchmarkScatterAsm_1M(b *testing.B)        { benchScatter(b, 1<<20, simdRadixScatter2AVX2) }
func BenchmarkScatterAsmPre_1M(b *testing.B)     { benchScatter(b, 1<<20, simdRadixScatter2PrefetchAVX2) }
func BenchmarkScatterGo_262k(b *testing.B)       { benchScatter(b, 262144, goScatter2) }
func BenchmarkScatterAsm_262k(b *testing.B)      { benchScatter(b, 262144, simdRadixScatter2AVX2) }
func BenchmarkScatterAsmPre_262k(b *testing.B)   { benchScatter(b, 262144, simdRadixScatter2PrefetchAVX2) }
