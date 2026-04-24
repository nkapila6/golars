//go:build amd64 && !noasm

package compute

import "unsafe"

// scatterUint64Prefetch2: 2-way unrolled asm scatter with PREFETCHT0
// hints. Exposes more ILP than the single-way kernel by overlapping
// two independent bucket lookups per iteration.
func scatterUint64Prefetch2(src *uint64, dst *uint64, n int, counts *[256]int, shift uint64)

// scatterUint64Prefetch2Any: general-mask variant of the 2-way
// prefetch scatter. Used by the 11-bit parallel radix (mask=0x7FF
// or 0x1FF for the last pass).
func scatterUint64Prefetch2Any(src *uint64, dst *uint64, n int, counts *int, shift, mask uint64)

// scatterUint64_8_prefetch2 is the Go-callable wrapper: prefix-sums
// counts then invokes the 2-way asm kernel.
func scatterUint64_8_prefetch2(src, dst []uint64, counts []int, byteIdx uint) {
	var offset int
	for i := range counts {
		c := counts[i]
		counts[i] = offset
		offset += c
	}
	n := len(src)
	if n == 0 {
		return
	}
	cts := (*[256]int)(unsafe.Pointer(&counts[0]))
	scatterUint64Prefetch2(&src[0], &dst[0], n, cts, uint64(byteIdx*8))
}
