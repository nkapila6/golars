//go:build amd64 && !noasm

package compute

import "unsafe"

// scatterInt64SignPrefetch2: asm 2-way scatter with sign-bit XOR for
// int64 radix passes. Values stored in dst are the original signed
// int64 bits; only the DIGIT extraction sees the sign-flipped form
// to preserve signed ordering after LSD sort.
func scatterInt64SignPrefetch2(src *int64, dst *int64, n int, counts *[256]int, shift uint64)

// scatterInt64_8_prefetch2 is the Go-callable wrapper. It prefix-sums
// counts in place then invokes the asm kernel.
func scatterInt64_8_prefetch2(src, dst []int64, counts []int, byteIdx uint) {
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
	scatterInt64SignPrefetch2(&src[0], &dst[0], n, cts, uint64(byteIdx*8))
}
