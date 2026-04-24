//go:build !amd64 || noasm

package compute

// scatterUint64_8_prefetch2: no-prefetch fallback that forwards to the
// generic scatter. Keeps the serial float radix dispatch code
// platform-free.
func scatterUint64_8_prefetch2(src, dst []uint64, counts []int, byteIdx uint) {
	scatterUint64(src, dst, counts, byteIdx*8, 0xFF)
}

// scatterInt64_8_prefetch2: sign-XOR variant fallback for non-amd64.
// Does the XOR in Go then invokes the generic uint64 scatter.
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
	const signBit uint64 = 1 << 63
	shift := byteIdx * 8
	for i := 0; i < n; i++ {
		v := src[i]
		d := int((uint64(v) ^ signBit) >> shift & 0xFF)
		p := counts[d]
		counts[d] = p + 1
		dst[p] = v
	}
}
