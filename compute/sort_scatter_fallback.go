//go:build !amd64 || noasm

package compute

// Non-amd64 fallback: direct Go implementations of the scatter hooks.
// The parallel radix path checks for these via a nil-check on
// scatterScalar / scatterWithPrefetch; on arm64 and other arches
// we set them to a pure-Go loop so the call site compiles cleanly.

// simdRadixScatter2AVX2 is a Go fallback for non-amd64.
func simdRadixScatter2AVX2(src, dst []uint64, off *[2048]int, shift, mask uint64) int {
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

// simdRadixScatter2PrefetchAVX2 is a Go fallback for non-amd64.
func simdRadixScatter2PrefetchAVX2(src, dst []uint64, off *[2048]int, shift, mask uint64) int {
	return simdRadixScatter2AVX2(src, dst, off, shift, mask)
}
