//go:build amd64 && !noasm

package compute

// simdRadixScatter2AVX2 and simdRadixScatter2PrefetchAVX2: inner-loop
// scatter for the radix sort. See sort_scatter_amd64.s for design notes.
//
// Both functions match the Go 2-way scalar scatter 1:1 - their only
// purpose is to give us a fixed instruction schedule and register
// pinning that the Go compiler sometimes loses under pressure. They
// are called from the parallel radix path after careful benchmarking
// (the prefetch variant regresses under 8-core load; kept accessible
// for the serial/single-thread paths).
func simdRadixScatter2AVX2(src, dst []uint64, off *[2048]int, shift, mask uint64) int

func simdRadixScatter2PrefetchAVX2(src, dst []uint64, off *[2048]int, shift, mask uint64) int
