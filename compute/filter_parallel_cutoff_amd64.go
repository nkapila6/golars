//go:build amd64

package compute

// parallelFilterCutoff is the row count below which fusedFilterXXX
// stays single-threaded. On amd64 (i7-10700 measured) parallel at 262K
// wins over serial 14 GB/s vs 6 GB/s - goroutine startup cost is
// amortised across the L3-miss stall.
const parallelFilterCutoff = 256 * 1024
