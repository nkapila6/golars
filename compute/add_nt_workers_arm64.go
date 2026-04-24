//go:build arm64

package compute

// addFloat64NTWorkers picks a worker count for the arm64 AddFloat64
// large-N path.
//
// M-series chips have significantly more memory bandwidth than
// typical DDR4 desktops (M3 Pro: ~150 GB/s LPDDR5 vs ~25-40 GB/s on
// Intel Comet Lake). Going from 2 workers → 4 workers recovers ~40%
// on 1M-row adds because the Go fallback scalar loop per worker is
// not SIMD-accelerated and two workers under-utilise the DRAM
// channels.
//
// Capped at 4 because past that point the bandwidth saturates and the
// goroutine-sync overhead starts to bite.
func addFloat64NTWorkers(par int) int {
	if par <= 0 || par >= 4 {
		return 4
	}
	return par
}
