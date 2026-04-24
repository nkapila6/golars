//go:build amd64

package compute

// addFloat64NTWorkers caps the worker count used by the
// AddFloat64 non-temporal-store path on amd64.
//
// VMOVNTPD saturates DRAM write bandwidth at ~2 cores on modern DDR4
// desktops (measured on i7-10700: 3+ writers regress by 5-10% due to
// memory-controller contention). We ignore the incoming par hint and
// pin to 2 here.
func addFloat64NTWorkers(par int) int {
	_ = par
	return 2
}
