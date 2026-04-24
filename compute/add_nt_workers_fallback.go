//go:build !amd64 && !arm64

package compute

// addFloat64NTWorkers fallback for architectures without a tuned
// kernel. Use the runtime-inferred worker count directly.
func addFloat64NTWorkers(par int) int {
	if par <= 0 {
		return 1
	}
	return par
}
