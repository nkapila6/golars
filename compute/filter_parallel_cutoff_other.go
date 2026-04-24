//go:build !amd64 && !arm64

package compute

// parallelFilterCutoff falls back to a conservative 1M on untuned
// platforms - until we profile there, defaulting to serial avoids
// regressions from goroutine overhead.
const parallelFilterCutoff = 1024 * 1024
