//go:build arm64 && !noasm

package compute

import (
	"unsafe"

	"github.com/Gaurav-Gosain/golars/series"
)

// simdFillNullInt64NEON writes out[i] = aVals[i] when validity bit i
// is set, else lit. Implemented in fillnull_neon_arm64.s. Returns
// elements written (always a multiple of 8).
func simdFillNullInt64NEON(condBits []byte, aVals []int64, lit int64, out []int64) int

// init registers the NEON kernel as series' acceleration hook. Unlike
// the amd64 path there's no runtime feature check - every arm64 chip
// shipping in the last decade has NEON, so the v8.0 baseline is
// safe. Float64 uses the same kernel via an int64 reinterpret.
func init() {
	series.FillNullInt64Accel = simdFillNullInt64NEON
	series.FillNullFloat64Accel = func(condBits []byte, src []float64, lit float64, out []float64) int {
		srcI := unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(src))), len(src))
		outI := unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(out))), len(out))
		return simdFillNullInt64NEON(condBits, srcI, *(*int64)(unsafe.Pointer(&lit)), outI)
	}
}
