//go:build amd64 && !noasm

package compute

import (
	"unsafe"

	"golang.org/x/sys/cpu"

	"github.com/Gaurav-Gosain/golars/series"
)

// init registers the SIMD acceleration hooks for series.FillNull. The
// AVX2 asm kernel in fillnull_avx2_amd64.s handles the fast path when
// the CPU reports AVX2 support; older x86 falls back to the pure-Go
// branchless blend in series/fillnull.go.
//
// int64 and float64 share the same kernel: the bit-blend doesn't care
// whether the 64-bit payload is a signed integer or an IEEE float,
// only that it's 8 bytes per lane. We cast the float slice to an
// int64 view via unsafe - no allocation, no copy.
func init() {
	if !cpu.X86.HasAVX2 {
		return
	}
	series.FillNullInt64Accel = simdFillNullInt64AVX2
	series.FillNullFloat64Accel = func(condBits []byte, src []float64, lit float64, out []float64) int {
		srcI := unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(src))), len(src))
		outI := unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(out))), len(out))
		return simdFillNullInt64AVX2(condBits, srcI, *(*int64)(unsafe.Pointer(&lit)), outI)
	}
}
