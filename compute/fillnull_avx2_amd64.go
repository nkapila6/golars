//go:build amd64 && !noasm

package compute

// simdFillNullInt64AVX2 writes out[i] = aVals[i] when validity bit i
// is set, else lit. Returns the count of elements written (multiple
// of 8). Implemented in fillnull_avx2_amd64.s using VPBLENDVB against
// a LUT-expanded mask vector; streaming-store path engaged for large
// outputs.
func simdFillNullInt64AVX2(condBits []byte, aVals []int64, lit int64, out []int64) int
