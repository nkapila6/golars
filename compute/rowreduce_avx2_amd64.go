//go:build amd64 && !noasm

package compute

// simdMaxInt64PairFoldAVX2 writes buf[i] = max(buf[i], col[i]) for the
// 16-aligned prefix, then a 4-aligned tail. Returns the number of
// elements processed (a multiple of 4 ≤ len(buf)).
func simdMaxInt64PairFoldAVX2(buf, col []int64) int

// simdMinInt64PairFoldAVX2 is the min counterpart.
func simdMinInt64PairFoldAVX2(buf, col []int64) int

// simdMaxInt64PairFoldNTAVX2 is the non-temporal variant: VMOVNTDQ
// stores bypass the cache hierarchy so the output writes cost 1x DRAM
// bandwidth instead of 2x (read-for-ownership + writeback). Only
// profitable when the output doesn't fit in L2, i.e. n >= ~128K on
// an 8 MB L2.
func simdMaxInt64PairFoldNTAVX2(buf, col []int64) int

// simdMinInt64PairFoldNTAVX2 is the NT-store min counterpart.
func simdMinInt64PairFoldNTAVX2(buf, col []int64) int

// MaxInt64PairFold is the exported hook used by dataframe.rowReduceInt64.
// When the CPU has AVX2 the SIMD kernel runs; scalar fallback covers
// pre-AVX2 hosts and any remainder past the SIMD-aligned prefix.
//
// Set at init() time in rowreduce_register_amd64.go.
var MaxInt64PairFold func(buf, col []int64) int

// MinInt64PairFold is the exported min hook.
var MinInt64PairFold func(buf, col []int64) int

// MaxInt64PairFoldNT / MinInt64PairFoldNT are the non-temporal-store
// variants. Callers should dispatch to these when the output buffer
// is too large to stay in L2 (n × 8 bytes > ~L2 / 2).
var MaxInt64PairFoldNT func(buf, col []int64) int
var MinInt64PairFoldNT func(buf, col []int64) int
