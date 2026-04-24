//go:build amd64 && !noasm

package compute

import "golang.org/x/sys/cpu"

// simdAddInt64NT writes out[i] = a[i] + b[i] using AVX2 streaming stores
// (VMOVNTDQ) so the write side bypasses L1 cache and avoids the
// read-for-ownership traffic that normally doubles DRAM write bandwidth.
// Requires len(out) == len(a) == len(b) and that out/a/b don't alias.
// For inputs shorter than 32 elements the caller should fall back to
// scalar.
func simdAddInt64NT(out, a, b []int64)

// simdAddFloat64NT is the float64 counterpart using VADDPD + VMOVNTPD.
func simdAddFloat64NT(out, a, b []float64)

// simdMulInt64NT multiplies two int64 slices with streaming MOVNTI stores.
// AVX2 lacks 64-bit packed multiply so the inner loop uses scalar IMUL
// instructions with non-temporal 8-byte stores.
func simdMulInt64NT(out, a, b []int64)

// simdCastInt64ToFloat64NT converts int64 → float64 with AVX2 streaming
// stores. Scalar CVTSI2SDQ × 4, packed into a YMM, then VMOVNTPD.
func simdCastInt64ToFloat64NT(out []float64, src []int64)

// simdCastInt64ToFloat64AVX2 is the cache-friendly AVX2 cast kernel.
// 8-wide unrolled (4 CVTs packed per YMM × 2 independent groups) with
// regular VMOVUPD stores. Used for cache-resident inputs where the
// streaming-store SFENCE overhead dominates.
func simdCastInt64ToFloat64AVX2(out []float64, src []int64)

// hasCastI64ToF64AVX2 gates the small-N AVX2 cast kernel. Resolved at
// init time so later calls see a constant. The microbench showed the
// plain Go scalar loop runs at 15 GB/s under a non-SIMD build but only
// 6 GB/s under GOEXPERIMENT=simd despite identical machine code - an
// alignment or codegen interaction we sidestep by always using asm.
var hasCastI64ToF64AVX2 = cpu.X86.HasAVX2

// castNTThreshold is the minimum size at which castInt64ToFloat64 dispatches
// to the streaming-store kernel. Below 1M the output still fits in L2 and the
// write-allocate cost is hidden; the streaming kernel only wins above that.
const castNTThreshold = 1024 * 1024
