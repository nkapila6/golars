//go:build amd64 && !noasm

// AVX2 horizontal reductions with software prefetch.
//
// These supersede the archsimd-based implementations in simd_amd64.go
// at sizes where L3-resident data (≥256 KiB) exposes load-latency
// instead of arithmetic throughput as the ceiling. The hand-rolled
// version:
//
//   - Issues PREFETCHT0 4 cache lines (256 B) ahead of the current
//     load. That overlaps the 30+ cycle L3 hit latency with the
//     in-flight VPADDQ work, which the hardware prefetcher does not
//     do reliably at the boundary of L2 → L3 transitions.
//
//   - Uses 4 parallel accumulators × 4 lanes × 2-element unroll per
//     iter = 32 int64s / 256 bytes per iteration. Matches one cache
//     line per accumulator.
//
//   - Skips Go's bounds check on every VMOVDQU load; the Go dispatcher
//     gates on len(a) >= 32 before calling.
//
// Inspired by ClickHouse's sum kernels and Rust polars' AVX2 agg
// intrinsics, both of which rely on explicit prefetch at this scale.

#include "textflag.h"

// func simdSumInt64AVX2Prefetch(a []int64) int64
TEXT ·simdSumInt64AVX2Prefetch(SB), NOSPLIT, $0-32
	MOVQ	a_base+0(FP), SI
	MOVQ	a_len+8(FP), CX
	XORQ	AX, AX          // i = 0
	XORQ	BX, BX          // scalar total for tail
	VPXOR	Y0, Y0, Y0      // acc0
	VPXOR	Y1, Y1, Y1      // acc1
	VPXOR	Y2, Y2, Y2      // acc2
	VPXOR	Y3, Y3, Y3      // acc3

	MOVQ	CX, R8
	ANDQ	$~31, R8        // R8 = n & ~31 (chunks of 32)
	CMPQ	R8, AX
	JLE	tail_i64

loop_i64:
	// Prefetch 4 cache lines ahead (next iteration + one more).
	// The current iteration touches (SI+AX*8 .. SI+AX*8+256); we
	// prefetch (SI+AX*8+256 .. +512) so the L2→L1 fill is in flight
	// by the time the next iteration issues its loads.
	PREFETCHT0	256(SI)(AX*8)
	PREFETCHT0	320(SI)(AX*8)
	PREFETCHT0	384(SI)(AX*8)
	PREFETCHT0	448(SI)(AX*8)

	// 4 independent VPADDQ chains, 4 int64 each = 16 int64, unrolled 2x.
	VMOVDQU	0(SI)(AX*8), Y4
	VMOVDQU	32(SI)(AX*8), Y5
	VMOVDQU	64(SI)(AX*8), Y6
	VMOVDQU	96(SI)(AX*8), Y7
	VPADDQ	Y4, Y0, Y0
	VPADDQ	Y5, Y1, Y1
	VPADDQ	Y6, Y2, Y2
	VPADDQ	Y7, Y3, Y3

	VMOVDQU	128(SI)(AX*8), Y4
	VMOVDQU	160(SI)(AX*8), Y5
	VMOVDQU	192(SI)(AX*8), Y6
	VMOVDQU	224(SI)(AX*8), Y7
	VPADDQ	Y4, Y0, Y0
	VPADDQ	Y5, Y1, Y1
	VPADDQ	Y6, Y2, Y2
	VPADDQ	Y7, Y3, Y3

	ADDQ	$32, AX
	CMPQ	AX, R8
	JL	loop_i64

	// Reduce 4 accumulators to 1 YMM.
	VPADDQ	Y1, Y0, Y0
	VPADDQ	Y3, Y2, Y2
	VPADDQ	Y2, Y0, Y0

	// Horizontal sum of 4 int64 lanes in Y0.
	VEXTRACTI128	$1, Y0, X1
	VPADDQ	X1, X0, X0      // now 2 int64 in X0
	PEXTRQ	$1, X0, R9
	MOVQ	X0, R10
	ADDQ	R9, R10
	ADDQ	R10, BX

tail_i64:
	// Scalar tail for the ragged elements.
	CMPQ	AX, CX
	JGE	done_i64
	MOVQ	0(SI)(AX*8), R11
	ADDQ	R11, BX
	INCQ	AX
	JMP	tail_i64

done_i64:
	VZEROUPPER
	MOVQ	BX, ret+24(FP)
	RET

// func simdSumFloat64AVX2Prefetch(a []float64) float64
TEXT ·simdSumFloat64AVX2Prefetch(SB), NOSPLIT, $0-32
	MOVQ	a_base+0(FP), SI
	MOVQ	a_len+8(FP), CX
	XORQ	AX, AX
	VXORPD	Y0, Y0, Y0
	VXORPD	Y1, Y1, Y1
	VXORPD	Y2, Y2, Y2
	VXORPD	Y3, Y3, Y3

	MOVQ	CX, R8
	ANDQ	$~31, R8
	CMPQ	R8, AX
	JLE	tail_f64

loop_f64:
	PREFETCHT0	256(SI)(AX*8)
	PREFETCHT0	320(SI)(AX*8)
	PREFETCHT0	384(SI)(AX*8)
	PREFETCHT0	448(SI)(AX*8)

	VMOVUPD	0(SI)(AX*8), Y4
	VMOVUPD	32(SI)(AX*8), Y5
	VMOVUPD	64(SI)(AX*8), Y6
	VMOVUPD	96(SI)(AX*8), Y7
	VADDPD	Y4, Y0, Y0
	VADDPD	Y5, Y1, Y1
	VADDPD	Y6, Y2, Y2
	VADDPD	Y7, Y3, Y3

	VMOVUPD	128(SI)(AX*8), Y4
	VMOVUPD	160(SI)(AX*8), Y5
	VMOVUPD	192(SI)(AX*8), Y6
	VMOVUPD	224(SI)(AX*8), Y7
	VADDPD	Y4, Y0, Y0
	VADDPD	Y5, Y1, Y1
	VADDPD	Y6, Y2, Y2
	VADDPD	Y7, Y3, Y3

	ADDQ	$32, AX
	CMPQ	AX, R8
	JL	loop_f64

	// Reduce accumulators: 4 YMM → 1 YMM → 1 scalar.
	VADDPD	Y1, Y0, Y0
	VADDPD	Y3, Y2, Y2
	VADDPD	Y2, Y0, Y0
	// Fold the 4 lanes of Y0 → X0 (scalar).
	VEXTRACTF128	$1, Y0, X1
	VADDPD	X1, X0, X0       // 2 doubles in X0
	VHADDPD	X0, X0, X0        // horizontal add: both lanes = sum
	// X0 lane 0 now holds the 4-lane sum.

tail_f64:
	// Scalar tail: add remaining doubles to X0 lane 0.
	CMPQ	AX, CX
	JGE	done_f64
	VADDSD	0(SI)(AX*8), X0, X0
	INCQ	AX
	JMP	tail_f64

done_f64:
	VZEROUPPER
	VMOVSD	X0, ret+24(FP)
	RET
