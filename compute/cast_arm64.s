//go:build arm64 && !noasm

// ARM64 NEON int64 -> float64 cast.
//
// SCVTF.2D does 2 lanes of int64->float64 in one instruction. We
// unroll 4x so each iteration consumes 8 int64s (64 bytes) with 4
// independent SCVTF chains; the extra ILP keeps the load/store
// scheduler fed and improves bandwidth on M-series cores. Trailing
// 1-7 elements run scalar via FMOVD + SCVTFD.
//
// SCVTF (vector) encoding for Vd.2D, Vn.2D:
//     0x4E61D800 | (Rn << 5) | Rd
// Values used in the 4-way block:
//     scvtf v4.2d, v0.2d -> 0x4E61D804
//     scvtf v5.2d, v1.2d -> 0x4E61D825
//     scvtf v6.2d, v2.2d -> 0x4E61D846
//     scvtf v7.2d, v3.2d -> 0x4E61D867

#include "textflag.h"

// func simdCastInt64ToFloat64NT(out []float64, src []int64)
TEXT ·simdCastInt64ToFloat64NT(SB), NOSPLIT, $0-48
	MOVD	out_base+0(FP), R0
	MOVD	out_len+8(FP), R1
	MOVD	src_base+24(FP), R2

	AND	$-8, R1, R4		// loop limit (n &^ 7)
	MOVD	ZR, R5
	CBZ	R4, cast64_tail
cast64_loop8:
	VLD1	(R2), [V0.D2, V1.D2, V2.D2, V3.D2]	// 8 int64s
	WORD	$0x4E61D804		// scvtf v4.2d, v0.2d
	WORD	$0x4E61D825		// scvtf v5.2d, v1.2d
	WORD	$0x4E61D846		// scvtf v6.2d, v2.2d
	WORD	$0x4E61D867		// scvtf v7.2d, v3.2d
	VST1	[V4.D2, V5.D2, V6.D2, V7.D2], (R0)	// 8 float64s
	ADD	$64, R0, R0
	ADD	$64, R2, R2
	ADD	$8, R5, R5
	CMP	R4, R5
	BLT	cast64_loop8
cast64_tail:
	CMP	R1, R5
	BGE	cast64_done
	MOVD	(R2), R6
	SCVTFD	R6, F0
	FMOVD	F0, (R0)
	ADD	$8, R0, R0
	ADD	$8, R2, R2
	ADD	$1, R5, R5
	B	cast64_tail
cast64_done:
	RET
