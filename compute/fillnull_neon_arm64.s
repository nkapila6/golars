//go:build arm64 && !noasm

// simdFillNullInt64NEON writes out[i] = aVals[i] when validity bit i
// is set, else lit. Patterned after simdBlendInt64NEON but with the
// second array replaced by a pre-broadcast scalar.
//
// 8 int64s per outer iteration (1 bitmap byte), four pair-blends
// inline so the loop body has zero control flow outside the outer
// counter. Uses the same VEOR / VAND / VEOR branchless blend the
// existing arm64 blend kernel uses:
//
//   out = lit XOR ((src XOR lit) AND mask)
//
// For mask == all-1 this reduces to src; for mask == all-0 it
// reduces to lit. The mask vector per pair is built from two bits of
// the bitmap (NEG of the zero-extended bit → 0 or -1 per lane).
//
// Signature:
//   func simdFillNullInt64NEON(condBits []byte, aVals []int64, lit int64, out []int64) int
//
// Returns the number of elements written - always a multiple of 8 ≤
// len(out). Scalar tail handled by the caller.
//
// FP layout: condBits +0/+8/+16, aVals +24/+32/+40, lit +48,
//   out +56/+64/+72, ret +80. Frame size 88 bytes.

#include "textflag.h"

TEXT ·simdFillNullInt64NEON(SB), NOSPLIT, $0-88
	MOVD condBits_base+0(FP), R0
	MOVD aVals_base+24(FP), R1
	MOVD lit+48(FP), R2
	MOVD out_base+56(FP), R3
	MOVD out_len+64(FP), R4

	// Broadcast lit into V7 once; reused across every pair blend.
	VDUP R2, V7.D2

	MOVD ZR, R5 // R5 = index i

fnull_outer8:
	// Need R5 + 8 <= R4 for a full 8-block.
	ADD $8, R5, R11
	CMP R4, R11
	BGT fnull_done

	LSR   $3, R5, R6
	MOVBU (R0)(R6), R7 // R7 = condBits byte

	// --- pair 0: bits 0, 1 ---
	AND  $1, R7, R8
	LSR  $1, R7, R9
	AND  $1, R9, R9
	NEG  R8, R8
	NEG  R9, R9
	VMOV R8, V8.D[0]
	VMOV R9, V8.D[1]
	VLD1 (R1), [V0.D2]
	VEOR V0.B16, V7.B16, V9.B16 // V9 = src XOR lit
	VAND V8.B16, V9.B16, V9.B16 // V9 &= mask
	VEOR V7.B16, V9.B16, V9.B16 // V9 ^= lit  ⇒ blend(mask, src, lit)
	VST1 [V9.D2], (R3)
	ADD  $16, R1, R1
	ADD  $16, R3, R3

	// --- pair 1: bits 2, 3 ---
	LSR  $2, R7, R8
	AND  $1, R8, R8
	LSR  $3, R7, R9
	AND  $1, R9, R9
	NEG  R8, R8
	NEG  R9, R9
	VMOV R8, V8.D[0]
	VMOV R9, V8.D[1]
	VLD1 (R1), [V0.D2]
	VEOR V0.B16, V7.B16, V9.B16
	VAND V8.B16, V9.B16, V9.B16
	VEOR V7.B16, V9.B16, V9.B16
	VST1 [V9.D2], (R3)
	ADD  $16, R1, R1
	ADD  $16, R3, R3

	// --- pair 2: bits 4, 5 ---
	LSR  $4, R7, R8
	AND  $1, R8, R8
	LSR  $5, R7, R9
	AND  $1, R9, R9
	NEG  R8, R8
	NEG  R9, R9
	VMOV R8, V8.D[0]
	VMOV R9, V8.D[1]
	VLD1 (R1), [V0.D2]
	VEOR V0.B16, V7.B16, V9.B16
	VAND V8.B16, V9.B16, V9.B16
	VEOR V7.B16, V9.B16, V9.B16
	VST1 [V9.D2], (R3)
	ADD  $16, R1, R1
	ADD  $16, R3, R3

	// --- pair 3: bits 6, 7 ---
	LSR  $6, R7, R8
	AND  $1, R8, R8
	LSR  $7, R7, R9
	AND  $1, R9, R9
	NEG  R8, R8
	NEG  R9, R9
	VMOV R8, V8.D[0]
	VMOV R9, V8.D[1]
	VLD1 (R1), [V0.D2]
	VEOR V0.B16, V7.B16, V9.B16
	VAND V8.B16, V9.B16, V9.B16
	VEOR V7.B16, V9.B16, V9.B16
	VST1 [V9.D2], (R3)
	ADD  $16, R1, R1
	ADD  $16, R3, R3

	ADD $8, R5, R5
	B   fnull_outer8

fnull_done:
	MOVD R5, ret+80(FP)
	RET
