//go:build amd64 && !noasm

// simdFillNullInt64AVX2 writes out[i] = aVals[i] when validity bit i
// is set, else lit. 16 int64s per outer iteration (two bytes of the
// bitmap) using VPBLENDVB against a mask vector pulled from the
// shared blendNibbleLUT in blend_avx2_amd64.s. For buffers >= 128K
// int64 (1 MiB) it uses VMOVNTDQ streaming stores to avoid the
// read-for-ownership write-back on output that's too big to stay in
// L2.
//
// Signature:
//   func simdFillNullInt64AVX2(condBits []byte, aVals []int64, lit int64, out []int64) int
//
// Returns the count of elements written (always a multiple of 8 ≤
// len(out)). Caller handles the scalar tail.
//
// FP layout: condBits +0/+8/+16, aVals +24/+32/+40, lit +48,
//   out +56/+64/+72, ret +80.

#include "textflag.h"

TEXT ·simdFillNullInt64AVX2(SB), NOSPLIT, $0-88
	MOVQ out_base+56(FP), DI
	MOVQ aVals_base+24(FP), SI
	MOVQ condBits_base+0(FP), R8
	MOVQ out_len+64(FP), CX
	MOVQ lit+48(FP), R9

	// Broadcast the scalar fill value into Y4 once; reused by every
	// VPBLENDVB below.
	MOVQ R9, X4
	VPBROADCASTQ X4, Y4

	MOVQ CX, R10
	ANDQ $~15, R10 // 16-element multiple

	XORQ AX, AX
	LEAQ ·blendNibbleLUT(SB), R11

	MOVQ CX, R14
	CMPQ R14, $131072
	JGE loopNT

loop16:
	CMPQ AX, R10
	JGE tail8

	MOVQ AX, R12
	SHRQ $3, R12 // byte index = i/8

	MOVBLZX (R8)(R12*1), R13  // bits for elements [i, i+8)
	MOVBLZX 1(R8)(R12*1), R14 // bits for [i+8, i+16)

	MOVQ R13, BX
	ANDQ $0x0f, R13
	SHRQ $4, BX
	MOVQ R14, DX
	ANDQ $0x0f, R14
	SHRQ $4, DX

	SHLQ $5, R13 // nibble × 32 bytes per LUT entry
	SHLQ $5, BX
	SHLQ $5, R14
	SHLQ $5, DX

	// Load 16 a values.
	VMOVDQU (SI)(AX*8), Y0
	VMOVDQU 32(SI)(AX*8), Y1
	VMOVDQU 64(SI)(AX*8), Y2
	VMOVDQU 96(SI)(AX*8), Y3

	// Load mask vectors from the LUT.
	VMOVDQU (R11)(R13*1), Y8
	VMOVDQU (R11)(BX*1), Y9
	VMOVDQU (R11)(R14*1), Y10
	VMOVDQU (R11)(DX*1), Y11

	// Blend: result = mask ? a : fill_broadcast (Y4). The LUT entry's
	// bytes are all-1s for valid lanes (pick a) and all-0s for null
	// lanes (pick fill).
	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y4, Y1
	VPBLENDVB Y10, Y2, Y4, Y2
	VPBLENDVB Y11, Y3, Y4, Y3

	VMOVDQU Y0, (DI)(AX*8)
	VMOVDQU Y1, 32(DI)(AX*8)
	VMOVDQU Y2, 64(DI)(AX*8)
	VMOVDQU Y3, 96(DI)(AX*8)

	ADDQ $16, AX
	JMP loop16

loopNT:
	CMPQ AX, R10
	JGE tail8_nt_done

	MOVQ AX, R12
	SHRQ $3, R12

	MOVBLZX (R8)(R12*1), R13
	MOVBLZX 1(R8)(R12*1), R14

	MOVQ R13, BX
	ANDQ $0x0f, R13
	SHRQ $4, BX
	MOVQ R14, DX
	ANDQ $0x0f, R14
	SHRQ $4, DX

	SHLQ $5, R13
	SHLQ $5, BX
	SHLQ $5, R14
	SHLQ $5, DX

	VMOVDQU (SI)(AX*8), Y0
	VMOVDQU 32(SI)(AX*8), Y1
	VMOVDQU 64(SI)(AX*8), Y2
	VMOVDQU 96(SI)(AX*8), Y3

	VMOVDQU (R11)(R13*1), Y8
	VMOVDQU (R11)(BX*1), Y9
	VMOVDQU (R11)(R14*1), Y10
	VMOVDQU (R11)(DX*1), Y11

	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y4, Y1
	VPBLENDVB Y10, Y2, Y4, Y2
	VPBLENDVB Y11, Y3, Y4, Y3

	VMOVNTDQ Y0, (DI)(AX*8)
	VMOVNTDQ Y1, 32(DI)(AX*8)
	VMOVNTDQ Y2, 64(DI)(AX*8)
	VMOVNTDQ Y3, 96(DI)(AX*8)

	ADDQ $16, AX
	JMP loopNT

tail8_nt_done:
	SFENCE

tail8:
	// Handle an 8-element trailing chunk if present.
	MOVQ CX, R10
	ANDQ $~7, R10
	CMPQ AX, R10
	JGE done

	MOVQ AX, R12
	SHRQ $3, R12
	MOVBLZX (R8)(R12*1), R13
	MOVQ R13, BX
	ANDQ $0x0f, R13
	SHRQ $4, BX
	SHLQ $5, R13
	SHLQ $5, BX

	VMOVDQU (SI)(AX*8), Y0
	VMOVDQU 32(SI)(AX*8), Y1
	VMOVDQU (R11)(R13*1), Y8
	VMOVDQU (R11)(BX*1), Y9
	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y4, Y1
	VMOVDQU Y0, (DI)(AX*8)
	VMOVDQU Y1, 32(DI)(AX*8)
	ADDQ $8, AX

done:
	VZEROUPPER
	MOVQ AX, ret+80(FP)
	RET
