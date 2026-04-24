//go:build goexperiment.simd && amd64

// simdBlendInt64AVX2 blends two int64 vectors under a packed bitmap
// mask using AVX2 VPBLENDVB. Processes 16 int64s per outer iteration
// (two bytes of the bitmap) so the 4-cycle VPBLENDVB latency overlaps
// across adjacent iterations.
//
// For buffers larger than half the L2 cache (~256KiB), uses VMOVNTDQ
// streaming stores to avoid the read-for-ownership write-back penalty.
//
// func simdBlendInt64AVX2(condBits []byte, aVals, bVals, out []int64) int

#include "textflag.h"

// Inputs: condBits (base+0,len+8,cap+16), aVals (+24/+32/+40),
// bVals (+48/+56/+64), out (+72/+80/+88), ret int (+96).
TEXT ·simdBlendInt64AVX2(SB), NOSPLIT, $0-104
	MOVQ out_base+72(FP), DI
	MOVQ aVals_base+24(FP), SI
	MOVQ bVals_base+48(FP), DX
	MOVQ condBits_base+0(FP), R8
	MOVQ out_len+80(FP), CX

	MOVQ CX, R9
	ANDQ $~15, R9 // 16-element multiple

	XORQ AX, AX
	LEAQ ·blendNibbleLUT(SB), R11

	// Decide between cached and streaming stores. Streaming wins above
	// ~1 MiB (131072 int64s) when the output no longer fits in L2.
	MOVQ CX, R14
	CMPQ R14, $131072
	JGE loopNT

loop16:
	CMPQ AX, R9
	JGE tail8

	MOVQ AX, R10
	SHRQ $3, R10 // byte index = i/8

	// Two bytes cover 16 elements.
	MOVBLZX (R8)(R10*1), R12  // byte covering elements [i, i+8)
	MOVBLZX 1(R8)(R10*1), R13 // byte covering [i+8, i+16)

	MOVQ R12, BX
	ANDQ $0x0f, R12
	SHRQ $4, BX
	MOVQ R13, R15
	ANDQ $0x0f, R13
	SHRQ $4, R15

	SHLQ $5, R12
	SHLQ $5, BX
	SHLQ $5, R13
	SHLQ $5, R15

	// Load 16 a values and 16 b values.
	VMOVDQU (SI)(AX*8), Y0
	VMOVDQU 32(SI)(AX*8), Y1
	VMOVDQU 64(SI)(AX*8), Y2
	VMOVDQU 96(SI)(AX*8), Y3

	VMOVDQU (DX)(AX*8), Y4
	VMOVDQU 32(DX)(AX*8), Y5
	VMOVDQU 64(DX)(AX*8), Y6
	VMOVDQU 96(DX)(AX*8), Y7

	// Load mask vectors.
	VMOVDQU (R11)(R12*1), Y8
	VMOVDQU (R11)(BX*1), Y9
	VMOVDQU (R11)(R13*1), Y10
	VMOVDQU (R11)(R15*1), Y11

	// Blend: result = mask ? a : b.
	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y5, Y1
	VPBLENDVB Y10, Y2, Y6, Y2
	VPBLENDVB Y11, Y3, Y7, Y3

	VMOVDQU Y0, (DI)(AX*8)
	VMOVDQU Y1, 32(DI)(AX*8)
	VMOVDQU Y2, 64(DI)(AX*8)
	VMOVDQU Y3, 96(DI)(AX*8)

	ADDQ $16, AX
	JMP loop16

loopNT:
	CMPQ AX, R9
	JGE tail8_nt_done

	MOVQ AX, R10
	SHRQ $3, R10

	MOVBLZX (R8)(R10*1), R12
	MOVBLZX 1(R8)(R10*1), R13

	MOVQ R12, BX
	ANDQ $0x0f, R12
	SHRQ $4, BX
	MOVQ R13, R15
	ANDQ $0x0f, R13
	SHRQ $4, R15

	SHLQ $5, R12
	SHLQ $5, BX
	SHLQ $5, R13
	SHLQ $5, R15

	VMOVDQU (SI)(AX*8), Y0
	VMOVDQU 32(SI)(AX*8), Y1
	VMOVDQU 64(SI)(AX*8), Y2
	VMOVDQU 96(SI)(AX*8), Y3

	VMOVDQU (DX)(AX*8), Y4
	VMOVDQU 32(DX)(AX*8), Y5
	VMOVDQU 64(DX)(AX*8), Y6
	VMOVDQU 96(DX)(AX*8), Y7

	VMOVDQU (R11)(R12*1), Y8
	VMOVDQU (R11)(BX*1), Y9
	VMOVDQU (R11)(R13*1), Y10
	VMOVDQU (R11)(R15*1), Y11

	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y5, Y1
	VPBLENDVB Y10, Y2, Y6, Y2
	VPBLENDVB Y11, Y3, Y7, Y3

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
	MOVQ CX, R9
	ANDQ $~7, R9
	CMPQ AX, R9
	JGE done

	MOVQ AX, R10
	SHRQ $3, R10
	MOVBLZX (R8)(R10*1), R12
	MOVQ R12, BX
	ANDQ $0x0f, R12
	SHRQ $4, BX
	SHLQ $5, R12
	SHLQ $5, BX

	VMOVDQU (SI)(AX*8), Y0
	VMOVDQU 32(SI)(AX*8), Y1
	VMOVDQU (DX)(AX*8), Y4
	VMOVDQU 32(DX)(AX*8), Y5
	VMOVDQU (R11)(R12*1), Y8
	VMOVDQU (R11)(BX*1), Y9
	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y5, Y1
	VMOVDQU Y0, (DI)(AX*8)
	VMOVDQU Y1, 32(DI)(AX*8)
	ADDQ $8, AX

done:
	VZEROUPPER
	MOVQ AX, ret+96(FP)
	RET

