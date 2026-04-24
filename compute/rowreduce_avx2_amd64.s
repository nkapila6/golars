//go:build amd64 && !noasm

// simdMaxInt64PairFoldAVX2 and simdMinInt64PairFoldAVX2: pair-fold
// horizontal reductions used by MaxHorizontal / MinHorizontal. Each
// reads buf[i] and col[i], writes max/min into buf[i]. int64 max/min
// on AVX2 is VPCMPGTQ + VPBLENDVB - no dedicated max instruction
// until AVX-512 (VPMAXSQ), but the 2-op sequence is still faster
// than scalar CMOV because it processes 4 int64s per instruction.
//
// Signature (both kernels):
//   func simdMaxInt64PairFoldAVX2(buf, col []int64) int
//   func simdMinInt64PairFoldAVX2(buf, col []int64) int
//
// Returns the number of elements written (multiple of 16 for the
// hot loop, down to 4 in the tail).

#include "textflag.h"

TEXT ·simdMaxInt64PairFoldAVX2(SB), NOSPLIT, $0-56
	MOVQ buf_base+0(FP), DI
	MOVQ col_base+24(FP), SI
	MOVQ buf_len+8(FP), CX

	MOVQ CX, R9
	ANDQ $~15, R9 // 16-element multiple

	XORQ AX, AX

max_loop16:
	CMPQ AX, R9
	JGE  max_tail4

	VMOVDQU (DI)(AX*8), Y0
	VMOVDQU 32(DI)(AX*8), Y1
	VMOVDQU 64(DI)(AX*8), Y2
	VMOVDQU 96(DI)(AX*8), Y3

	VMOVDQU (SI)(AX*8), Y4
	VMOVDQU 32(SI)(AX*8), Y5
	VMOVDQU 64(SI)(AX*8), Y6
	VMOVDQU 96(SI)(AX*8), Y7

	// mask = (buf > col) ? -1 : 0
	VPCMPGTQ Y4, Y0, Y8
	VPCMPGTQ Y5, Y1, Y9
	VPCMPGTQ Y6, Y2, Y10
	VPCMPGTQ Y7, Y3, Y11

	// max = mask ? buf : col  (high bit of each byte is set when buf>col)
	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y5, Y1
	VPBLENDVB Y10, Y2, Y6, Y2
	VPBLENDVB Y11, Y3, Y7, Y3

	VMOVDQU Y0, (DI)(AX*8)
	VMOVDQU Y1, 32(DI)(AX*8)
	VMOVDQU Y2, 64(DI)(AX*8)
	VMOVDQU Y3, 96(DI)(AX*8)

	ADDQ $16, AX
	JMP  max_loop16

max_tail4:
	// 4-element trailing chunk for sizes not divisible by 16.
	MOVQ CX, R9
	ANDQ $~3, R9

max_loop4:
	CMPQ AX, R9
	JGE  max_done

	VMOVDQU  (DI)(AX*8), Y0
	VMOVDQU  (SI)(AX*8), Y4
	VPCMPGTQ Y4, Y0, Y8
	VPBLENDVB Y8, Y0, Y4, Y0
	VMOVDQU  Y0, (DI)(AX*8)
	ADDQ     $4, AX
	JMP      max_loop4

max_done:
	VZEROUPPER
	MOVQ AX, ret+48(FP)
	RET

TEXT ·simdMinInt64PairFoldAVX2(SB), NOSPLIT, $0-56
	MOVQ buf_base+0(FP), DI
	MOVQ col_base+24(FP), SI
	MOVQ buf_len+8(FP), CX

	MOVQ CX, R9
	ANDQ $~15, R9

	XORQ AX, AX

min_loop16:
	CMPQ AX, R9
	JGE  min_tail4

	VMOVDQU (DI)(AX*8), Y0
	VMOVDQU 32(DI)(AX*8), Y1
	VMOVDQU 64(DI)(AX*8), Y2
	VMOVDQU 96(DI)(AX*8), Y3

	VMOVDQU (SI)(AX*8), Y4
	VMOVDQU 32(SI)(AX*8), Y5
	VMOVDQU 64(SI)(AX*8), Y6
	VMOVDQU 96(SI)(AX*8), Y7

	// mask = (col > buf) ? -1 : 0  (pick buf when col > buf, i.e. buf is smaller)
	VPCMPGTQ Y0, Y4, Y8
	VPCMPGTQ Y1, Y5, Y9
	VPCMPGTQ Y2, Y6, Y10
	VPCMPGTQ Y3, Y7, Y11

	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y5, Y1
	VPBLENDVB Y10, Y2, Y6, Y2
	VPBLENDVB Y11, Y3, Y7, Y3

	VMOVDQU Y0, (DI)(AX*8)
	VMOVDQU Y1, 32(DI)(AX*8)
	VMOVDQU Y2, 64(DI)(AX*8)
	VMOVDQU Y3, 96(DI)(AX*8)

	ADDQ $16, AX
	JMP  min_loop16

min_tail4:
	MOVQ CX, R9
	ANDQ $~3, R9

min_loop4:
	CMPQ AX, R9
	JGE  min_done

	VMOVDQU  (DI)(AX*8), Y0
	VMOVDQU  (SI)(AX*8), Y4
	VPCMPGTQ Y0, Y4, Y8
	VPBLENDVB Y8, Y0, Y4, Y0
	VMOVDQU  Y0, (DI)(AX*8)
	ADDQ     $4, AX
	JMP      min_loop4

min_done:
	VZEROUPPER
	MOVQ AX, ret+48(FP)
	RET

// NT-store variants for buffers too large for L2. VMOVNTDQ bypasses
// L1/L2/L3 entirely - the CPU's write-combine buffer flushes directly
// to DRAM, saving the read-for-ownership that cached stores pay for.
// Net bandwidth reduction is ~25% on a 1M-row pair-fold (8 MB in +
// 8 MB write+RFO → 8 MB in + 8 MB straight-write). Requires SFENCE
// at the end to make writes globally visible.

// func simdMaxInt64PairFoldNTAVX2(buf, col []int64) int
TEXT ·simdMaxInt64PairFoldNTAVX2(SB), NOSPLIT, $0-56
	MOVQ buf_base+0(FP), DI
	MOVQ col_base+24(FP), SI
	MOVQ buf_len+8(FP), CX

	MOVQ CX, R9
	ANDQ $~15, R9

	XORQ AX, AX

max_nt_loop16:
	CMPQ AX, R9
	JGE  max_nt_tail4

	VMOVDQU (DI)(AX*8), Y0
	VMOVDQU 32(DI)(AX*8), Y1
	VMOVDQU 64(DI)(AX*8), Y2
	VMOVDQU 96(DI)(AX*8), Y3

	VMOVDQU (SI)(AX*8), Y4
	VMOVDQU 32(SI)(AX*8), Y5
	VMOVDQU 64(SI)(AX*8), Y6
	VMOVDQU 96(SI)(AX*8), Y7

	VPCMPGTQ Y4, Y0, Y8
	VPCMPGTQ Y5, Y1, Y9
	VPCMPGTQ Y6, Y2, Y10
	VPCMPGTQ Y7, Y3, Y11

	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y5, Y1
	VPBLENDVB Y10, Y2, Y6, Y2
	VPBLENDVB Y11, Y3, Y7, Y3

	VMOVNTDQ Y0, (DI)(AX*8)
	VMOVNTDQ Y1, 32(DI)(AX*8)
	VMOVNTDQ Y2, 64(DI)(AX*8)
	VMOVNTDQ Y3, 96(DI)(AX*8)

	ADDQ $16, AX
	JMP  max_nt_loop16

max_nt_tail4:
	MOVQ CX, R9
	ANDQ $~3, R9

max_nt_loop4:
	CMPQ AX, R9
	JGE  max_nt_done

	// Tail uses cached stores to avoid fragmenting the NT stream;
	// the few extra cache lines don't matter at N >= 128K.
	VMOVDQU  (DI)(AX*8), Y0
	VMOVDQU  (SI)(AX*8), Y4
	VPCMPGTQ Y4, Y0, Y8
	VPBLENDVB Y8, Y0, Y4, Y0
	VMOVDQU  Y0, (DI)(AX*8)
	ADDQ     $4, AX
	JMP      max_nt_loop4

max_nt_done:
	SFENCE
	VZEROUPPER
	MOVQ AX, ret+48(FP)
	RET

// func simdMinInt64PairFoldNTAVX2(buf, col []int64) int
TEXT ·simdMinInt64PairFoldNTAVX2(SB), NOSPLIT, $0-56
	MOVQ buf_base+0(FP), DI
	MOVQ col_base+24(FP), SI
	MOVQ buf_len+8(FP), CX

	MOVQ CX, R9
	ANDQ $~15, R9

	XORQ AX, AX

min_nt_loop16:
	CMPQ AX, R9
	JGE  min_nt_tail4

	VMOVDQU (DI)(AX*8), Y0
	VMOVDQU 32(DI)(AX*8), Y1
	VMOVDQU 64(DI)(AX*8), Y2
	VMOVDQU 96(DI)(AX*8), Y3

	VMOVDQU (SI)(AX*8), Y4
	VMOVDQU 32(SI)(AX*8), Y5
	VMOVDQU 64(SI)(AX*8), Y6
	VMOVDQU 96(SI)(AX*8), Y7

	// mask = col > buf? → pick buf (smaller) when yes
	VPCMPGTQ Y0, Y4, Y8
	VPCMPGTQ Y1, Y5, Y9
	VPCMPGTQ Y2, Y6, Y10
	VPCMPGTQ Y3, Y7, Y11

	VPBLENDVB Y8, Y0, Y4, Y0
	VPBLENDVB Y9, Y1, Y5, Y1
	VPBLENDVB Y10, Y2, Y6, Y2
	VPBLENDVB Y11, Y3, Y7, Y3

	VMOVNTDQ Y0, (DI)(AX*8)
	VMOVNTDQ Y1, 32(DI)(AX*8)
	VMOVNTDQ Y2, 64(DI)(AX*8)
	VMOVNTDQ Y3, 96(DI)(AX*8)

	ADDQ $16, AX
	JMP  min_nt_loop16

min_nt_tail4:
	MOVQ CX, R9
	ANDQ $~3, R9

min_nt_loop4:
	CMPQ AX, R9
	JGE  min_nt_done

	VMOVDQU  (DI)(AX*8), Y0
	VMOVDQU  (SI)(AX*8), Y4
	VPCMPGTQ Y0, Y4, Y8
	VPBLENDVB Y8, Y0, Y4, Y0
	VMOVDQU  Y0, (DI)(AX*8)
	ADDQ     $4, AX
	JMP      min_nt_loop4

min_nt_done:
	SFENCE
	VZEROUPPER
	MOVQ AX, ret+48(FP)
	RET
