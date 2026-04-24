//go:build amd64 && !noasm

// func simdAddFloat64NT(out, a, b []float64)
// AVX2 non-temporal stores for float64 add. Bypasses write-allocate.

#include "textflag.h"

TEXT ·simdAddFloat64NT(SB), NOSPLIT, $0-72
	MOVQ out_base+0(FP), DI
	MOVQ a_base+24(FP), SI
	MOVQ b_base+48(FP), DX
	MOVQ out_len+8(FP), CX
	XORQ AX, AX

	MOVQ CX, R8
	ANDQ $~31, R8

loop32:
	CMPQ AX, R8
	JGE tail_scalar

	VMOVUPD (SI)(AX*8), Y0
	VMOVUPD (DX)(AX*8), Y1
	VADDPD Y0, Y1, Y0
	VMOVNTPD Y0, (DI)(AX*8)

	VMOVUPD 32(SI)(AX*8), Y2
	VMOVUPD 32(DX)(AX*8), Y3
	VADDPD Y2, Y3, Y2
	VMOVNTPD Y2, 32(DI)(AX*8)

	VMOVUPD 64(SI)(AX*8), Y4
	VMOVUPD 64(DX)(AX*8), Y5
	VADDPD Y4, Y5, Y4
	VMOVNTPD Y4, 64(DI)(AX*8)

	VMOVUPD 96(SI)(AX*8), Y6
	VMOVUPD 96(DX)(AX*8), Y7
	VADDPD Y6, Y7, Y6
	VMOVNTPD Y6, 96(DI)(AX*8)

	VMOVUPD 128(SI)(AX*8), Y8
	VMOVUPD 128(DX)(AX*8), Y9
	VADDPD Y8, Y9, Y8
	VMOVNTPD Y8, 128(DI)(AX*8)

	VMOVUPD 160(SI)(AX*8), Y10
	VMOVUPD 160(DX)(AX*8), Y11
	VADDPD Y10, Y11, Y10
	VMOVNTPD Y10, 160(DI)(AX*8)

	VMOVUPD 192(SI)(AX*8), Y12
	VMOVUPD 192(DX)(AX*8), Y13
	VADDPD Y12, Y13, Y12
	VMOVNTPD Y12, 192(DI)(AX*8)

	VMOVUPD 224(SI)(AX*8), Y14
	VMOVUPD 224(DX)(AX*8), Y15
	VADDPD Y14, Y15, Y14
	VMOVNTPD Y14, 224(DI)(AX*8)

	ADDQ $32, AX
	JMP loop32

tail_scalar:
	SFENCE
	VZEROUPPER

	CMPQ AX, CX
	JGE done

tail_loop:
	VMOVSD (SI)(AX*8), X0
	VMOVSD (DX)(AX*8), X1
	VADDSD X0, X1, X0
	VMOVSD X0, (DI)(AX*8)
	INCQ AX
	CMPQ AX, CX
	JL tail_loop

done:
	RET


// func simdMulInt64NT(out, a, b []int64)
TEXT ·simdMulInt64NT(SB), NOSPLIT, $0-72
	MOVQ out_base+0(FP), DI
	MOVQ a_base+24(FP), SI
	MOVQ b_base+48(FP), DX
	MOVQ out_len+8(FP), CX
	XORQ AX, AX

	MOVQ CX, R8
	ANDQ $~15, R8

mul_loop16:
	CMPQ AX, R8
	JGE mul_tail_scalar

	// AVX2 lacks VPMULLQ (int64×int64 → int64); use scalar imul x 16 with
	// NT-streaming movq. Still avoids write-allocate. Not as fast as VADDPD
	// but closes most of the gap vs Go's autovec.
	MOVQ (SI)(AX*8), R9
	IMULQ (DX)(AX*8), R9
	MOVNTIQ R9, (DI)(AX*8)

	MOVQ 8(SI)(AX*8), R9
	IMULQ 8(DX)(AX*8), R9
	MOVNTIQ R9, 8(DI)(AX*8)

	MOVQ 16(SI)(AX*8), R9
	IMULQ 16(DX)(AX*8), R9
	MOVNTIQ R9, 16(DI)(AX*8)

	MOVQ 24(SI)(AX*8), R9
	IMULQ 24(DX)(AX*8), R9
	MOVNTIQ R9, 24(DI)(AX*8)

	MOVQ 32(SI)(AX*8), R9
	IMULQ 32(DX)(AX*8), R9
	MOVNTIQ R9, 32(DI)(AX*8)

	MOVQ 40(SI)(AX*8), R9
	IMULQ 40(DX)(AX*8), R9
	MOVNTIQ R9, 40(DI)(AX*8)

	MOVQ 48(SI)(AX*8), R9
	IMULQ 48(DX)(AX*8), R9
	MOVNTIQ R9, 48(DI)(AX*8)

	MOVQ 56(SI)(AX*8), R9
	IMULQ 56(DX)(AX*8), R9
	MOVNTIQ R9, 56(DI)(AX*8)

	MOVQ 64(SI)(AX*8), R9
	IMULQ 64(DX)(AX*8), R9
	MOVNTIQ R9, 64(DI)(AX*8)

	MOVQ 72(SI)(AX*8), R9
	IMULQ 72(DX)(AX*8), R9
	MOVNTIQ R9, 72(DI)(AX*8)

	MOVQ 80(SI)(AX*8), R9
	IMULQ 80(DX)(AX*8), R9
	MOVNTIQ R9, 80(DI)(AX*8)

	MOVQ 88(SI)(AX*8), R9
	IMULQ 88(DX)(AX*8), R9
	MOVNTIQ R9, 88(DI)(AX*8)

	MOVQ 96(SI)(AX*8), R9
	IMULQ 96(DX)(AX*8), R9
	MOVNTIQ R9, 96(DI)(AX*8)

	MOVQ 104(SI)(AX*8), R9
	IMULQ 104(DX)(AX*8), R9
	MOVNTIQ R9, 104(DI)(AX*8)

	MOVQ 112(SI)(AX*8), R9
	IMULQ 112(DX)(AX*8), R9
	MOVNTIQ R9, 112(DI)(AX*8)

	MOVQ 120(SI)(AX*8), R9
	IMULQ 120(DX)(AX*8), R9
	MOVNTIQ R9, 120(DI)(AX*8)

	ADDQ $16, AX
	JMP mul_loop16

mul_tail_scalar:
	SFENCE

	CMPQ AX, CX
	JGE mul_done

mul_tail_loop:
	MOVQ (SI)(AX*8), R9
	IMULQ (DX)(AX*8), R9
	MOVQ R9, (DI)(AX*8)
	INCQ AX
	CMPQ AX, CX
	JL mul_tail_loop

mul_done:
	RET
