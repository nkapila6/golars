//go:build amd64 && !noasm

// Streaming-store int64 add for amd64 with AVX2.
//
// func simdAddInt64NT(out, a, b []int64)
//
// Uses VMOVNTDQ (non-temporal store) to bypass cache for the output buffer,
// saving the write-allocate / read-for-ownership traffic that doubles write
// bandwidth. Chunks must be 32-byte aligned for VMOVNTDQ; we fall back to
// VMOVDQU stores for any unaligned prefix/suffix.
//
// This mirrors the movnt trick used in Clickhouse and polars' arrow kernels
// for >= 1MiB inputs where write-back allocation saturates DRAM bandwidth.

#include "textflag.h"

// func simdAddInt64NT(out, a, b []int64)
TEXT ·simdAddInt64NT(SB), NOSPLIT, $0-72
	MOVQ out_base+0(FP), DI
	MOVQ a_base+24(FP), SI
	MOVQ b_base+48(FP), DX
	MOVQ out_len+8(FP), CX
	XORQ AX, AX // i = 0

	// Tail below 32 elements handled by scalar loop.
	MOVQ CX, R8
	ANDQ $~31, R8 // R8 = n rounded down to 32-elem boundary

loop32:
	CMPQ AX, R8
	JGE tail_scalar

	// 4 iterations of 4-lane (4 × 64-bit) int64 add = 16 int64 per pass,
	// two passes per 32-elem block = 32 int64s per outer iteration, each
	// written via MOVNTDQ (stream store, bypasses L1).
	VMOVDQU (SI)(AX*8), Y0
	VMOVDQU (DX)(AX*8), Y1
	VPADDQ Y0, Y1, Y0
	VMOVNTDQ Y0, (DI)(AX*8)

	VMOVDQU 32(SI)(AX*8), Y2
	VMOVDQU 32(DX)(AX*8), Y3
	VPADDQ Y2, Y3, Y2
	VMOVNTDQ Y2, 32(DI)(AX*8)

	VMOVDQU 64(SI)(AX*8), Y4
	VMOVDQU 64(DX)(AX*8), Y5
	VPADDQ Y4, Y5, Y4
	VMOVNTDQ Y4, 64(DI)(AX*8)

	VMOVDQU 96(SI)(AX*8), Y6
	VMOVDQU 96(DX)(AX*8), Y7
	VPADDQ Y6, Y7, Y6
	VMOVNTDQ Y6, 96(DI)(AX*8)

	VMOVDQU 128(SI)(AX*8), Y8
	VMOVDQU 128(DX)(AX*8), Y9
	VPADDQ Y8, Y9, Y8
	VMOVNTDQ Y8, 128(DI)(AX*8)

	VMOVDQU 160(SI)(AX*8), Y10
	VMOVDQU 160(DX)(AX*8), Y11
	VPADDQ Y10, Y11, Y10
	VMOVNTDQ Y10, 160(DI)(AX*8)

	VMOVDQU 192(SI)(AX*8), Y12
	VMOVDQU 192(DX)(AX*8), Y13
	VPADDQ Y12, Y13, Y12
	VMOVNTDQ Y12, 192(DI)(AX*8)

	VMOVDQU 224(SI)(AX*8), Y14
	VMOVDQU 224(DX)(AX*8), Y15
	VPADDQ Y14, Y15, Y14
	VMOVNTDQ Y14, 224(DI)(AX*8)

	ADDQ $32, AX
	JMP loop32

tail_scalar:
	// SFENCE ensures streaming stores are visible before the scalar tail.
	SFENCE
	VZEROUPPER

	CMPQ AX, CX
	JGE done

tail_loop:
	MOVQ (SI)(AX*8), R9
	MOVQ (DX)(AX*8), R10
	ADDQ R10, R9
	MOVQ R9, (DI)(AX*8)
	INCQ AX
	CMPQ AX, CX
	JL tail_loop

done:
	RET
