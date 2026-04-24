//go:build amd64 && !noasm

// scatterInt64SignPrefetch2: 2-way scatter with sign-bit XOR for
// int64 radix. Before computing the digit, XOR the value with the
// sign bit so negative int64 values map to lower unsigned histograms
// (preserving signed order after LSD sort). The stored dst value is
// the original int64 - only the DIGIT uses the XOR'd form.
//
// Shape:
//   load v0, v1
//   XOR sign-bit mask
//   compute digits
//   RMW cnt[d0], prefetch dst[p0]
//   RMW cnt[d1], prefetch dst[p1]
//   store dst[p0] = v0 (unmasked), dst[p1] = v1 (unmasked)

#include "textflag.h"

// func scatterInt64SignPrefetch2(src, dst *int64, n int, counts *[256]int, shift uint64)
TEXT ·scatterInt64SignPrefetch2(SB), NOSPLIT, $0-40
	MOVQ src+0(FP), SI
	MOVQ dst+8(FP), DI
	MOVQ n+16(FP), R11
	MOVQ counts+24(FP), R8
	MOVQ shift+32(FP), CX

	XORQ AX, AX

	MOVQ $0x8000000000000000, BX    // sign-bit mask

	CMPQ R11, $2
	JLT  tail_check

loop2:
	MOVQ 0(SI)(AX*8), DX    // v0 (original)
	MOVQ 8(SI)(AX*8), R15   // v1 (original)

	MOVQ DX, R10
	XORQ BX, R10
	SHRQ CL, R10
	ANDQ $0xFF, R10
	MOVQ R15, R12
	XORQ BX, R12
	SHRQ CL, R12
	ANDQ $0xFF, R12

	MOVQ 0(R8)(R10*8), R13
	LEAQ 1(R13), R9
	MOVQ R9, 0(R8)(R10*8)
	PREFETCHT0 0(DI)(R13*8)

	MOVQ 0(R8)(R12*8), R14
	LEAQ 1(R14), R9
	MOVQ R9, 0(R8)(R12*8)
	PREFETCHT0 0(DI)(R14*8)

	MOVQ DX, 0(DI)(R13*8)
	MOVQ R15, 0(DI)(R14*8)

	ADDQ $2, AX
	SUBQ $2, R11
	CMPQ R11, $2
	JGE  loop2

tail_check:
	TESTQ R11, R11
	JLE   done

tail1:
	MOVQ 0(SI)(AX*8), DX
	MOVQ DX, R10
	XORQ BX, R10
	SHRQ CL, R10
	ANDQ $0xFF, R10
	MOVQ 0(R8)(R10*8), R12
	PREFETCHT0 0(DI)(R12*8)
	LEAQ 1(R12), R13
	MOVQ R13, 0(R8)(R10*8)
	MOVQ DX, 0(DI)(R12*8)
	INCQ AX
	DECQ R11
	JNZ  tail1

done:
	RET
