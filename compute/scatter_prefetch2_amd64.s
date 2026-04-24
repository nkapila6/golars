//go:build amd64 && !noasm

// scatterUint64Prefetch2: 2-way unrolled 8-bit scatter with PREFETCHT0
// on each dst write target.
//
// Hot-loop shape (per pair of elements):
//   load src v0, v1 (2 loads - dual-issue on Skylake)
//   compute d0, d1  (shift+mask)
//   load p0 = cnt[d0], increment cnt[d0]
//   prefetch dst[p0]
//   load p1 = cnt[d1] (reads updated cnt if d0==d1)
//   increment cnt[d1]
//   prefetch dst[p1]
//   store dst[p0], dst[p1]
//
// The second cnt load intentionally comes AFTER the first increment
// so the branchless d0==d1 collision case resolves correctly. Prefetch
// is issued right after each p is known, giving the L2 fill the whole
// critical path from shift → store to overlap.

#include "textflag.h"

// func scatterUint64Prefetch2(src, dst *uint64, n int, counts *[256]int, shift uint64)
TEXT ·scatterUint64Prefetch2(SB), NOSPLIT, $0-40
	MOVQ src+0(FP), SI
	MOVQ dst+8(FP), DI
	MOVQ n+16(FP), R11
	MOVQ counts+24(FP), R8
	MOVQ shift+32(FP), CX   // CX = shift (CL is used by SHRQ)

	XORQ AX, AX             // AX = i

	CMPQ R11, $2
	JLT  tail_check

loop2:
	// Load v0, v1.
	MOVQ 0(SI)(AX*8), DX    // v0
	MOVQ 8(SI)(AX*8), R15   // v1

	// Compute d0, d1.
	MOVQ DX, R10
	SHRQ CL, R10
	ANDQ $0xFF, R10
	MOVQ R15, R12
	SHRQ CL, R12
	ANDQ $0xFF, R12

	// RMW cnt[d0]; prefetch dst[p0].
	MOVQ 0(R8)(R10*8), R13  // p0
	LEAQ 1(R13), R9
	MOVQ R9, 0(R8)(R10*8)
	PREFETCHT0 0(DI)(R13*8)

	// RMW cnt[d1] (reads updated cnt if d0==d1); prefetch dst[p1].
	MOVQ 0(R8)(R12*8), R14  // p1 (sees d0==d1 increment)
	LEAQ 1(R14), R9
	MOVQ R9, 0(R8)(R12*8)
	PREFETCHT0 0(DI)(R14*8)

	// Store dst.
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
