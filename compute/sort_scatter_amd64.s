//go:build amd64 && !noasm

// Radix-scatter inner loop in hand-written amd64 asm.
//
// Semantics identical to the Go 2-way scalar kernel in
// radixSortUint64FromFloatParallel: for each element v of src,
// compute d = (v >> shift) & mask, write v to dst[off[d]], then
// bump off[d]. Sequential bumping handles digit collisions the
// same way the Go loop does (order-preserving).
//
// The wins we're chasing vs Go-generated code:
//   * Keep shift in CL and mask in R9 for the whole loop
//     (the Go compiler sometimes reloads them under register pressure).
//   * Software-prefetch the destination cache line for a future
//     iteration. Measured here: PREFETCHT0 at distance 16 is neutral
//     in serial-single-thread (no L3 contention), regresses under
//     parallel load. Two variants exposed so the dispatcher can pick.
//   * Explicit 2-way scheduling of the collision-safe bump sequence
//     (off[d0] read → bump; off[d1] read sees the bumped value when
//     d0==d1). Matches the Go loop structure byte-for-byte; the win
//     is mainly register pinning.
//
// Signature:
//   func simdRadixScatter2AVX2(
//       src []uint64, dst []uint64, off *[2048]int,
//       shift uint64, mask uint64) int
//
// Returns the number of elements processed (even number unless
// src len is odd - the single-element tail is left for the caller).
//
// FP layout (5 args + 1 return):
//   src     base +0, len +8, cap +16  (slice, 24 bytes)
//   dst     base +24, len +32, cap +40 (24 bytes)
//   off     ptr +48                    (8 bytes)
//   shift        +56                   (8 bytes)
//   mask         +64                   (8 bytes)
//   ret int      +72                   (8 bytes)
//   Total frame: 0-80

#include "textflag.h"

TEXT ·simdRadixScatter2AVX2(SB), NOSPLIT, $0-80
	MOVQ src_base+0(FP), SI
	MOVQ src_len+8(FP), R10         // n (total iterations)
	MOVQ dst_base+24(FP), DI
	MOVQ off+48(FP), R8
	MOVQ shift+56(FP), CX           // shift goes in CL for SHRQ
	MOVQ mask+64(FP), R9

	XORQ AX, AX                     // loop counter i

loop2:
	// Need i+2 <= n for a 2-way iteration.
	LEAQ 2(AX), DX
	CMPQ DX, R10
	JG   done

	MOVQ (SI)(AX*8), R11            // v0
	MOVQ 8(SI)(AX*8), R12           // v1

	// d0 = (v0 >> shift) & mask
	MOVQ R11, R13
	SHRQ CL, R13
	ANDQ R9, R13

	// d1 = (v1 >> shift) & mask
	MOVQ R12, R14
	SHRQ CL, R14
	ANDQ R9, R14

	// p0 = off[d0]
	MOVQ (R8)(R13*8), R15
	// off[d0] = p0 + 1  (use LEA so we don't clobber R15)
	LEAQ 1(R15), BX
	MOVQ BX, (R8)(R13*8)

	// p1 = off[d1]  (reads the bumped value when d0==d1)
	MOVQ (R8)(R14*8), BX
	// off[d1] = p1 + 1
	LEAQ 1(BX), DX
	MOVQ DX, (R8)(R14*8)

	// dst[p0] = v0
	MOVQ R11, (DI)(R15*8)
	// dst[p1] = v1
	MOVQ R12, (DI)(BX*8)

	ADDQ $2, AX
	JMP  loop2

done:
	MOVQ AX, ret+72(FP)
	RET

// simdRadixScatter2PrefetchAVX2 is the same kernel with PREFETCHT0
// hints on the current iteration's destination addresses. The idea:
// by the time Go's store buffer commits the write, the cache line is
// already being pulled in. Use with caution: in parallel mode this
// regressed previously due to L3 contention from 8 cores prefetching
// simultaneously.
//
// Same signature as simdRadixScatter2AVX2.
TEXT ·simdRadixScatter2PrefetchAVX2(SB), NOSPLIT, $0-80
	MOVQ src_base+0(FP), SI
	MOVQ src_len+8(FP), R10
	MOVQ dst_base+24(FP), DI
	MOVQ off+48(FP), R8
	MOVQ shift+56(FP), CX
	MOVQ mask+64(FP), R9

	XORQ AX, AX

ploop2:
	LEAQ 2(AX), DX
	CMPQ DX, R10
	JG   pdone

	MOVQ (SI)(AX*8), R11
	MOVQ 8(SI)(AX*8), R12

	MOVQ R11, R13
	SHRQ CL, R13
	ANDQ R9, R13

	MOVQ R12, R14
	SHRQ CL, R14
	ANDQ R9, R14

	// Prefetch destination cache lines for this pair BEFORE reading
	// off[], giving the DRAM controller a head start.
	MOVQ (R8)(R13*8), R15
	PREFETCHT0 (DI)(R15*8)
	LEAQ     1(R15), BX
	MOVQ     BX, (R8)(R13*8)

	MOVQ (R8)(R14*8), BX
	PREFETCHT0 (DI)(BX*8)
	LEAQ     1(BX), DX
	MOVQ     DX, (R8)(R14*8)

	MOVQ R11, (DI)(R15*8)
	MOVQ R12, (DI)(BX*8)

	ADDQ $2, AX
	JMP  ploop2

pdone:
	MOVQ AX, ret+72(FP)
	RET
