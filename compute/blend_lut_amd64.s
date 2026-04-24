//go:build amd64 && !noasm

// blendNibbleLUT is a 16-entry × 32-byte lookup table that expands a
// 4-bit validity mask into a 32-byte YMM register suitable for
// VPBLENDVB. Each byte in the LUT entry is either 0x00 or 0xFF; when
// fed to VPBLENDVB, 0xFF (high bit set) picks the first operand,
// 0x00 picks the second. We use it from both the blend and fill-null
// kernels, so it lives here (unconstrained by the goexperiment tag
// that gates the full archsimd-based kernels).
//
// Layout: entry i at offset i × 32. Within each entry, lanes are
// laid out as four 8-byte chunks (one per validity bit of the
// nibble), each filled with either 0xFFFFFFFFFFFFFFFF or zero.

#include "textflag.h"

DATA ·blendNibbleLUT+0(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+8(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+16(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+24(SB)/8, $0x0000000000000000

DATA ·blendNibbleLUT+32(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+40(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+48(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+56(SB)/8, $0x0000000000000000

DATA ·blendNibbleLUT+64(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+72(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+80(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+88(SB)/8, $0x0000000000000000

DATA ·blendNibbleLUT+96(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+104(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+112(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+120(SB)/8, $0x0000000000000000

DATA ·blendNibbleLUT+128(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+136(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+144(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+152(SB)/8, $0x0000000000000000

DATA ·blendNibbleLUT+160(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+168(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+176(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+184(SB)/8, $0x0000000000000000

DATA ·blendNibbleLUT+192(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+200(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+208(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+216(SB)/8, $0x0000000000000000

DATA ·blendNibbleLUT+224(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+232(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+240(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+248(SB)/8, $0x0000000000000000

DATA ·blendNibbleLUT+256(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+264(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+272(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+280(SB)/8, $0xffffffffffffffff

DATA ·blendNibbleLUT+288(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+296(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+304(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+312(SB)/8, $0xffffffffffffffff

DATA ·blendNibbleLUT+320(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+328(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+336(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+344(SB)/8, $0xffffffffffffffff

DATA ·blendNibbleLUT+352(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+360(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+368(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+376(SB)/8, $0xffffffffffffffff

DATA ·blendNibbleLUT+384(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+392(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+400(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+408(SB)/8, $0xffffffffffffffff

DATA ·blendNibbleLUT+416(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+424(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+432(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+440(SB)/8, $0xffffffffffffffff

DATA ·blendNibbleLUT+448(SB)/8, $0x0000000000000000
DATA ·blendNibbleLUT+456(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+464(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+472(SB)/8, $0xffffffffffffffff

DATA ·blendNibbleLUT+480(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+488(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+496(SB)/8, $0xffffffffffffffff
DATA ·blendNibbleLUT+504(SB)/8, $0xffffffffffffffff

GLOBL ·blendNibbleLUT(SB), RODATA|NOPTR, $512
