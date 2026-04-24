//go:build arm64 && !noasm

package compute

// simd_arm64.go is the entry point for ARM64 builds (Apple Silicon,
// AWS Graviton, Ampere, Raspberry Pi 5, ...). NEON is baseline on
// AArch64 so we don't gate on CPU features or GOEXPERIMENT; every
// kernel ships hand-rolled Plan 9 asm in simd_arm64.s.
//
// Build-tag layout:
//
//	simd_amd64.go     (goexperiment.simd && amd64)     AVX2/AVX-512
//	simd_arm64.go     (arm64 && !noasm)                NEON (this file)
//	simd_fallback.go  (neither)                        pure scalar
//
// -tags noasm falls every target through to simd_fallback.go; useful
// when bisecting a codegen bug.
//
// Each Go-level simdXxx function here is a thin dispatcher: for tiny
// inputs where vector setup would dominate, it runs a scalar loop;
// for larger inputs it calls into the NEON assembly kernel and (for
// kernels that return a processed-count) lets the caller pick up any
// ragged tail.

const simdAvailable = true

// hasSIMDInt64 reports whether the int64 fast paths route through
// NEON. NEON is mandatory on AArch64 so this is always true.
func hasSIMDInt64() bool { return true }

// --- Assembly stubs (implementations in simd_arm64.s) -------------

// Reductions.
func simdSumInt64NEON(a []int64) int64
func simdSumFloat64NEON(a []float64) float64
func simdMinFloat64NEON(vals []float64) (best float64, hasNaN bool)
func simdMaxFloat64NEON(vals []float64) (best float64, hasNaN bool)

// Vector binary ops (slice + slice + out). Process full-multiple-of-4
// chunks via 2-lane × 2-way unroll; caller handles ragged tail.
func simdAddInt64NEON(a, b, out []int64) int
func simdAddFloat64NEON(a, b, out []float64) int

// Vector + scalar literal (returns count of elements processed).
func simdAddLitInt64NEON(src []int64, lit int64, out []int64) int
func simdSubLitInt64NEON(src []int64, lit int64, out []int64) int
func simdAddLitFloat64NEON(src []float64, lit float64, out []float64) int
func simdSubLitFloat64NEON(src []float64, lit float64, out []float64) int
func simdMulLitFloat64NEON(src []float64, lit float64, out []float64) int
func simdDivLitFloat64NEON(src []float64, lit float64, out []float64) int

// Comparisons produce a packed bitmap (1 bit per element). Process
// chunks of 8 -> 1 byte. op encodes {Eq=0, Ne=1, Lt=2, Le=3, Gt=4, Ge=5}.
func simdCompareInt64NEON(av, bv []int64, bits []byte, op compareOp) int
func simdCompareInt64LitNEON(av []int64, lit int64, bits []byte, op compareOp) int
func simdCompareFloat64NEON(av, bv []float64, bits []byte, op compareOp) int
func simdCompareFloat64LitNEON(av []float64, lit float64, bits []byte, op compareOp) int

// Blends: pick from aVals where the bit is 1, bVals where 0.
func simdBlendInt64NEON(condBits []byte, aVals, bVals, out []int64)
func simdBlendFloat64NEON(condBits []byte, aVals, bVals, out []float64)

// --- Go dispatchers ----------------------------------------------
//
// Each dispatcher handles the scalar crossover point, the ragged
// tail (for kernels that return a count), and passes through to
// the asm. Break-even thresholds are conservative; the NEON setup
// cost is dominated by horizontal-fold epilogues for reductions.

func simdSumInt64(a []int64) int64 {
	if len(a) < 16 {
		var total int64
		for _, v := range a {
			total += v
		}
		return total
	}
	return simdSumInt64NEON(a)
}

func simdSumFloat64(a []float64) float64 {
	if len(a) < 16 {
		var total float64
		for _, v := range a {
			total += v
		}
		return total
	}
	return simdSumFloat64NEON(a)
}

func simdMinFloat64(vals []float64) (float64, bool) {
	if len(vals) < 16 {
		best := vals[0]
		hasNaN := best != best
		for _, v := range vals[1:] {
			if v != v {
				hasNaN = true
				continue
			}
			if v < best {
				best = v
			}
		}
		return best, hasNaN
	}
	return simdMinFloat64NEON(vals)
}

func simdMaxFloat64(vals []float64) (float64, bool) {
	if len(vals) < 16 {
		best := vals[0]
		hasNaN := best != best
		for _, v := range vals[1:] {
			if v != v {
				hasNaN = true
				continue
			}
			if v > best {
				best = v
			}
		}
		return best, hasNaN
	}
	return simdMaxFloat64NEON(vals)
}

// simdCompareInt64 / simdCompareFloat64 return the count of elements
// that reached the SIMD path (a multiple of 8). Caller's scalar loop
// handles the tail.
func simdCompareInt64(av, bv []int64, bits []byte, op compareOp) int {
	if len(av) < 16 {
		return 0
	}
	return simdCompareInt64NEON(av, bv, bits, op)
}

func simdCompareFloat64(av, bv []float64, bits []byte, op compareOp) int {
	if len(av) < 16 {
		return 0
	}
	return simdCompareFloat64NEON(av, bv, bits, op)
}

func simdCompareInt64Lit(av []int64, lit int64, bits []byte, op compareOp) int {
	if len(av) < 16 {
		return 0
	}
	return simdCompareInt64LitNEON(av, lit, bits, op)
}

func simdCompareFloat64Lit(av []float64, lit float64, bits []byte, op compareOp) int {
	if len(av) < 16 {
		return 0
	}
	return simdCompareFloat64LitNEON(av, lit, bits, op)
}

func simdAddLitInt64(src []int64, lit int64, out []int64) int {
	if len(src) < 8 {
		return 0
	}
	return simdAddLitInt64NEON(src, lit, out)
}

func simdSubLitInt64(src []int64, lit int64, out []int64) int {
	if len(src) < 8 {
		return 0
	}
	return simdSubLitInt64NEON(src, lit, out)
}

func simdAddLitFloat64(src []float64, lit float64, out []float64) int {
	if len(src) < 8 {
		return 0
	}
	return simdAddLitFloat64NEON(src, lit, out)
}

func simdSubLitFloat64(src []float64, lit float64, out []float64) int {
	if len(src) < 8 {
		return 0
	}
	return simdSubLitFloat64NEON(src, lit, out)
}

func simdMulLitFloat64(src []float64, lit float64, out []float64) int {
	if len(src) < 8 {
		return 0
	}
	return simdMulLitFloat64NEON(src, lit, out)
}

func simdDivLitFloat64(src []float64, lit float64, out []float64) int {
	if len(src) < 8 {
		return 0
	}
	return simdDivLitFloat64NEON(src, lit, out)
}

// simdBlendInt64 / simdBlendFloat64 cover the full length because the
// caller doesn't participate in a tail loop: the NEON kernel handles
// any partial trailing chunk scalar-style.
func simdBlendInt64(condBits []byte, aVals, bVals, out []int64) {
	if len(out) < 8 {
		for i := range len(out) {
			bit := int64(condBits[i>>3]>>uint(i&7)) & 1
			mask := -bit
			out[i] = bVals[i] ^ ((aVals[i] ^ bVals[i]) & mask)
		}
		return
	}
	simdBlendInt64NEON(condBits, aVals, bVals, out)
}

func simdBlendFloat64(condBits []byte, aVals, bVals, out []float64) {
	if len(out) < 8 {
		for i := range len(out) {
			if condBits[i>>3]&(1<<(i&7)) != 0 {
				out[i] = aVals[i]
			} else {
				out[i] = bVals[i]
			}
		}
		return
	}
	simdBlendFloat64NEON(condBits, aVals, bVals, out)
}

// simdAddInt64 and simdAddFloat64 are declared in the amd64 file but
// not actually called from any kernel outside tests. Keep a minimal
// scalar dispatcher here for symmetry.
func simdAddInt64(a, b, out []int64) {
	n := len(out)
	if n >= 16 {
		i := simdAddInt64NEON(a, b, out)
		for ; i < n; i++ {
			out[i] = a[i] + b[i]
		}
		return
	}
	for i := range n {
		out[i] = a[i] + b[i]
	}
}

func simdAddFloat64(a, b, out []float64) {
	n := len(out)
	if n >= 16 {
		i := simdAddFloat64NEON(a, b, out)
		for ; i < n; i++ {
			out[i] = a[i] + b[i]
		}
		return
	}
	for i := range n {
		out[i] = a[i] + b[i]
	}
}
