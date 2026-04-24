//go:build (!goexperiment.simd || !amd64) && (!arm64 || noasm)

package compute

// simdAvailable is false on platforms with no SIMD code path compiled
// in: any build without GOEXPERIMENT=simd on amd64, and any arm64
// build explicitly disabled with `-tags noasm`. Kernels skip the SIMD
// branch in that case; the scalar implementation runs as before.
//
// The arm64-native NEON path lives in simd_arm64.go + simd_arm64.s.
const simdAvailable = false

func hasSIMDInt64() bool { return false }

func simdSumInt64(a []int64) int64 {
	var total int64
	for _, v := range a {
		total += v
	}
	return total
}

func simdSumFloat64(a []float64) float64 {
	var total float64
	for _, v := range a {
		total += v
	}
	return total
}

func simdMinFloat64(vals []float64) (float64, bool) {
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

func simdMaxFloat64(vals []float64) (float64, bool) {
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

func simdCompareInt64(av, bv []int64, bits []byte, op compareOp) int {
	_ = av
	_ = bv
	_ = bits
	_ = op
	return 0
}

func simdCompareFloat64(av, bv []float64, bits []byte, op compareOp) int {
	_ = av
	_ = bv
	_ = bits
	_ = op
	return 0
}

// simdCompareInt64Lit and simdCompareFloat64Lit have no scalar SIMD
// fallback; the caller falls through to its byte-at-a-time loop.
func simdCompareInt64Lit(_ []int64, _ int64, _ []byte, _ compareOp) int       { return 0 }
func simdCompareFloat64Lit(_ []float64, _ float64, _ []byte, _ compareOp) int { return 0 }

// simd*Lit arith fallbacks: scalar loop handled by the caller.
func simdAddLitInt64(_ []int64, _ int64, _ []int64) int         { return 0 }
func simdSubLitInt64(_ []int64, _ int64, _ []int64) int         { return 0 }
func simdAddLitFloat64(_ []float64, _ float64, _ []float64) int { return 0 }
func simdSubLitFloat64(_ []float64, _ float64, _ []float64) int { return 0 }
func simdMulLitFloat64(_ []float64, _ float64, _ []float64) int { return 0 }
func simdDivLitFloat64(_ []float64, _ float64, _ []float64) int { return 0 }

// simdBlendInt64 is the scalar branchless fallback. XOR-select avoids
// branches so the compiler can keep the inner loop tight even without
// vector instructions.
func simdBlendInt64(condBits []byte, aVals, bVals, out []int64) {
	n := len(out)
	for i := range n {
		bit := int64(condBits[i>>3]>>uint(i&7)) & 1
		mask := -bit
		out[i] = bVals[i] ^ ((aVals[i] ^ bVals[i]) & mask)
	}
}

func simdBlendFloat64(condBits []byte, aVals, bVals, out []float64) {
	n := len(out)
	for i := range n {
		if condBits[i>>3]&(1<<(i&7)) != 0 {
			out[i] = aVals[i]
		} else {
			out[i] = bVals[i]
		}
	}
}
