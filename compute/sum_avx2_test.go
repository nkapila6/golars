//go:build amd64 && !noasm

package compute

import (
	"math/rand/v2"
	"testing"
)

// Correctness: the asm kernels must produce the same integer sum as a
// reference scalar sum, and the float kernel must agree with a tree-sum
// reference within 1e-9 relative error (FP reassociation is allowed;
// polars also reassociates).

func TestSumInt64AVX2PrefetchCorrectness(t *testing.T) {
	if !hasAVX2Prefetch {
		t.Skip("requires AVX2")
	}
	r := rand.New(rand.NewPCG(42, 43))
	for _, n := range []int{0, 1, 7, 31, 32, 33, 64, 1023, 1024, 1025, 65_536} {
		vals := make([]int64, n)
		var refTotal int64
		for i := range vals {
			vals[i] = r.Int64N(1 << 20)
			refTotal += vals[i]
		}
		got := simdSumInt64AVX2Prefetch(vals)
		if got != refTotal {
			t.Errorf("n=%d: got %d, want %d", n, got, refTotal)
		}
	}
}

func TestSumFloat64AVX2PrefetchCorrectness(t *testing.T) {
	if !hasAVX2Prefetch {
		t.Skip("requires AVX2")
	}
	r := rand.New(rand.NewPCG(42, 43))
	for _, n := range []int{0, 1, 7, 31, 32, 33, 64, 1023, 1024, 65_536} {
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = r.Float64()
		}
		got := simdSumFloat64AVX2Prefetch(vals)
		var ref float64
		for _, v := range vals {
			ref += v
		}
		if n == 0 && got != 0 {
			t.Errorf("n=%d: got %v, want 0", n, got)
			continue
		}
		if ref == 0 {
			continue
		}
		relErr := (got - ref) / ref
		if relErr < 0 {
			relErr = -relErr
		}
		if relErr > 1e-9 {
			t.Errorf("n=%d: got %v want %v (rel err %v)", n, got, ref, relErr)
		}
	}
}
