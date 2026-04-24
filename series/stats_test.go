package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestSeriesSkewKurtosis(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Known values from scipy: skew([2,8,0,4,1,9,9,0]) ~= 0.2650554122698573
	// (bias=False) and kurt ~= -1.307507
	vals := []float64{2, 8, 0, 4, 1, 9, 9, 0}
	s, _ := series.FromFloat64("x", vals, nil, series.WithAllocator(alloc))
	defer s.Release()

	sk, err := s.Skew()
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(sk-0.2650554122698573) > 1e-6 {
		t.Fatalf("Skew (biased): got %v want ~0.2651", sk)
	}
	skU, _ := s.SkewUnbiased()
	if math.Abs(skU-0.3305821804079746) > 1e-6 {
		t.Fatalf("Skew (unbiased): got %v", skU)
	}

	k, err := s.Kurtosis()
	if err != nil {
		t.Fatal(err)
	}
	// polars default: m4/m2^2 - 3 on the same data ~= -1.7213
	if k < -2 || k > -1 {
		t.Fatalf("Kurtosis (biased): got %v want in (-2,-1)", k)
	}
}

func TestSeriesEntropy(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Uniform over 4 distinct values -> entropy (base 2) = 2
	s, _ := series.FromInt64("x", []int64{1, 2, 3, 4, 1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	defer s.Release()
	h, err := s.Entropy(2)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(h-2) > 1e-9 {
		t.Fatalf("Entropy: got %v want 2", h)
	}
}

func TestSeriesPearsonCorr(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a", []float64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	b, _ := series.FromFloat64("b", []float64{2, 4, 6, 8, 10}, nil, series.WithAllocator(alloc))
	defer a.Release()
	defer b.Release()
	c, err := a.PearsonCorr(b)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(c-1) > 1e-12 {
		t.Fatalf("Corr: got %v want 1", c)
	}

	neg, _ := series.FromFloat64("n", []float64{5, 4, 3, 2, 1}, nil, series.WithAllocator(alloc))
	defer neg.Release()
	cn, _ := a.PearsonCorr(neg)
	if math.Abs(cn+1) > 1e-12 {
		t.Fatalf("Anti-corr: got %v want -1", cn)
	}
}

func TestSeriesCovariance(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a", []float64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	b, _ := series.FromFloat64("b", []float64{2, 4, 6, 8, 10}, nil, series.WithAllocator(alloc))
	defer a.Release()
	defer b.Release()
	cov, err := a.Covariance(b, 1)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(cov-5) > 1e-9 {
		t.Fatalf("Cov: got %v want 5", cov)
	}
}
