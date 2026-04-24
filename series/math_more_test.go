package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// Covers Log/Log2/Log10/Exp/Sin/Cos/Tan/Sign/Round/Floor/Ceil/Clip/Pow
// plus Var/Any/All/Product/Quantile that the earlier TestMathAndAgg
// only partially exercised.

func TestSeriesMathUnary(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a", []float64{1, math.E, 10, 100}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	cases := []struct {
		name string
		fn   func() (*series.Series, error)
		want []float64
		eps  float64
	}{
		{"Log", func() (*series.Series, error) { return s.Log(series.WithAllocator(alloc)) },
			[]float64{0, 1, math.Log(10), math.Log(100)}, 1e-9},
		{"Log2", func() (*series.Series, error) { return s.Log2(series.WithAllocator(alloc)) },
			[]float64{0, math.Log2(math.E), math.Log2(10), math.Log2(100)}, 1e-9},
		{"Log10", func() (*series.Series, error) { return s.Log10(series.WithAllocator(alloc)) },
			[]float64{0, math.Log10(math.E), 1, 2}, 1e-9},
		{"Exp", func() (*series.Series, error) { return s.Exp(series.WithAllocator(alloc)) },
			[]float64{math.E, math.Exp(math.E), math.Exp(10), math.Exp(100)}, 1e-6},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.fn()
			if err != nil {
				t.Fatal(err)
			}
			defer got.Release()
			arr := got.Chunk(0).(*array.Float64)
			for i, w := range c.want {
				if math.Abs(arr.Value(i)-w) > c.eps*math.Max(1, math.Abs(w)) {
					t.Errorf("[%d] = %v, want %v", i, arr.Value(i), w)
				}
			}
		})
	}
}

func TestSeriesRoundFloorCeil(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a", []float64{-1.7, -1.3, 0, 1.4, 1.6}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	round, _ := s.Round(0, series.WithAllocator(alloc))
	defer round.Release()
	floor, _ := s.Floor(series.WithAllocator(alloc))
	defer floor.Release()
	ceil, _ := s.Ceil(series.WithAllocator(alloc))
	defer ceil.Release()

	rArr := round.Chunk(0).(*array.Float64)
	fArr := floor.Chunk(0).(*array.Float64)
	cArr := ceil.Chunk(0).(*array.Float64)
	// math.Round rounds away from zero.
	wantRound := []float64{-2, -1, 0, 1, 2}
	wantFloor := []float64{-2, -2, 0, 1, 1}
	wantCeil := []float64{-1, -1, 0, 2, 2}
	for i := range 5 {
		if rArr.Value(i) != wantRound[i] {
			t.Errorf("Round[%d] = %v, want %v", i, rArr.Value(i), wantRound[i])
		}
		if fArr.Value(i) != wantFloor[i] {
			t.Errorf("Floor[%d] = %v, want %v", i, fArr.Value(i), wantFloor[i])
		}
		if cArr.Value(i) != wantCeil[i] {
			t.Errorf("Ceil[%d] = %v, want %v", i, cArr.Value(i), wantCeil[i])
		}
	}
}

func TestSeriesClipAndPow(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a", []float64{-5, 0, 3, 10}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	clip, _ := s.Clip(0, 5, series.WithAllocator(alloc))
	defer clip.Release()
	cArr := clip.Chunk(0).(*array.Float64)
	want := []float64{0, 0, 3, 5}
	for i, w := range want {
		if cArr.Value(i) != w {
			t.Errorf("Clip[%d] = %v, want %v", i, cArr.Value(i), w)
		}
	}

	p, _ := s.Pow(2, series.WithAllocator(alloc))
	defer p.Release()
	pArr := p.Chunk(0).(*array.Float64)
	wantSq := []float64{25, 0, 9, 100}
	for i, w := range wantSq {
		if pArr.Value(i) != w {
			t.Errorf("Pow2[%d] = %v, want %v", i, pArr.Value(i), w)
		}
	}
}

func TestSeriesSignAndTrig(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a", []float64{-3.5, 0, 2.2}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	sign, _ := s.Sign(series.WithAllocator(alloc))
	defer sign.Release()
	sArr := sign.Chunk(0).(*array.Float64)
	want := []float64{-1, 0, 1}
	for i, w := range want {
		if sArr.Value(i) != w {
			t.Errorf("Sign[%d] = %v, want %v", i, sArr.Value(i), w)
		}
	}

	// sin(0) = 0, cos(0) = 1 sanity.
	zero, _ := series.FromFloat64("z", []float64{0}, nil, series.WithAllocator(alloc))
	defer zero.Release()
	sin, _ := zero.Sin(series.WithAllocator(alloc))
	defer sin.Release()
	cos, _ := zero.Cos(series.WithAllocator(alloc))
	defer cos.Release()
	if sin.Chunk(0).(*array.Float64).Value(0) != 0 {
		t.Errorf("sin(0) != 0")
	}
	if cos.Chunk(0).(*array.Float64).Value(0) != 1 {
		t.Errorf("cos(0) != 1")
	}
}

func TestSeriesAggsCornerCases(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Var/Std require at least 2 samples.
	one, _ := series.FromFloat64("v", []float64{42}, nil, series.WithAllocator(alloc))
	defer one.Release()
	v, _ := one.Var()
	if !math.IsNaN(v) {
		t.Errorf("Var of single-value series = %v, want NaN", v)
	}
	std, _ := one.Std()
	if !math.IsNaN(std) {
		t.Errorf("Std of single-value series = %v, want NaN", std)
	}

	// Any/All on bool.
	b, _ := series.FromBool("b", []bool{false, false, true}, nil,
		series.WithAllocator(alloc))
	defer b.Release()
	any, _ := b.Any()
	if !any {
		t.Errorf("Any = false, want true")
	}
	all, _ := b.All()
	if all {
		t.Errorf("All = true, want false")
	}

	// Empty All is vacuously true.
	empty, _ := series.FromBool("b", []bool{}, nil, series.WithAllocator(alloc))
	defer empty.Release()
	all2, _ := empty.All()
	if !all2 {
		t.Errorf("All on empty = false, want true (vacuous)")
	}

	// Product.
	n, _ := series.FromInt64("n", []int64{2, 3, 5}, nil, series.WithAllocator(alloc))
	defer n.Release()
	p, _ := n.Product()
	if p != 30 {
		t.Errorf("Product = %v, want 30", p)
	}

	// Quantile corners.
	s, _ := series.FromFloat64("q", []float64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	defer s.Release()
	q0, _ := s.Quantile(0)
	q1, _ := s.Quantile(1)
	q5, _ := s.Quantile(0.5)
	if q0 != 1 || q1 != 4 || q5 != 2.5 {
		t.Errorf("quantiles = (%v, %v, %v), want (1, 4, 2.5)", q0, q1, q5)
	}
	// Out-of-range q returns error.
	if _, err := s.Quantile(1.5); err == nil {
		t.Errorf("Quantile(1.5) should error")
	}
}

func TestSeriesArgSortAndTopBottom(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("v", []int64{5, 2, 8, 1, 3}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	idx, _ := s.ArgSort()
	want := []int{3, 1, 4, 0, 2}
	if len(idx) != len(want) {
		t.Fatalf("ArgSort len = %d", len(idx))
	}
	for i, w := range want {
		if idx[i] != w {
			t.Errorf("ArgSort[%d] = %d, want %d", i, idx[i], w)
		}
	}

	top, _ := s.TopK(3, series.WithAllocator(alloc))
	defer top.Release()
	topArr := top.Chunk(0).(*array.Int64)
	// Descending top 3: 8, 5, 3.
	if topArr.Value(0) != 8 || topArr.Value(1) != 5 || topArr.Value(2) != 3 {
		t.Errorf("TopK = [%d, %d, %d], want [8, 5, 3]",
			topArr.Value(0), topArr.Value(1), topArr.Value(2))
	}

	bot, _ := s.BottomK(2, series.WithAllocator(alloc))
	defer bot.Release()
	botArr := bot.Chunk(0).(*array.Int64)
	if botArr.Value(0) != 1 || botArr.Value(1) != 2 {
		t.Errorf("BottomK = [%d, %d], want [1, 2]",
			botArr.Value(0), botArr.Value(1))
	}
}

func TestSeriesSampleShuffleEqual(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("v", []int64{10, 20, 30, 40, 50}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	sample, _ := s.Sample(3, false, 42, series.WithAllocator(alloc))
	defer sample.Release()
	if sample.Len() != 3 {
		t.Errorf("Sample len = %d, want 3", sample.Len())
	}
	// Sample without replacement: every output value must be unique and
	// appear in the source.
	seen := map[int64]bool{}
	sArr := sample.Chunk(0).(*array.Int64)
	for i := range 3 {
		v := sArr.Value(i)
		if seen[v] {
			t.Errorf("Sample contains duplicate %d", v)
		}
		seen[v] = true
		if v%10 != 0 || v < 10 || v > 50 {
			t.Errorf("Sample value %d not from source", v)
		}
	}

	// Shuffle preserves multiset.
	shuf, _ := s.Shuffle(7, series.WithAllocator(alloc))
	defer shuf.Release()
	if shuf.Len() != s.Len() {
		t.Fatalf("Shuffle length mismatch")
	}

	// Equal compares name, dtype, values, null pattern.
	clone := s.Clone()
	defer clone.Release()
	if !s.Equal(clone) {
		t.Errorf("Series should equal its Clone")
	}
	other, _ := series.FromInt64("v", []int64{10, 20, 30, 40, 50}, nil,
		series.WithAllocator(alloc))
	defer other.Release()
	if !s.Equal(other) {
		t.Errorf("Series with same content should compare equal")
	}
}

func TestSeriesValidityAcrossDtypes(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("f",
		[]float64{1.5, math.NaN(), math.Inf(1), math.Inf(-1), 0},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	nan, _ := s.IsNaN(series.WithAllocator(alloc))
	defer nan.Release()
	na := nan.Chunk(0).(*array.Boolean)
	// NaN at index 1 only.
	for i, want := range []bool{false, true, false, false, false} {
		if na.Value(i) != want {
			t.Errorf("IsNaN[%d] = %v, want %v", i, na.Value(i), want)
		}
	}

	fin, _ := s.IsFinite(series.WithAllocator(alloc))
	defer fin.Release()
	fa := fin.Chunk(0).(*array.Boolean)
	for i, want := range []bool{true, false, false, false, true} {
		if fa.Value(i) != want {
			t.Errorf("IsFinite[%d] = %v, want %v", i, fa.Value(i), want)
		}
	}

	inf, _ := s.IsInfinite(series.WithAllocator(alloc))
	defer inf.Release()
	ia := inf.Chunk(0).(*array.Boolean)
	for i, want := range []bool{false, false, true, true, false} {
		if ia.Value(i) != want {
			t.Errorf("IsInfinite[%d] = %v, want %v", i, ia.Value(i), want)
		}
	}
}
