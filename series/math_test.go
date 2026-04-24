package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestMathAndAgg(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		[]float64{-2.0, -1.0, 0.0, 1.5, 4.0},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	absS, err := s.Abs(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer absS.Release()
	absArr := absS.Chunk(0).(*array.Float64)
	if absArr.Value(0) != 2.0 {
		t.Errorf("Abs[0] = %v, want 2.0", absArr.Value(0))
	}

	sqrtS, _ := s.Abs(series.WithAllocator(alloc))
	defer sqrtS.Release()
	sqrtR, _ := sqrtS.Sqrt(series.WithAllocator(alloc))
	defer sqrtR.Release()
	if v := sqrtR.Chunk(0).(*array.Float64).Value(4); v != 2.0 {
		t.Errorf("Sqrt(|4.0|) = %v, want 2.0", v)
	}

	sum, err := s.Sum()
	if err != nil || sum != 2.5 {
		t.Errorf("Sum = %v err=%v, want 2.5", sum, err)
	}
	mean, _ := s.Mean()
	if math.Abs(mean-0.5) > 1e-9 {
		t.Errorf("Mean = %v, want 0.5", mean)
	}
	minV, _ := s.Min()
	if minV != -2.0 {
		t.Errorf("Min = %v, want -2.0", minV)
	}
	maxV, _ := s.Max()
	if maxV != 4.0 {
		t.Errorf("Max = %v, want 4.0", maxV)
	}
	med, _ := s.Median()
	if med != 0.0 {
		t.Errorf("Median = %v, want 0.0", med)
	}
	std, _ := s.Std()
	if std <= 0 {
		t.Errorf("Std = %v, want > 0", std)
	}

	ai, _ := s.ArgMin()
	if ai != 0 {
		t.Errorf("ArgMin = %d, want 0", ai)
	}
	ax, _ := s.ArgMax()
	if ax != 4 {
		t.Errorf("ArgMax = %d, want 4", ax)
	}
}

func TestReverseAndHeadTail(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("v", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()

	rev, _ := s.Reverse(series.WithAllocator(alloc))
	defer rev.Release()
	got := rev.Chunk(0).(*array.Int64)
	for i, want := range []int64{5, 4, 3, 2, 1} {
		if got.Value(i) != want {
			t.Errorf("Reverse[%d] = %d, want %d", i, got.Value(i), want)
		}
	}

	h, _ := s.Head(3)
	defer h.Release()
	if h.Len() != 3 {
		t.Errorf("Head len = %d, want 3", h.Len())
	}
	tl, _ := s.Tail(2)
	defer tl.Release()
	if tl.Len() != 2 {
		t.Errorf("Tail len = %d, want 2", tl.Len())
	}
}

func TestStrNamespace(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"hello", " world ", "foo", "bar"},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	upper, _ := s.Str().Upper(series.WithAllocator(alloc))
	defer upper.Release()
	if upper.Chunk(0).(*array.String).Value(0) != "HELLO" {
		t.Errorf("Upper[0] = %q", upper.Chunk(0).(*array.String).Value(0))
	}

	trim, _ := s.Str().Trim(series.WithAllocator(alloc))
	defer trim.Release()
	if trim.Chunk(0).(*array.String).Value(1) != "world" {
		t.Errorf("Trim[1] = %q", trim.Chunk(0).(*array.String).Value(1))
	}

	lens, _ := s.Str().LenBytes(series.WithAllocator(alloc))
	defer lens.Release()
	if lens.Chunk(0).(*array.Int64).Value(0) != 5 {
		t.Errorf("LenBytes[0] = %d", lens.Chunk(0).(*array.Int64).Value(0))
	}

	contains, _ := s.Str().Contains("oo", series.WithAllocator(alloc))
	defer contains.Release()
	carr := contains.Chunk(0).(*array.Boolean)
	if carr.Value(0) || !carr.Value(2) {
		t.Errorf("Contains mismatch: %v %v", carr.Value(0), carr.Value(2))
	}
}

func TestCumulativeAndPct(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("v", []float64{5, 3, 7, 1}, nil, series.WithAllocator(alloc))
	defer s.Release()

	cmin, _ := s.CumMin(series.WithAllocator(alloc))
	defer cmin.Release()
	arr := cmin.Chunk(0).(*array.Float64)
	for i, w := range []float64{5, 3, 3, 1} {
		if arr.Value(i) != w {
			t.Errorf("CumMin[%d] = %v, want %v", i, arr.Value(i), w)
		}
	}

	cmax, _ := s.CumMax(series.WithAllocator(alloc))
	defer cmax.Release()
	xarr := cmax.Chunk(0).(*array.Float64)
	for i, w := range []float64{5, 5, 7, 7} {
		if xarr.Value(i) != w {
			t.Errorf("CumMax[%d] = %v, want %v", i, xarr.Value(i), w)
		}
	}

	pct, _ := s.PctChange(1, series.WithAllocator(alloc))
	defer pct.Release()
	parr := pct.Chunk(0).(*array.Float64)
	// pct[1] = 3/5 - 1 = -0.4
	if math.Abs(parr.Value(1)-(-0.4)) > 1e-9 {
		t.Errorf("PctChange[1] = %v, want -0.4", parr.Value(1))
	}
}
