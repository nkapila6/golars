package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

func floatValues(t *testing.T, s *series.Series) []float64 {
	t.Helper()
	arr := s.Chunk(0).(*array.Float64)
	out := make([]float64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		out[i] = arr.Value(i)
	}
	return out
}

// TestEWMMeanMatchesFormula pins the adjusted-form recursion so
// future refactors don't silently drift. Inputs [1, 2, 3] with
// alpha=0.5 produce:
//
//	y0 = 1
//	y1 = (0.5*1 + 2) / (0.5 + 1) = 2.5/1.5 ~= 1.6666666
//	y2 = (0.25*1 + 0.5*2 + 3) / (0.25 + 0.5 + 1) = 4.25/1.75 ~= 2.428571
func TestEWMMeanMatchesFormula(t *testing.T) {
	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil)
	defer s.Release()
	out, err := s.EWMMean(0.5)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	got := floatValues(t, out)
	want := []float64{1.0, 5.0 / 3.0, 17.0 / 7.0}
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d want %d", len(got), len(want))
	}
	for i := range want {
		if math.Abs(got[i]-want[i]) > 1e-9 {
			t.Errorf("row %d got %v want %v", i, got[i], want[i])
		}
	}
}

// TestEWMVarNonneg sanity-checks that variance is non-negative and
// zero on the first observation.
func TestEWMVarNonneg(t *testing.T) {
	s, _ := series.FromFloat64("x", []float64{1, 2, 4, 8, 16}, nil)
	defer s.Release()
	out, err := s.EWMVar(0.5)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	vals := floatValues(t, out)
	if vals[0] != 0 {
		t.Errorf("var[0] = %v, want 0 (only one observation)", vals[0])
	}
	for i := 1; i < len(vals); i++ {
		if vals[i] < 0 {
			t.Errorf("var[%d] = %v is negative", i, vals[i])
		}
	}
}

func TestEWMStdIsSqrtVar(t *testing.T) {
	s, _ := series.FromFloat64("x", []float64{1, 2, 3, 4, 5}, nil)
	defer s.Release()
	v, _ := s.EWMVar(0.3)
	st, _ := s.EWMStd(0.3)
	defer v.Release()
	defer st.Release()
	vv := floatValues(t, v)
	ss := floatValues(t, st)
	for i := range vv {
		if math.Abs(math.Sqrt(vv[i])-ss[i]) > 1e-9 {
			t.Errorf("std[%d] = %v, sqrt(var) = %v", i, ss[i], math.Sqrt(vv[i]))
		}
	}
}

func TestEWMRejectsBadAlpha(t *testing.T) {
	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil)
	defer s.Release()
	for _, a := range []float64{-0.1, 0, 1.1, math.NaN()} {
		if _, err := s.EWMMean(a); err == nil {
			t.Errorf("alpha=%v: expected error", a)
		}
	}
}
