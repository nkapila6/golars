package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestMathExtraUnary(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("x", []float64{0.1, 1, 2}, nil, series.WithAllocator(alloc))
	defer s.Release()

	cases := []struct {
		name string
		fn   func(*series.Series) (*series.Series, error)
		want []float64
	}{
		{"cbrt", func(s *series.Series) (*series.Series, error) { return s.Cbrt() }, []float64{math.Cbrt(0.1), 1, math.Cbrt(2)}},
		{"log1p", func(s *series.Series) (*series.Series, error) { return s.Log1p() }, []float64{math.Log1p(0.1), math.Log1p(1), math.Log1p(2)}},
		{"expm1", func(s *series.Series) (*series.Series, error) { return s.Expm1() }, []float64{math.Expm1(0.1), math.Expm1(1), math.Expm1(2)}},
		{"sinh", func(s *series.Series) (*series.Series, error) { return s.Sinh() }, []float64{math.Sinh(0.1), math.Sinh(1), math.Sinh(2)}},
		{"cosh", func(s *series.Series) (*series.Series, error) { return s.Cosh() }, []float64{math.Cosh(0.1), math.Cosh(1), math.Cosh(2)}},
		{"tanh", func(s *series.Series) (*series.Series, error) { return s.Tanh() }, []float64{math.Tanh(0.1), math.Tanh(1), math.Tanh(2)}},
	}
	for _, c := range cases {
		out, err := c.fn(s)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		arr := out.Chunk(0).(*array.Float64)
		for i, w := range c.want {
			if math.Abs(arr.Value(i)-w) > 1e-12 {
				t.Fatalf("%s idx %d: got %v want %v", c.name, i, arr.Value(i), w)
			}
		}
		out.Release()
	}
}

func TestMathRadiansDegrees(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("x", []float64{0, 90, 180}, nil, series.WithAllocator(alloc))
	defer s.Release()
	rad, err := s.Radians()
	if err != nil {
		t.Fatal(err)
	}
	defer rad.Release()
	arr := rad.Chunk(0).(*array.Float64)
	if math.Abs(arr.Value(1)-math.Pi/2) > 1e-12 || math.Abs(arr.Value(2)-math.Pi) > 1e-12 {
		t.Fatalf("Radians conversion wrong: %v", []float64{arr.Value(0), arr.Value(1), arr.Value(2)})
	}

	back, err := rad.Degrees()
	if err != nil {
		t.Fatal(err)
	}
	defer back.Release()
	barr := back.Chunk(0).(*array.Float64)
	if math.Abs(barr.Value(1)-90) > 1e-9 {
		t.Fatalf("Degrees roundtrip wrong: %v", barr.Value(1))
	}
}

func TestArctan2(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	y, _ := series.FromFloat64("y", []float64{1, 0, -1}, nil, series.WithAllocator(alloc))
	x, _ := series.FromFloat64("x", []float64{0, 1, 0}, nil, series.WithAllocator(alloc))
	defer y.Release()
	defer x.Release()

	out, err := y.Arctan2(x)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	if math.Abs(arr.Value(0)-math.Pi/2) > 1e-12 ||
		math.Abs(arr.Value(1)) > 1e-12 ||
		math.Abs(arr.Value(2)+math.Pi/2) > 1e-12 {
		t.Fatalf("Arctan2 wrong: %v %v %v", arr.Value(0), arr.Value(1), arr.Value(2))
	}
}
