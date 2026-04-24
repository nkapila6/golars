package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestRollingSum(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.RollingSum(series.RollingOptions{WindowSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	// First 2 rows < min_periods=window: null
	if arr.IsValid(0) || arr.IsValid(1) {
		t.Fatal("expected first two rows to be null")
	}
	want := []float64{6, 9, 12}
	for i, v := range want {
		if arr.Value(i+2) != v {
			t.Fatalf("idx %d: got %v want %v", i+2, arr.Value(i+2), v)
		}
	}
}

func TestRollingMean(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("x", []float64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, _ := s.RollingMean(series.RollingOptions{WindowSize: 3, MinPeriods: 1})
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	// min_periods=1 so first two rows are means of partial windows.
	want := []float64{1, 1.5, 2, 3, 4}
	for i, v := range want {
		if math.Abs(arr.Value(i)-v) > 1e-12 {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestRollingMinMax(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{3, 1, 4, 1, 5, 9, 2, 6}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mn, _ := s.RollingMin(series.RollingOptions{WindowSize: 3})
	defer mn.Release()
	mx, _ := s.RollingMax(series.RollingOptions{WindowSize: 3})
	defer mx.Release()
	mnArr := mn.Chunk(0).(*array.Float64)
	mxArr := mx.Chunk(0).(*array.Float64)
	wantMin := []float64{1, 1, 1, 1, 2, 2}
	wantMax := []float64{4, 4, 5, 9, 9, 9}
	for i, v := range wantMin {
		if mnArr.Value(i+2) != v {
			t.Fatalf("min idx %d: got %v want %v", i+2, mnArr.Value(i+2), v)
		}
		if mxArr.Value(i+2) != wantMax[i] {
			t.Fatalf("max idx %d: got %v want %v", i+2, mxArr.Value(i+2), wantMax[i])
		}
	}
}

func TestRollingStd(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Window of all equal values has std=0.
	s, _ := series.FromFloat64("x", []float64{2, 2, 2, 2, 2}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, _ := s.RollingStd(series.RollingOptions{WindowSize: 3})
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	for i := 2; i < 5; i++ {
		if math.Abs(arr.Value(i)) > 1e-9 {
			t.Fatalf("idx %d: got %v want 0", i, arr.Value(i))
		}
	}
}
