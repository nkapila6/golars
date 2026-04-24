package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestCumSumInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.CumSum(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	want := []int64{1, 3, 6, 10, 15}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), w)
		}
	}
}

func TestCumSumFloat64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a", []float64{0.5, 1.5, 2}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.CumSum(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	want := []float64{0.5, 2.0, 4.0}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%v want %v", i, arr.Value(i), w)
		}
	}
}

func TestCumSumWithNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 4}, []bool{true, false, true, true},
		series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.CumSum(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	// Running sum skips nulls: [1, null, 4, 8]
	if !arr.IsValid(0) || arr.Value(0) != 1 {
		t.Fatalf("[0]=%d valid=%v", arr.Value(0), arr.IsValid(0))
	}
	if arr.IsValid(1) {
		t.Fatalf("[1] should be null (source was null)")
	}
	if !arr.IsValid(2) || arr.Value(2) != 4 {
		t.Fatalf("[2]=%d", arr.Value(2))
	}
	if !arr.IsValid(3) || arr.Value(3) != 8 {
		t.Fatalf("[3]=%d", arr.Value(3))
	}
}

func TestCumSumEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.CumSum(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Fatal("empty in → empty out")
	}
}

func TestDiffInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 3, 6, 10}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Diff(1, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	if arr.IsValid(0) {
		t.Fatal("[0] should be null")
	}
	want := []int64{0, 2, 3, 4}
	for i := 1; i < 4; i++ {
		if arr.Value(i) != want[i] {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), want[i])
		}
	}
}

func TestDiffPeriods2(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{10, 20, 30, 40, 50}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Diff(2, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	if arr.IsValid(0) || arr.IsValid(1) {
		t.Fatal("[0..2] should be null")
	}
	for i := 2; i < 5; i++ {
		if arr.Value(i) != 20 {
			t.Fatalf("[%d]=%d want 20", i, arr.Value(i))
		}
	}
}

func TestDiffZeroIsError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	if _, err := s.Diff(0, series.WithAllocator(alloc)); err == nil {
		t.Fatal("Diff(0) should error")
	}
}

func TestDiffNegative(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 3, 6}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Diff(-1, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	// Diff(-1): out[i] = s[i] - s[i+1]. Last becomes null.
	if !arr.IsValid(0) || arr.Value(0) != -2 {
		t.Fatalf("[0]=%d", arr.Value(0))
	}
	if !arr.IsValid(1) || arr.Value(1) != -3 {
		t.Fatalf("[1]=%d", arr.Value(1))
	}
	if arr.IsValid(2) {
		t.Fatal("[2] should be null")
	}
}
