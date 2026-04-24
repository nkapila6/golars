package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestSeriesFillNanFloat64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("x",
		[]float64{1, math.NaN(), 3, math.NaN()},
		nil,
		series.WithAllocator(alloc))
	defer s.Release()

	out, err := s.FillNan(0, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Float64)
	want := []float64{1, 0, 3, 0}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestSeriesFillNanPreservesNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("x",
		[]float64{1, math.NaN(), 3, 4},
		[]bool{true, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	out, err := s.FillNan(99, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Float64)
	if !arr.IsValid(0) || arr.Value(0) != 1 {
		t.Fatalf("row 0: want 1, got valid=%v v=%v", arr.IsValid(0), arr.Value(0))
	}
	if !arr.IsValid(1) || arr.Value(1) != 99 {
		t.Fatalf("row 1: want 99, got valid=%v v=%v", arr.IsValid(1), arr.Value(1))
	}
	if arr.IsValid(2) {
		t.Fatalf("row 2: original null must stay null")
	}
	if !arr.IsValid(3) || arr.Value(3) != 4 {
		t.Fatalf("row 3: want 4, got valid=%v v=%v", arr.IsValid(3), arr.Value(3))
	}
}

func TestSeriesFillNanIdentityOnInteger(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.FillNan(0)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	if arr.Value(0) != 1 || arr.Value(1) != 2 || arr.Value(2) != 3 {
		t.Fatal("int64 FillNan must be identity")
	}
}

func TestSeriesForwardFillInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x",
		[]int64{0, 1, 0, 0, 5, 0},
		[]bool{false, true, false, false, true, false},
		series.WithAllocator(alloc))
	defer s.Release()

	out, err := s.ForwardFill(0, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Int64)
	// Leading null stays null; then 1 propagates through the next two
	// positions; then 5 comes in and fills the final trailing null.
	if arr.IsValid(0) {
		t.Fatal("leading null must stay null")
	}
	wantRest := []int64{1, 1, 1, 5, 5}
	for i, v := range wantRest {
		idx := i + 1
		if !arr.IsValid(idx) || arr.Value(idx) != v {
			t.Fatalf("idx %d: want %d, got valid=%v v=%d", idx, v, arr.IsValid(idx), arr.Value(idx))
		}
	}
}

func TestSeriesForwardFillWithLimit(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x",
		[]int64{1, 0, 0, 0, 0, 5},
		[]bool{true, false, false, false, false, true},
		series.WithAllocator(alloc))
	defer s.Release()

	out, err := s.ForwardFill(2, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Int64)
	// limit=2: at most two consecutive fills after each value.
	expect := []struct {
		valid bool
		val   int64
	}{
		{true, 1}, {true, 1}, {true, 1}, {false, 0}, {false, 0}, {true, 5},
	}
	for i, e := range expect {
		if arr.IsValid(i) != e.valid {
			t.Fatalf("idx %d validity: got %v want %v", i, arr.IsValid(i), e.valid)
		}
		if e.valid && arr.Value(i) != e.val {
			t.Fatalf("idx %d value: got %d want %d", i, arr.Value(i), e.val)
		}
	}
}

func TestSeriesBackwardFillString(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("x",
		[]string{"a", "", "", "b", ""},
		[]bool{true, false, false, true, false},
		series.WithAllocator(alloc))
	defer s.Release()

	out, err := s.BackwardFill(0, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.String)
	// Trailing null (idx=4) stays null; idx 1 and 2 fill with "b".
	want := []struct {
		valid bool
		val   string
	}{
		{true, "a"}, {true, "b"}, {true, "b"}, {true, "b"}, {false, ""},
	}
	for i, e := range want {
		if arr.IsValid(i) != e.valid {
			t.Fatalf("idx %d valid: got %v want %v", i, arr.IsValid(i), e.valid)
		}
		if e.valid && arr.Value(i) != e.val {
			t.Fatalf("idx %d value: got %q want %q", i, arr.Value(i), e.val)
		}
	}
}

func TestSeriesForwardFillNoNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.ForwardFill(0)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	for i, v := range []int64{1, 2, 3} {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %d want %d", i, arr.Value(i), v)
		}
	}
}
