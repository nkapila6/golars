package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestFillNullInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.FillNull(int64(99), series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.NullCount() != 0 {
		t.Fatalf("FillNull left nulls: count=%d", out.NullCount())
	}
	arr := out.Chunk(0).(*array.Int64)
	want := []int64{1, 99, 3, 99, 5}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%d want %d", i, arr.Value(i), w)
		}
	}
}

func TestFillNullFloat64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		[]float64{1, 0, 3, 0},
		[]bool{true, false, true, false},
		series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.FillNull(-1.5, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	if arr.Value(1) != -1.5 || arr.Value(3) != -1.5 {
		t.Fatalf("fill got %v", []float64{arr.Value(0), arr.Value(1), arr.Value(2), arr.Value(3)})
	}
}

func TestFillNullString(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("a", []string{"a", "", "c", ""}, []bool{true, false, true, false}, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.FillNull("NA", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.String)
	if arr.Value(1) != "NA" || arr.Value(3) != "NA" {
		t.Fatal("fill string failed")
	}
}

func TestFillNullBool(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromBool("a", []bool{true, false, false, true}, []bool{true, false, true, false}, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.FillNull(true, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Boolean)
	if !arr.Value(1) || !arr.Value(3) {
		t.Fatal("fill bool failed")
	}
	if arr.Value(0) != true || arr.Value(2) != false {
		t.Fatal("non-null values mutated")
	}
}

func TestFillNullWrongType(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 0}, []bool{true, false}, series.WithAllocator(alloc))
	defer s.Release()
	_, err := s.FillNull("string not int", series.WithAllocator(alloc))
	if err == nil {
		t.Fatal("expected type mismatch error")
	}
}

func TestFillNullNoNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.FillNull(int64(99), series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.NullCount() != 0 {
		t.Fatal("should have no nulls")
	}
	arr := out.Chunk(0).(*array.Int64)
	for i, w := range []int64{1, 2, 3} {
		if arr.Value(i) != w {
			t.Fatalf("mutated: [%d]=%d", i, arr.Value(i))
		}
	}
}
