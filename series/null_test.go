package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestIsNullNoNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	m, err := s.IsNull(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Release()
	arr := m.Chunk(0).(*array.Boolean)
	if arr.Len() != 3 {
		t.Fatalf("len=%d", arr.Len())
	}
	for i := range 3 {
		if arr.Value(i) {
			t.Fatalf("IsNull[%d] should be false", i)
		}
	}
}

func TestIsNullWithNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 0, 3, 0}, []bool{true, false, true, false}, series.WithAllocator(alloc))
	defer s.Release()
	m, err := s.IsNull(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Release()
	arr := m.Chunk(0).(*array.Boolean)
	want := []bool{false, true, false, true}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("IsNull[%d]=%v want %v", i, arr.Value(i), w)
		}
	}
}

func TestIsNotNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a", []float64{1.0, 0, 3.0, 0}, []bool{true, false, true, false}, series.WithAllocator(alloc))
	defer s.Release()
	m, err := s.IsNotNull(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Release()
	arr := m.Chunk(0).(*array.Boolean)
	want := []bool{true, false, true, false}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("IsNotNull[%d]=%v want %v", i, arr.Value(i), w)
		}
	}
}

func TestIsNullEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{}, nil, series.WithAllocator(alloc))
	defer s.Release()
	m, err := s.IsNull(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Release()
	if m.Len() != 0 {
		t.Fatalf("empty input should give empty output, got len=%d", m.Len())
	}
}

// IsNull on a 65-element (crosses byte boundary) series ensures the
// bit-packing handles tail bits correctly.
func TestIsNullCrossByteBoundary(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	n := 65
	vals := make([]int64, n)
	valid := make([]bool, n)
	for i := range n {
		valid[i] = i%3 != 0 // every 3rd is null
	}
	s, _ := series.FromInt64("a", vals, valid, series.WithAllocator(alloc))
	defer s.Release()
	m, err := s.IsNull(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Release()
	arr := m.Chunk(0).(*array.Boolean)
	for i := range n {
		want := i%3 == 0
		if arr.Value(i) != want {
			t.Fatalf("IsNull[%d]=%v want %v", i, arr.Value(i), want)
		}
	}
}
