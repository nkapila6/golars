package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestUniqueInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, err := series.FromInt64("a", []int64{3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5}, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	u, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer u.Release()
	arr := u.Chunk(0).(*array.Int64)
	want := []int64{3, 1, 4, 5, 9, 2, 6}
	if arr.Len() != len(want) {
		t.Fatalf("unique int64 len = %d, want %d", arr.Len(), len(want))
	}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("unique[%d]=%d want %d", i, arr.Value(i), v)
		}
	}
}

func TestUniqueInt64WithNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, err := series.FromInt64("a",
		[]int64{1, 0, 2, 0, 1},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	u, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer u.Release()
	if u.Len() != 3 {
		t.Fatalf("len=%d want 3", u.Len())
	}
	if u.NullCount() != 1 {
		t.Fatalf("nullcount=%d want 1", u.NullCount())
	}
}

func TestUniqueFloat64NaNCollapse(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	nan1 := math.NaN()
	nan2 := math.Float64frombits(math.Float64bits(nan1) | 1)
	s, err := series.FromFloat64("a",
		[]float64{0, -0, nan1, 1.0, nan2, 1.0},
		nil,
		series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	u, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer u.Release()
	if u.Len() != 3 {
		t.Fatalf("len=%d want 3 (0, NaN, 1)", u.Len())
	}
}

func TestUniqueBool(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	s, err := series.FromBool("a", []bool{true, false, true, true, false}, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	u, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer u.Release()
	if u.Len() != 2 {
		t.Fatalf("len=%d want 2", u.Len())
	}
}

func TestUniqueString(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	s, err := series.FromString("a", []string{"a", "b", "a", "c", "b"}, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	u, err := s.Unique(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer u.Release()
	if u.Len() != 3 {
		t.Fatalf("len=%d want 3", u.Len())
	}
	n, err := s.NUnique()
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("nunique=%d want 3", n)
	}
}
