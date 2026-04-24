package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestValueCountsInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 1, 3, 2, 1}, nil, series.WithAllocator(alloc))
	defer s.Release()
	vals, counts, err := s.ValueCounts(false, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()
	vArr := vals.Chunk(0).(*array.Int64)
	cArr := counts.Chunk(0).(*array.Uint32)
	if vArr.Len() != 3 || cArr.Len() != 3 {
		t.Fatalf("len mismatch: %d %d", vArr.Len(), cArr.Len())
	}
	// first-occurrence order: 1, 2, 3
	if vArr.Value(0) != 1 || vArr.Value(1) != 2 || vArr.Value(2) != 3 {
		t.Fatalf("values got %v %v %v", vArr.Value(0), vArr.Value(1), vArr.Value(2))
	}
	if cArr.Value(0) != 3 || cArr.Value(1) != 2 || cArr.Value(2) != 1 {
		t.Fatalf("counts got %v %v %v", cArr.Value(0), cArr.Value(1), cArr.Value(2))
	}
}

func TestValueCountsSortedByCount(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Arrange the occurrence order so that unsorted would put the
	// min-count first: [3, 1, 1, 2, 2, 2]. Unsorted would be 3→2→1.
	// Sorted by count desc: 2, 1, 3.
	s, _ := series.FromInt64("a", []int64{3, 1, 1, 2, 2, 2}, nil, series.WithAllocator(alloc))
	defer s.Release()
	vals, counts, err := s.ValueCounts(true, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()
	vArr := vals.Chunk(0).(*array.Int64)
	cArr := counts.Chunk(0).(*array.Uint32)
	if vArr.Value(0) != 2 || cArr.Value(0) != 3 {
		t.Fatalf("top should be (2, 3); got (%d, %d)", vArr.Value(0), cArr.Value(0))
	}
	if cArr.Value(1) < cArr.Value(2) {
		t.Fatal("counts not descending")
	}
}

func TestValueCountsString(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("a", []string{"x", "y", "x", "y", "z"}, nil, series.WithAllocator(alloc))
	defer s.Release()
	vals, counts, err := s.ValueCounts(false, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()
	if vals.Len() != 3 {
		t.Fatalf("len=%d", vals.Len())
	}
}

func TestValueCountsBool(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromBool("a", []bool{true, false, true, true, false, true}, nil, series.WithAllocator(alloc))
	defer s.Release()
	vals, counts, err := s.ValueCounts(true, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()
	vArr := vals.Chunk(0).(*array.Boolean)
	cArr := counts.Chunk(0).(*array.Uint32)
	if !vArr.Value(0) || cArr.Value(0) != 4 {
		t.Fatal("expected (true, 4) first")
	}
	if vArr.Value(1) || cArr.Value(1) != 2 {
		t.Fatal("expected (false, 2) second")
	}
}

func TestValueCountsIgnoresNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 1, 0},
		[]bool{true, false, true, false},
		series.WithAllocator(alloc))
	defer s.Release()
	vals, counts, err := s.ValueCounts(false, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()
	if vals.Len() != 1 {
		t.Fatalf("should have 1 distinct non-null value, got %d", vals.Len())
	}
}

func TestValueCountsEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{}, nil, series.WithAllocator(alloc))
	defer s.Release()
	vals, counts, err := s.ValueCounts(false, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer vals.Release()
	defer counts.Release()
	if vals.Len() != 0 || counts.Len() != 0 {
		t.Fatal("empty input → empty output")
	}
}
