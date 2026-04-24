package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestArgTrue(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromBool("b", []bool{false, true, false, true, true}, nil, series.WithAllocator(alloc))
	defer s.Release()
	idx, err := s.ArgTrue()
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1, 3, 4}
	if len(idx) != len(want) {
		t.Fatalf("len: got %d want %d", len(idx), len(want))
	}
	for i, v := range want {
		if idx[i] != v {
			t.Fatalf("idx[%d] = %d, want %d", i, idx[i], v)
		}
	}
}

func TestArgUnique(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{1, 2, 1, 3, 2, 4}, nil, series.WithAllocator(alloc))
	defer s.Release()
	idx, err := s.ArgUnique()
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{0, 1, 3, 5}
	if len(idx) != len(want) {
		t.Fatalf("len: got %d want %d", len(idx), len(want))
	}
	for i, v := range want {
		if idx[i] != v {
			t.Fatalf("idx[%d] = %d, want %d", i, idx[i], v)
		}
	}
}

func TestPeakMaxMin(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{1, 3, 2, 4, 1, 5, 0}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mx, err := s.PeakMax()
	if err != nil {
		t.Fatal(err)
	}
	defer mx.Release()
	arr := mx.Chunk(0).(*array.Boolean)
	// Local maxima in 1,3,2,4,1,5,0: positions 1 (3), 3 (4), 5 (5)
	want := []bool{false, true, false, true, false, true, false}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("PeakMax[%d]: got %v want %v", i, arr.Value(i), v)
		}
	}
	mn, _ := s.PeakMin()
	defer mn.Release()
	marr := mn.Chunk(0).(*array.Boolean)
	// Local minima: position 2 (2), 4 (1)
	wantMin := []bool{false, false, true, false, true, false, false}
	for i, v := range wantMin {
		if marr.Value(i) != v {
			t.Fatalf("PeakMin[%d]: got %v want %v", i, marr.Value(i), v)
		}
	}
}

func TestApproxNUnique(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// 1000 distinct values; HLL estimate should land within 2% at m=2^14.
	vals := make([]int64, 10000)
	for i := range vals {
		vals[i] = int64(i % 1000)
	}
	s, _ := series.FromInt64("x", vals, nil, series.WithAllocator(alloc))
	defer s.Release()
	est, err := s.ApproxNUnique()
	if err != nil {
		t.Fatal(err)
	}
	if est < 950 || est > 1050 {
		t.Fatalf("ApproxNUnique: got %d, want ~1000", est)
	}
}
