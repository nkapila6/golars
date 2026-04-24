package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// consolidateInt64 extracts the full logical Int64 array from a Series,
// concatenating chunks if necessary. Shift now returns a 2-chunk view
// (zero-copy null prefix + source slice) so tests must go through the
// logical accessor rather than out.Chunk(0). Caller must Release the
// result (the *array.Int64 satisfies arrow.Array); we deliberately do
// NOT use t.Cleanup here because the checked-allocator's AssertSize
// defer runs before Cleanup.
func consolidateInt64(t *testing.T, s *series.Series) *array.Int64 {
	t.Helper()
	arr, err := s.Consolidated()
	if err != nil {
		t.Fatal(err)
	}
	return arr.(*array.Int64)
}

func TestShiftForward(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Shift(2, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := consolidateInt64(t, out)
	defer arr.Release()
	want := []int64{0, 0, 1, 2, 3}
	validExp := []bool{false, false, true, true, true}
	for i := range 5 {
		if arr.IsValid(i) != validExp[i] {
			t.Fatalf("valid[%d]=%v want %v", i, arr.IsValid(i), validExp[i])
		}
		if validExp[i] && arr.Value(i) != want[i] {
			t.Fatalf("val[%d]=%d want %d", i, arr.Value(i), want[i])
		}
	}
}

func TestShiftBackward(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Shift(-2, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := consolidateInt64(t, out)
	defer arr.Release()
	want := []int64{3, 4, 5}
	for i, w := range want {
		if !arr.IsValid(i) || arr.Value(i) != w {
			t.Fatalf("[%d]=%d/%v want %d", i, arr.Value(i), arr.IsValid(i), w)
		}
	}
	if arr.IsValid(3) || arr.IsValid(4) {
		t.Fatal("tail should be null")
	}
}

func TestShiftZero(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Shift(0, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := consolidateInt64(t, out)
	defer arr.Release()
	for i, w := range []int64{1, 2, 3} {
		if arr.Value(i) != w {
			t.Fatalf("shift 0 should copy: [%d]=%d", i, arr.Value(i))
		}
	}
}

func TestShiftExceedsLength(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Shift(10, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.NullCount() != 3 {
		t.Fatalf("huge shift should be all null, got %d nulls", out.NullCount())
	}
}

func TestShiftEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Shift(1, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Fatal("empty in → empty out")
	}
}

func TestShiftWithNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 4}, []bool{true, false, true, true},
		series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Shift(1, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	// Expected: [null, 1, null(was at 1), 3]
	arr := consolidateInt64(t, out)
	defer arr.Release()
	if arr.IsValid(0) {
		t.Fatal("[0] should be null (out of bounds)")
	}
	if !arr.IsValid(1) || arr.Value(1) != 1 {
		t.Fatal("[1] should be 1")
	}
	if arr.IsValid(2) {
		t.Fatal("[2] should be null (original [1] was null)")
	}
}
