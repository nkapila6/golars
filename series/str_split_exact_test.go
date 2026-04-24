package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestStrSplitExact(t *testing.T) {
	s, _ := series.FromString("tags",
		[]string{"a,b,c", "", "x"},
		[]bool{true, true, true})
	defer s.Release()

	out, err := s.Str().SplitExact(",")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	la := out.Chunk(0).(*array.List)
	if la.Len() != 3 {
		t.Fatalf("len=%d want 3", la.Len())
	}
	offs := la.Offsets()
	// Row 0 -> 3 elements; row 1 -> 1 ("") ; row 2 -> 1 ("x")
	wantLens := []int32{3, 1, 1}
	for i, w := range wantLens {
		got := offs[i+1] - offs[i]
		if got != w {
			t.Errorf("row %d len=%d want %d", i, got, w)
		}
	}
	vals := la.ListValues().(*array.String)
	if vals.Value(0) != "a" || vals.Value(1) != "b" || vals.Value(2) != "c" {
		t.Errorf("row0 elements wrong: %q %q %q", vals.Value(0), vals.Value(1), vals.Value(2))
	}
	if vals.Value(4) != "x" {
		t.Errorf("row2 element wrong: %q", vals.Value(4))
	}
}

func TestStrSplitExactNull(t *testing.T) {
	s, _ := series.FromString("tags",
		[]string{"a,b", ""},
		[]bool{false, true})
	defer s.Release()
	out, err := s.Str().SplitExact(",")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	la := out.Chunk(0).(*array.List)
	if !la.IsNull(0) {
		t.Errorf("row 0 should be null")
	}
}
