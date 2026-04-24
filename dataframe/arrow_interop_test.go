package dataframe_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestArrowInteropRoundtrip(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromString("b", []string{"x", "y", "z"}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	// DataFrame → arrow.Table → DataFrame.
	tbl := df.ToArrowTable()
	defer tbl.Release()
	if tbl.NumCols() != 2 || tbl.NumRows() != 3 {
		t.Fatalf("table shape = (%d, %d), want (2, 3)", tbl.NumCols(), tbl.NumRows())
	}
	back, err := dataframe.FromArrowTable(tbl)
	if err != nil {
		t.Fatal(err)
	}
	defer back.Release()

	if !back.Equals(df) {
		t.Errorf("roundtripped DataFrame != original")
	}
}

func TestFromMap(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df, err := dataframe.FromMap(map[string]any{
		"id":   []int64{1, 2, 3},
		"name": []string{"a", "b", "c"},
	}, []string{"id", "name"})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if df.Width() != 2 || df.Height() != 3 {
		t.Fatalf("shape = (%d, %d)", df.Width(), df.Height())
	}
	names := df.ColumnNames()
	if names[0] != "id" || names[1] != "name" {
		t.Errorf("column order = %v, want [id name]", names)
	}
}
