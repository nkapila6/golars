package dataframe_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func frame(t *testing.T, alloc memory.Allocator, ids []int64, names []string) *dataframe.DataFrame {
	t.Helper()
	a, err := series.FromInt64("id", ids, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	b, err := series.FromString("name", names, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(a, b)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func TestConcatBasic(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := frame(t, alloc, []int64{1, 2}, []string{"x", "y"})
	defer a.Release()
	b := frame(t, alloc, []int64{3, 4, 5}, []string{"p", "q", "r"})
	defer b.Release()

	out, err := dataframe.Concat(a, b)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 5 {
		t.Fatalf("height=%d want 5", out.Height())
	}
	if out.Width() != 2 {
		t.Fatalf("width=%d want 2", out.Width())
	}
}

func TestConcatSchemaMismatch(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := frame(t, alloc, []int64{1}, []string{"x"})
	defer a.Release()
	// different column name
	idCol, _ := series.FromInt64("other", []int64{2}, nil, series.WithAllocator(alloc))
	nameCol, _ := series.FromString("name", []string{"y"}, nil, series.WithAllocator(alloc))
	b, err := dataframe.New(idCol, nameCol)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	if _, err := dataframe.Concat(a, b); err == nil {
		t.Fatal("expected schema mismatch")
	}
}

func TestConcatEmptyFrames(t *testing.T) {
	out, err := dataframe.Concat()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Width() != 0 || out.Height() != 0 {
		t.Fatal("zero-arg Concat must return empty")
	}
}

func TestConcatSingleClones(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := frame(t, alloc, []int64{1, 2}, []string{"x", "y"})
	defer a.Release()
	out, err := dataframe.Concat(a)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != a.Height() {
		t.Fatal("single-frame Concat should preserve height")
	}
}

func TestVStackAlias(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := frame(t, alloc, []int64{1}, []string{"x"})
	defer a.Release()
	b := frame(t, alloc, []int64{2, 3}, []string{"y", "z"})
	defer b.Release()

	out, err := a.VStack(b)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 3 {
		t.Fatalf("height=%d", out.Height())
	}
}
