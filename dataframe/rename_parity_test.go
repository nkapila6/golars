// Behavioural parity with polars' tests/unit/operations/test_rename.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Divergences NOT ported:
//   - Bulk-dict rename (`df.rename({"a": "x", "b": "y"})`)  : golars
//     exposes single-column Rename only; loop the callers.
//   - Lambda rename (`df.rename(lambda col: ...)`)          : Python-
//     specific convenience; Go code can compose loops.
//   - Swap-in-one-call (`{"a": "b", "b": "a"}`)             : the
//     intermediate "b" already exists, which would error under golars'
//     duplicate-column check. Test below asserts that error instead.

package dataframe_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_with_column_renamed: renaming a single column
// preserves the data and the order of other columns.
func TestParityRenameSingle(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{3, 4}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	got, err := df.Rename("b", "c")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	names := got.Schema().Names()
	if len(names) != 2 || names[0] != "a" || names[1] != "c" {
		t.Fatalf("names = %v, want [a c]", names)
	}
	col, _ := got.Column("c")
	arr := col.Chunk(0).(*array.Int64)
	if arr.Value(0) != 3 || arr.Value(1) != 4 {
		t.Errorf("c = [%d, %d], want [3, 4]", arr.Value(0), arr.Value(1))
	}
	if _, err := got.Column("b"); err == nil {
		t.Errorf("old name 'b' still present")
	}
}

// Polars: rename to the same name is a no-op.
func TestParityRenameIdentityNoOp(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	got, err := df.Rename("a", "a")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	names := got.Schema().Names()
	if len(names) != 1 || names[0] != "a" {
		t.Errorf("names = %v, want [a]", names)
	}
}

// Polars: renaming to an existing column name is an error (golars
// diverges here: polars allows the clash as a temporary state inside
// a bulk rename; golars refuses: see file header comment).
func TestParityRenameClashErrors(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{3, 4}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	_, err := df.Rename("a", "b")
	if !errors.Is(err, dataframe.ErrDuplicateColumn) {
		t.Errorf("err = %v, want ErrDuplicateColumn", err)
	}
}

// Polars: test_rename_lf: LazyFrame.rename flows through collect
// and preserves non-renamed columns.
func TestParityRenameLazy(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{2}, nil, series.WithAllocator(alloc))
	c, _ := series.FromInt64("c", []int64{3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Rename("a", "foo").
		Rename("b", "bar").
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	names := out.Schema().Names()
	want := []string{"foo", "bar", "c"}
	if len(names) != len(want) {
		t.Fatalf("names = %v, want %v", names, want)
	}
	for i, w := range want {
		if names[i] != w {
			t.Errorf("names[%d] = %q, want %q", i, names[i], w)
		}
	}
}

// Polars: unknown column rename is an error.
func TestParityRenameUnknownColumn(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	_, err := df.Rename("nonexistent", "x")
	if !errors.Is(err, dataframe.ErrColumnNotFound) {
		t.Errorf("err = %v, want ErrColumnNotFound", err)
	}
}
