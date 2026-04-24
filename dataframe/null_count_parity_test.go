// Behavioural parity with polars' tests/unit/dataframe/test_null_count.py.
// See NOTICE for license & attribution details. Fresh Go tests written
// against golars' API using the polars scenarios as a specification.
//
// Polars returns df.null_count() as a single-row DataFrame of per-
// column null totals. golars doesn't (yet) surface that exact shape,
// so we assert the equivalent invariant column-by-column via
// Series.NullCount(). See TODO at the bottom for the missing method.

package dataframe_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_null_count (property test): for every column, the
// reported null count equals the number of None values in the source.
// We verify this directly across mixed dtypes.
func TestParityNullCountPerColumn(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	i64, _ := series.FromInt64("i",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	f64, _ := series.FromFloat64("f",
		[]float64{0.1, 0, 0, 0.4, 0},
		[]bool{true, false, false, true, false},
		series.WithAllocator(alloc))
	str, _ := series.FromString("s",
		[]string{"a", "", "c", "", ""},
		[]bool{true, false, true, false, false},
		series.WithAllocator(alloc))
	boo, _ := series.FromBool("b",
		[]bool{true, false, true, false, true},
		nil, series.WithAllocator(alloc))

	df, err := dataframe.New(i64, f64, str, boo)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	want := map[string]int{"i": 2, "f": 3, "s": 3, "b": 0}
	for name, wantCount := range want {
		c, err := df.Column(name)
		if err != nil {
			t.Fatal(err)
		}
		if c.NullCount() != wantCount {
			t.Fatalf("column %q null count = %d, want %d",
				name, c.NullCount(), wantCount)
		}
	}
}

// Polars: test_null_count explicit example: zero-col DataFrame.
func TestParityNullCountEmptyFrame(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df, err := dataframe.New()
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if df.Width() != 0 {
		t.Fatalf("width = %d want 0", df.Width())
	}
	if df.Height() != 0 {
		t.Fatalf("height = %d want 0", df.Height())
	}
}

// Polars: test_null_count explicit example: zero-ROW, multi-col.
// A schema-only frame still reports widthCols columns, each with
// null_count == 0.
func TestParityNullCountZeroRows(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	x, _ := series.FromInt64("x", nil, nil, series.WithAllocator(alloc))
	y, _ := series.FromInt64("y", nil, nil, series.WithAllocator(alloc))
	z, _ := series.FromInt64("z", nil, nil, series.WithAllocator(alloc))
	df, err := dataframe.New(x, y, z)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if df.Height() != 0 {
		t.Fatalf("height = %d want 0", df.Height())
	}
	if df.Width() != 3 {
		t.Fatalf("width = %d want 3", df.Width())
	}
	for _, c := range df.Columns() {
		if c.NullCount() != 0 {
			t.Fatalf("column %q on zero-row frame has null count %d, want 0",
				c.Name(), c.NullCount())
		}
	}
}

// Polars: test_null_count: single column of all nulls reports its
// full length as the null count.
func TestParityNullCountAllNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// 10 rows, every row is null.
	valid := make([]bool, 10) // zero = all false = all null
	vals := make([]int64, 10)
	s, _ := series.FromInt64("col", vals, valid, series.WithAllocator(alloc))
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if got := df.ColumnAt(0).NullCount(); got != 10 {
		t.Fatalf("all-null column null count = %d, want 10", got)
	}
}

// Polars: test_null_count: no nulls anywhere reports 0 without
// materialising a validity bitmap.
func TestParityNullCountNoNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("col", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if got := df.ColumnAt(0).NullCount(); got != 0 {
		t.Fatalf("no-null column reports %d, want 0", got)
	}
}

// TODO(polars-parity): implement DataFrame.NullCount() returning a
// single-row DataFrame with per-column totals (matching
// pl.DataFrame.null_count). Currently callers must iterate columns
// and call Series.NullCount() directly; when the method lands, a new
// test here should assert the 1×ncols result shape + row values.
