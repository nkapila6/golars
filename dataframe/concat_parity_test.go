// Behavioural parity with polars' tests/unit/dataframe/test_vstack.py.
// See NOTICE for license & attribution details. No code copied; these
// tests were written fresh against golars' API using the polars
// scenarios as a specification.
//
// Polars' `df.vstack(other)` ≡ golars' `df.VStack(other)` (and the
// variadic `dataframe.Concat(...)`).
//
// Scenarios NOT ported:
//   - in-place vstack (golars frames are immutable)
//   - nested-null lists         (no List dtype)
//   - bad input type            (Go's static typing handles this)

package dataframe_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// readAllInt64 flattens every chunk of an int64 column into a slice.
// Concat / VStack produces multi-chunk columns; polars' assertions
// compare against logical row order, not physical layout.
func readAllInt64(t *testing.T, s *series.Series) []int64 {
	t.Helper()
	out := make([]int64, 0, s.Len())
	for _, c := range s.Chunks() {
		a := c.(*array.Int64)
		for i := range a.Len() {
			out = append(out, a.Value(i))
		}
	}
	return out
}

func readAllString(t *testing.T, s *series.Series) []string {
	t.Helper()
	out := make([]string, 0, s.Len())
	for _, c := range s.Chunks() {
		a := c.(*array.String)
		for i := range a.Len() {
			out = append(out, a.Value(i))
		}
	}
	return out
}

func testFrameTriple(t *testing.T, alloc memory.Allocator, foo []int64, bar []int64, ham []string) *dataframe.DataFrame {
	t.Helper()
	f, err := series.FromInt64("foo", foo, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	b, err := series.FromInt64("bar", bar, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	h, err := series.FromString("ham", ham, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(f, b, h)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// Polars: test_vstack: two 2-row frames with matching schema stack
// to a 4-row frame preserving column order and dtypes.
func TestParityVStackBasic(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df1 := testFrameTriple(t, alloc, []int64{1, 2}, []int64{6, 7}, []string{"a", "b"})
	defer df1.Release()
	df2 := testFrameTriple(t, alloc, []int64{3, 4}, []int64{8, 9}, []string{"c", "d"})
	defer df2.Release()

	out, err := df1.VStack(df2)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Height() != 4 || out.Width() != 3 {
		t.Fatalf("shape %d×%d want 4×3", out.Height(), out.Width())
	}
	if got := out.Schema().Names(); !strSliceEq(got, []string{"foo", "bar", "ham"}) {
		t.Fatalf("column order = %v", got)
	}
	foo := readAllInt64(t, out.ColumnAt(0))
	ham := readAllString(t, out.ColumnAt(2))
	if !int64SliceEq(foo, []int64{1, 2, 3, 4}) {
		t.Fatalf("foo=%v", foo)
	}
	if !strSliceEq(ham, []string{"a", "b", "c", "d"}) {
		t.Fatalf("ham=%v", ham)
	}
}

// Polars: test_vstack_self: a frame stacked onto itself doubles rows.
// Verifies refcount correctness: the same arrow buffer is shared.
func TestParityVStackSelf(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := testFrameTriple(t, alloc, []int64{1, 2}, []int64{6, 7}, []string{"a", "b"})
	defer df.Release()

	out, err := df.VStack(df)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Height() != 4 {
		t.Fatalf("self-stack height = %d, want 4", out.Height())
	}
	foo := readAllInt64(t, out.ColumnAt(0))
	if !int64SliceEq(foo, []int64{1, 2, 1, 2}) {
		t.Fatalf("foo=%v", foo)
	}
}

// Polars: test_vstack_column_number_mismatch: differing width errors.
func TestParityVStackColumnNumberMismatch(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df1 := testFrameTriple(t, alloc, []int64{1, 2}, []int64{6, 7}, []string{"a", "b"})
	defer df1.Release()
	df2 := df1.Drop("ham")
	defer df2.Release()

	if _, err := df1.VStack(df2); err == nil {
		t.Fatal("width mismatch must error")
	}
}

// Polars: test_vstack_column_name_mismatch: same width, different
// column names → error.
func TestParityVStackColumnNameMismatch(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df1 := testFrameTriple(t, alloc, []int64{1, 2}, []int64{6, 7}, []string{"a", "b"})
	defer df1.Release()

	// Build df2 with the same dtypes but a different column name in
	// position 0 so we hit the NAME-mismatch path, not dtype.
	renamed, err := df1.Rename("foo", "oof")
	if err != nil {
		t.Fatal(err)
	}
	defer renamed.Release()

	_, err = df1.VStack(renamed)
	if err == nil {
		t.Fatal("name mismatch must error")
	}
	if !strings.Contains(err.Error(), "name") {
		t.Fatalf("error should mention name mismatch: %v", err)
	}
}

// Polars: test_vstack_with_null_column: stacking a frame of pure-Null
// dtype onto a typed frame. golars does not expose a pure-Null column
// constructor, so we emulate by using an all-null Int64 column; this
// still exercises the "schema agrees, all values null" path.
//
// TODO(polars-parity): when/if golars grows a Null dtype, switch this
// to the polars-style mixed-dtype widen-to-f64 test.
func TestParityVStackWithAllNullsColumn(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("x", []float64{3.5}, nil, series.WithAllocator(alloc))
	aDF, err := dataframe.New(a)
	if err != nil {
		t.Fatal(err)
	}
	defer aDF.Release()

	// All-null column matching dtype. This is closer to polars'
	// behaviour AFTER the Null-to-f64 cast is applied: so the
	// observable result (one f64 null stacked onto [3.5]) matches.
	b, _ := series.FromFloat64("x", []float64{0}, []bool{false}, series.WithAllocator(alloc))
	bDF, err := dataframe.New(b)
	if err != nil {
		t.Fatal(err)
	}
	defer bDF.Release()

	out, err := aDF.VStack(bDF)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Height() != 2 {
		t.Fatalf("height=%d want 2", out.Height())
	}
	if got := out.ColumnAt(0).NullCount(); got != 1 {
		t.Fatalf("null count = %d want 1", got)
	}
	// Chunk 0 is the typed 3.5; chunk 1 is the null row. Verify each.
	c0 := out.ColumnAt(0).Chunks()[0].(*array.Float64)
	if !c0.IsValid(0) || c0.Value(0) != 3.5 {
		t.Fatalf("row 0 should be 3.5, got valid=%v v=%v", c0.IsValid(0), c0.Value(0))
	}
	c1 := out.ColumnAt(0).Chunks()[1].(*array.Float64)
	if c1.IsValid(0) {
		t.Fatal("row 1 should be null")
	}
}

// Golars-specific: variadic Concat matches the composition of two
// pairwise VStacks. Polars doesn't have this exact API; we verify
// internal consistency so the Concat path stays aligned with VStack.
func TestParityConcatMatchesRepeatedVStack(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df1 := testFrameTriple(t, alloc, []int64{1}, []int64{10}, []string{"a"})
	defer df1.Release()
	df2 := testFrameTriple(t, alloc, []int64{2}, []int64{20}, []string{"b"})
	defer df2.Release()
	df3 := testFrameTriple(t, alloc, []int64{3}, []int64{30}, []string{"c"})
	defer df3.Release()

	viaConcat, err := dataframe.Concat(df1, df2, df3)
	if err != nil {
		t.Fatal(err)
	}
	defer viaConcat.Release()

	viaPair1, _ := df1.VStack(df2)
	defer viaPair1.Release()
	viaPair2, _ := viaPair1.VStack(df3)
	defer viaPair2.Release()

	if viaConcat.Height() != viaPair2.Height() {
		t.Fatalf("height mismatch: Concat=%d VStack-chain=%d",
			viaConcat.Height(), viaPair2.Height())
	}
	a := readAllInt64(t, viaConcat.ColumnAt(0))
	b := readAllInt64(t, viaPair2.ColumnAt(0))
	if !int64SliceEq(a, b) {
		t.Fatalf("divergence: Concat=%v VStack-chain=%v", a, b)
	}
}

// Polars: the zero-arg Concat yields an empty frame rather than
// erroring. Keeps callers that conditionally accumulate frames from
// needing a guard.
func TestParityConcatEmpty(t *testing.T) {
	out, err := dataframe.Concat()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 0 || out.Width() != 0 {
		t.Fatalf("shape %d×%d, want 0×0", out.Height(), out.Width())
	}
}

func strSliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func int64SliceEq(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
