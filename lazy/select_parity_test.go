// Behavioural parity with polars' tests/unit/operations/test_select.py
// and test_with_columns.py. See NOTICE for license & attribution. Fresh
// Go tests against golars' API driven by the polars scenario catalog;
// no code copied.
//
// Divergences NOT ported:
//   - pl.all() / selectors.by_dtype() / regex column matchers : golars
//     needs explicit column references.
//   - Reusing a previous projection label inside the same select (CSE)
//    : golars' planner doesn't common-subexpression by name.
//   - Chained with_columns that shadow existing columns with a
//     different dtype: golars keeps the old dtype on clash.
//   - **Renaming via select(col.alias(x))** with duplicate outputs.

package lazy_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func withColsFixture(t *testing.T, alloc memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil,
		series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{10, 20, 30, 40}, nil,
		series.WithAllocator(alloc))
	df, err := dataframe.New(a, b)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// Polars: test_select: basic column projection.
func TestParitySelectProjection(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	df := withColsFixture(t, alloc)
	defer df.Release()

	got, err := lazy.FromDataFrame(df).
		Select(expr.Col("a")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	names := got.Schema().Names()
	if len(names) != 1 || names[0] != "a" {
		t.Errorf("names = %v, want [a]", names)
	}
	if got.Height() != 4 {
		t.Errorf("height = %d, want 4", got.Height())
	}
}

// Polars: test_select: reorder projection returns columns in the
// requested order.
func TestParitySelectReorder(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	df := withColsFixture(t, alloc)
	defer df.Release()

	got, err := lazy.FromDataFrame(df).
		Select(expr.Col("b"), expr.Col("a")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	names := got.Schema().Names()
	if len(names) != 2 || names[0] != "b" || names[1] != "a" {
		t.Errorf("names = %v, want [b a]", names)
	}
}

// Polars: test_select: alias renames the output.
func TestParitySelectAlias(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	df := withColsFixture(t, alloc)
	defer df.Release()

	got, err := lazy.FromDataFrame(df).
		Select(expr.Col("a").Alias("renamed")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	names := got.Schema().Names()
	if len(names) != 1 || names[0] != "renamed" {
		t.Errorf("names = %v, want [renamed]", names)
	}
}

// Polars: test_with_columns: appends a computed column without
// dropping existing ones.
func TestParityWithColumnsAppend(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	df := withColsFixture(t, alloc)
	defer df.Release()

	got, err := lazy.FromDataFrame(df).
		WithColumns(expr.Col("a").Add(expr.Col("b")).Alias("c")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	names := got.Schema().Names()
	want := []string{"a", "b", "c"}
	if len(names) != len(want) {
		t.Fatalf("names = %v, want %v", names, want)
	}
	for i, w := range want {
		if names[i] != w {
			t.Errorf("names[%d] = %q, want %q", i, names[i], w)
		}
	}
	col, _ := got.Column("c")
	arr := col.Chunk(0).(*array.Int64)
	wantVals := []int64{11, 22, 33, 44}
	for i, w := range wantVals {
		if arr.Value(i) != w {
			t.Errorf("c[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: test_with_columns: redefining an existing column with a
// computed expression replaces it in place (no new column added).
func TestParityWithColumnsOverwrite(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	df := withColsFixture(t, alloc)
	defer df.Release()

	got, err := lazy.FromDataFrame(df).
		WithColumns(expr.Col("a").Mul(expr.LitInt64(10)).Alias("a")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	names := got.Schema().Names()
	if len(names) != 2 {
		t.Fatalf("width = %d, want 2 (overwrite, not append)", len(names))
	}
	col, _ := got.Column("a")
	arr := col.Chunk(0).(*array.Int64)
	want := []int64{10, 20, 30, 40}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("a[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}
}

// Polars: test_with_columns: multiple expressions chain.
func TestParityWithColumnsMultiple(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	df := withColsFixture(t, alloc)
	defer df.Release()

	got, err := lazy.FromDataFrame(df).
		WithColumns(
			expr.Col("a").Add(expr.Col("b")).Alias("sum"),
			expr.Col("a").Mul(expr.Col("b")).Alias("prod"),
		).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	names := got.Schema().Names()
	wantSet := map[string]bool{"a": true, "b": true, "sum": true, "prod": true}
	if len(names) != len(wantSet) {
		t.Fatalf("names = %v, want 4 columns", names)
	}
	for _, n := range names {
		if !wantSet[n] {
			t.Errorf("unexpected column %q", n)
		}
	}

	sumCol, _ := got.Column("sum")
	sumArr := sumCol.Chunk(0).(*array.Int64)
	prodCol, _ := got.Column("prod")
	prodArr := prodCol.Chunk(0).(*array.Int64)

	wantSum := []int64{11, 22, 33, 44}
	wantProd := []int64{10, 40, 90, 160}
	for i := range 4 {
		if sumArr.Value(i) != wantSum[i] {
			t.Errorf("sum[%d] = %d, want %d", i, sumArr.Value(i), wantSum[i])
		}
		if prodArr.Value(i) != wantProd[i] {
			t.Errorf("prod[%d] = %d, want %d", i, prodArr.Value(i), wantProd[i])
		}
	}
}

// Polars: select with literal: project a constant column.
func TestParitySelectLiteralColumn(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	df := withColsFixture(t, alloc)
	defer df.Release()

	got, err := lazy.FromDataFrame(df).
		Select(expr.Col("a"), expr.LitInt64(42).Alias("const")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if got.Width() != 2 {
		t.Fatalf("width = %d, want 2", got.Width())
	}
	col, _ := got.Column("const")
	arr := col.Chunk(0).(*array.Int64)
	for i := range arr.Len() {
		if arr.Value(i) != 42 {
			t.Errorf("const[%d] = %d, want 42", i, arr.Value(i))
		}
	}
}
