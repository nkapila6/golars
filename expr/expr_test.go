package expr_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
)

func TestChildrenAndWithChildren(t *testing.T) {
	t.Parallel()
	e := expr.Col("a").Add(expr.Col("b"))
	kids := expr.Children(e)
	if len(kids) != 2 {
		t.Fatalf("expected 2 children, got %d", len(kids))
	}
	rebuilt := expr.WithChildren(e, []expr.Expr{expr.LitInt64(5), kids[1]})
	if rebuilt.String() != "(5 + col(\"b\"))" {
		t.Errorf("rebuilt = %s, want (5 + col(\"b\"))", rebuilt)
	}
}

func TestColumnsReferenced(t *testing.T) {
	t.Parallel()
	e := expr.Col("a").Mul(expr.Col("b")).Add(expr.Col("a"))
	cols := expr.Columns(e)
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Errorf("Columns = %v, want [a b]", cols)
	}
}

func TestOutputName(t *testing.T) {
	t.Parallel()
	if got := expr.OutputName(expr.Col("a").Add(expr.LitInt64(1))); got != "a" {
		t.Errorf("OutputName = %q, want a", got)
	}
	if got := expr.OutputName(expr.Col("a").Add(expr.LitInt64(1)).Alias("b")); got != "b" {
		t.Errorf("OutputName = %q, want b", got)
	}
}

func TestHashStable(t *testing.T) {
	t.Parallel()
	a := expr.Col("x").Add(expr.LitInt64(1))
	b := expr.Col("x").Add(expr.LitInt64(1))
	if a.Hash() != b.Hash() {
		t.Errorf("equal expressions should hash equally")
	}
	c := expr.Col("x").Add(expr.LitInt64(2))
	if a.Hash() == c.Hash() {
		t.Errorf("different expressions should not collide (given our hash space)")
	}
}

func TestEqualExprs(t *testing.T) {
	t.Parallel()
	a := expr.Col("x").Add(expr.LitInt64(1))
	b := expr.Col("x").Add(expr.LitInt64(1))
	if !expr.Equal(a, b) {
		t.Errorf("a and b should be equal")
	}
}

func TestCastCollapse(t *testing.T) {
	t.Parallel()
	e := expr.Col("x").Cast(dtype.Int64()).Cast(dtype.Int64())
	if e.String() != "col(\"x\").cast(i64)" {
		t.Errorf("cast collapse failed: %s", e)
	}
}

func TestAliasCollapse(t *testing.T) {
	t.Parallel()
	e := expr.Col("x").Alias("a").Alias("b")
	if e.String() != "col(\"x\").alias(\"b\")" {
		t.Errorf("alias collapse failed: %s", e)
	}
}

func TestContainsAgg(t *testing.T) {
	t.Parallel()
	if expr.ContainsAgg(expr.Col("x").Add(expr.LitInt64(1))) {
		t.Error("expected no agg")
	}
	if !expr.ContainsAgg(expr.Col("x").Sum().Add(expr.LitInt64(1))) {
		t.Error("expected agg found")
	}
}

func TestLitTypes(t *testing.T) {
	t.Parallel()
	if got := expr.LitInt64(42).String(); got != "42" {
		t.Errorf("LitInt64 = %q, want 42", got)
	}
	if got := expr.LitBool(true).String(); got != "true" {
		t.Errorf("LitBool = %q, want true", got)
	}
	if got := expr.LitString("hi").String(); got != `"hi"` {
		t.Errorf("LitString = %q, want \"hi\"", got)
	}
}
