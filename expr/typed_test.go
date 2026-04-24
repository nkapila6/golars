package expr_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
)

// TestTypedColMatchesUntyped proves the typed facade produces the
// same AST string as hand-rolled untyped calls. Any drift breaks
// the lazy planner's cse + simplification passes.
func TestTypedColMatchesUntyped(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		typed   expr.Expr
		untyped expr.Expr
	}{
		{
			name:    "gt-lit",
			typed:   expr.C[int64]("qty").Gt(2),
			untyped: expr.Col("qty").GtLit(int64(2)),
		},
		{
			name:    "eq-string",
			typed:   expr.C[string]("region").Eq("us"),
			untyped: expr.Col("region").EqLit("us"),
		},
		{
			name:    "arith-chain",
			typed:   expr.C[int64]("qty").Mul(2).Add(1).Expr,
			untyped: expr.Col("qty").MulLit(int64(2)).AddLit(int64(1)),
		},
		{
			name:    "same-col-add",
			typed:   expr.C[float64]("a").AddCol(expr.C[float64]("b")).Expr,
			untyped: expr.Col("a").Add(expr.Col("b")),
		},
		{
			name:    "agg-alias",
			typed:   expr.C[int64]("qty").Sum().As("total").Expr,
			untyped: expr.Col("qty").Sum().Alias("total"),
		},
		{
			name:    "mean-promotes-float64",
			typed:   expr.C[int64]("qty").Mean().Expr,
			untyped: expr.Col("qty").Mean(),
		},
		{
			name:    "cast-helper",
			typed:   expr.C[int64]("qty").CastFloat64().Expr,
			untyped: expr.Col("qty").Cast(dtype.Float64()),
		},
		{
			name:    "isnull",
			typed:   expr.C[string]("note").IsNull(),
			untyped: expr.Col("note").IsNull(),
		},
	}
	for _, tc := range cases {
		if tc.typed.String() != tc.untyped.String() {
			t.Errorf("%s: typed=%s untyped=%s", tc.name, tc.typed, tc.untyped)
		}
		if tc.typed.Hash() != tc.untyped.Hash() {
			t.Errorf("%s: hash mismatch typed=%d untyped=%d", tc.name, tc.typed.Hash(), tc.untyped.Hash())
		}
	}
}

// TestLitOfMatchesLit keeps the typed literal in lock-step with the
// untyped one so callers can mix the two freely.
func TestLitOfMatchesLit(t *testing.T) {
	t.Parallel()
	if expr.LitOf(int64(7)).String() != expr.Lit(int64(7)).String() {
		t.Error("LitOf[int64] diverges from Lit")
	}
	if expr.LitOf(3.14).String() != expr.Lit(3.14).String() {
		t.Error("LitOf[float64] diverges from Lit")
	}
	if expr.LitOf("hi").String() != expr.Lit("hi").String() {
		t.Error("LitOf[string] diverges from Lit")
	}
	if expr.LitOf(true).String() != expr.Lit(true).String() {
		t.Error("LitOf[bool] diverges from Lit")
	}
}
