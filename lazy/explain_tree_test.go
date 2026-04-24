package lazy_test

import (
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)


func buildPlan(t *testing.T) lazy.Node {
	t.Helper()
	dept, _ := series.FromString("dept",
		[]string{"eng", "sales", "eng", "ops"}, nil)
	salary, _ := series.FromInt64("salary",
		[]int64{100, 80, 115, 70}, nil)
	df, _ := dataframe.New(dept, salary)
	t.Cleanup(df.Release)
	return lazy.FromDataFrame(df).
		Filter(expr.Col("salary").Gt(expr.Lit(int64(75)))).
		GroupBy("dept").
		Agg(
			expr.Col("salary").Sum().Alias("total"),
			expr.Col("salary").Mean().Alias("avg"),
		).
		Sort("total", true).
		Plan()
}

func TestExplainTreeUnicode(t *testing.T) {
	t.Parallel()
	got := lazy.ExplainTree(buildPlan(t))
	// Root flush-left, each descendant prefixed with └── and
	// connector glyphs. Exact bytes matter: the test pins the
	// layout so future refactors don't silently change output.
	want := "" +
		"SORT [total desc]\n" +
		"└── AGG keys=[dept] [col(\"salary\").sum().alias(\"total\"), col(\"salary\").mean().alias(\"avg\")]\n" +
		"    └── FILTER (col(\"salary\") > 75)\n" +
		"        └── SCAN df\n"
	if got != want {
		t.Errorf("ExplainTree mismatch\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestExplainTreeASCII(t *testing.T) {
	t.Parallel()
	got := lazy.ExplainTreeASCII(buildPlan(t))
	// ASCII style swaps the box-drawing characters for |-- / `-- so
	// the output is safe on terminals that cannot render Unicode.
	if !strings.Contains(got, "`-- FILTER") {
		t.Errorf("ASCII tree missing `-- branch marker:\n%s", got)
	}
	if strings.ContainsAny(got, "├└│") {
		t.Errorf("ASCII tree leaked Unicode glyphs:\n%s", got)
	}
}

func TestExplainTreeBranching(t *testing.T) {
	t.Parallel()
	// Joins have two children; ExplainTree must draw ├── for the
	// first and └── for the last.
	left, _ := dataframe.New(mustSer("id", []int64{1, 2}))
	right, _ := dataframe.New(mustSer("id", []int64{1, 3}))
	t.Cleanup(func() { left.Release(); right.Release() })

	plan := lazy.FromDataFrame(left).
		Join(lazy.FromDataFrame(right), []string{"id"}, dataframe.InnerJoin).
		Plan()
	got := lazy.ExplainTree(plan)
	if !strings.Contains(got, "├── ") || !strings.Contains(got, "└── ") {
		t.Errorf("join tree missing both branch markers:\n%s", got)
	}
}

func mustSer(name string, vs []int64) *series.Series {
	s, err := series.FromInt64(name, vs, nil)
	if err != nil {
		panic(err)
	}
	return s
}

func TestExplainTreeFullSections(t *testing.T) {
	t.Parallel()
	out, err := lazy.ExplainTreeFull(buildPlan(t))
	if err != nil {
		t.Fatalf("ExplainTreeFull: %v", err)
	}
	for _, marker := range []string{
		"== Logical plan ==",
		"== Optimizer ==",
		"== Optimized plan ==",
		"└── ",
	} {
		if !strings.Contains(out, marker) {
			t.Errorf("ExplainTreeFull missing %q:\n%s", marker, out)
		}
	}
}
