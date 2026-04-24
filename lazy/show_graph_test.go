package lazy_test

import (
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestMermaidGraphShapes(t *testing.T) {
	dept, _ := series.FromString("dept", []string{"a", "b"}, nil)
	sal, _ := series.FromInt64("sal", []int64{100, 200}, nil)
	df, _ := dataframe.New(dept, sal)
	defer df.Release()

	plan := lazy.FromDataFrame(df).
		Filter(expr.Col("sal").GtLit(int64(50))).
		GroupBy("dept").
		Agg(expr.Col("sal").Sum().Alias("total")).
		Plan()

	out := lazy.MermaidGraph(plan)
	if !strings.HasPrefix(out, "graph TD\n") {
		t.Errorf("missing graph header: %q", out)
	}
	for _, marker := range []string{"SCAN", "FILTER", "AGG", "-->"} {
		if !strings.Contains(out, marker) {
			t.Errorf("mermaid output missing %q:\n%s", marker, out)
		}
	}
}
