package lazy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestProfilerCapturesEveryNode(t *testing.T) {
	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil)
	df, _ := dataframe.New(a)
	defer df.Release()

	p := lazy.NewProfiler()
	out, err := lazy.FromDataFrame(df).
		Filter(expr.Col("a").Gt(expr.Lit(int64(2)))).
		Select(expr.Col("a")).
		Collect(context.Background(), lazy.WithProfiler(p))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	spans := p.Spans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span")
	}
	// Sanity-check the report emits something coherent.
	rep := p.Report()
	if !strings.Contains(rep, "TOTAL") {
		t.Fatalf("report missing TOTAL line:\n%s", rep)
	}
	ct := p.ChromeTrace()
	if !strings.HasPrefix(ct, `{"traceEvents":[`) {
		t.Fatalf("chrome-trace malformed: %s", ct[:80])
	}
}
