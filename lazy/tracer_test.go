package lazy_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

type fakeTracer struct{ spans atomic.Int32 }

func (t *fakeTracer) StartSpan(ctx context.Context, name string) (context.Context, lazy.Span) {
	t.spans.Add(1)
	return ctx, &fakeSpan{}
}

type fakeSpan struct{}

func (*fakeSpan) End()                    {}
func (*fakeSpan) SetAttr(_ string, _ any) {}

func TestTracerWrapsEveryNode(t *testing.T) {
	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil)
	df, _ := dataframe.New(a)
	defer df.Release()
	tr := &fakeTracer{}
	out, err := lazy.FromDataFrame(df).
		Filter(expr.Col("a").Gt(expr.Lit(int64(0)))).
		Select(expr.Col("a")).
		Collect(context.Background(), lazy.WithTracer(tr))
	if err != nil {
		t.Fatal(err)
	}
	out.Release()
	if tr.spans.Load() < 2 {
		t.Fatalf("expected at least 2 spans, got %d", tr.spans.Load())
	}
}
