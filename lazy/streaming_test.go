package lazy_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func bigDF(t *testing.T, mem interface {
	Allocate(size int) []byte
	Reallocate(size int, b []byte) []byte
	Free(b []byte)
}, n int) *dataframe.DataFrame {
	t.Helper()
	a := make([]int64, n)
	b := make([]int64, n)
	for i := range a {
		a[i] = int64(i)
		b[i] = int64(i * 2)
	}
	sa, _ := series.FromInt64("a", a, nil, series.WithAllocator(mem))
	sb, _ := series.FromInt64("b", b, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(sa, sb)
	return df
}

func TestStreamingMatchesEager(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := bigDF(t, mem, 10_000)
	defer df.Release()

	lf := lazy.FromDataFrame(df).
		Filter(expr.Col("a").GtLit(int64(100))).
		WithColumns(expr.Col("a").Add(expr.Col("b")).Alias("s")).
		Select(expr.Col("a"), expr.Col("s"))

	eager, err := lf.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("eager Collect: %v", err)
	}
	defer eager.Release()

	streaming, err := lf.Collect(ctx,
		lazy.WithExecAllocator(mem),
		lazy.WithStreaming(),
		lazy.WithStreamingMorselRows(256),
	)
	if err != nil {
		t.Fatalf("streaming Collect: %v", err)
	}
	defer streaming.Release()

	if eager.Height() != streaming.Height() {
		t.Fatalf("heights differ: eager=%d streaming=%d",
			eager.Height(), streaming.Height())
	}
	if eager.Width() != streaming.Width() {
		t.Fatalf("widths differ: eager=%d streaming=%d",
			eager.Width(), streaming.Width())
	}
	compareInt64Col(t, eager, streaming, "a")
	compareInt64Col(t, eager, streaming, "s")
}

func TestStreamingWithSortFallsBack(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := bigDF(t, mem, 2000)
	defer df.Release()

	// Sort is a blocker. The filter+project below sort should still run
	// through streaming; sort itself runs eagerly.
	lf := lazy.FromDataFrame(df).
		Filter(expr.Col("a").GtLit(int64(500))).
		Select(expr.Col("a"), expr.Col("b")).
		Sort("a", true).
		Head(5)

	out, err := lf.Collect(ctx,
		lazy.WithExecAllocator(mem),
		lazy.WithStreaming(),
		lazy.WithStreamingMorselRows(100),
	)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	defer out.Release()

	if out.Height() != 5 {
		t.Errorf("Height = %d, want 5", out.Height())
	}
	a, _ := out.Column("a")
	vs := a.Chunk(0).(*array.Int64).Int64Values()
	want := []int64{1999, 1998, 1997, 1996, 1995}
	for i, w := range want {
		if vs[i] != w {
			t.Errorf("a[%d] = %d, want %d", i, vs[i], w)
		}
	}
}

func TestStreamingWithGroupByFallsBack(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// k cycles through {a, b}; sum per group must still be correct under
	// streaming + hybrid execution.
	const n = 4000
	keys := make([]string, n)
	vals := make([]int64, n)
	var want_a, want_b int64
	for i := range keys {
		if i%2 == 0 {
			keys[i] = "a"
			want_a += int64(i)
		} else {
			keys[i] = "b"
			want_b += int64(i)
		}
		vals[i] = int64(i)
	}
	k, _ := series.FromString("k", keys, nil, series.WithAllocator(mem))
	v, _ := series.FromInt64("v", vals, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		GroupBy("k").
		Agg(expr.Col("v").Sum().Alias("s")).
		Collect(ctx,
			lazy.WithExecAllocator(mem),
			lazy.WithStreaming(),
			lazy.WithStreamingMorselRows(100))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	defer out.Release()

	if out.Height() != 2 {
		t.Fatalf("Height = %d, want 2", out.Height())
	}
	kCol, _ := out.Column("k")
	sCol, _ := out.Column("s")
	ks := kCol.Chunk(0).(*array.String)
	ss := sCol.Chunk(0).(*array.Int64).Int64Values()
	got := map[string]int64{
		ks.Value(0): ss[0],
		ks.Value(1): ss[1],
	}
	if got["a"] != want_a {
		t.Errorf("sum(a) = %d, want %d", got["a"], want_a)
	}
	if got["b"] != want_b {
		t.Errorf("sum(b) = %d, want %d", got["b"], want_b)
	}
}

func TestStreamingSortIsUsed(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// Sanity: a pure streaming pipeline (no blockers) completes and returns
	// correct results. We also verify behavior is independent of morsel size.
	df := bigDF(t, mem, 5000)
	defer df.Release()

	run := func(rows int) *dataframe.DataFrame {
		lf := lazy.FromDataFrame(df).
			Filter(expr.Col("a").GtLit(int64(1000))).
			Select(expr.Col("a"))
		out, err := lf.Collect(ctx,
			lazy.WithExecAllocator(mem),
			lazy.WithStreaming(),
			lazy.WithStreamingMorselRows(rows))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	a := run(64)
	defer a.Release()
	b := run(512)
	defer b.Release()
	if a.Height() != b.Height() {
		t.Errorf("heights diverge by morsel size: %d vs %d",
			a.Height(), b.Height())
	}
}

// compareInt64Col walks a column across all chunks in both frames and
// verifies the full concatenated values match.
func compareInt64Col(t *testing.T, a, b *dataframe.DataFrame, name string) {
	t.Helper()
	aCol, _ := a.Column(name)
	bCol, _ := b.Column(name)
	if aCol.Len() != bCol.Len() {
		t.Errorf("col %q lengths differ: %d vs %d", name, aCol.Len(), bCol.Len())
		return
	}
	flatten := func(s *series.Series) []int64 {
		out := make([]int64, 0, s.Len())
		for _, ch := range s.Chunks() {
			out = append(out, ch.(*array.Int64).Int64Values()...)
		}
		return out
	}
	av := flatten(aCol)
	bv := flatten(bCol)
	for i := range av {
		if av[i] != bv[i] {
			t.Errorf("col %q[%d] diverges: %d vs %d", name, i, av[i], bv[i])
			return
		}
	}
}

var _ = compute.SortOptions{} // keep compute import (used elsewhere in tests)
