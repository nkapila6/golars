package lazy_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func benchFilterProjectDF(n int) *dataframe.DataFrame {
	a := make([]int64, n)
	b := make([]int64, n)
	for i := range a {
		a[i] = int64(i)
		b[i] = int64(i * 3)
	}
	sa, _ := series.FromInt64("a", a, nil)
	sb, _ := series.FromInt64("b", b, nil)
	df, _ := dataframe.New(sa, sb)
	return df
}

func BenchmarkLazyEagerVsStreaming(b *testing.B) {
	ctx := context.Background()

	for _, n := range []int{1 << 16, 1 << 20} {
		df := benchFilterProjectDF(n)

		lf := lazy.FromDataFrame(df).
			Filter(expr.Col("a").GtLit(int64(n/4))).
			WithColumns(expr.Col("a").Add(expr.Col("b")).Alias("s")).
			Select(expr.Col("a"), expr.Col("s"))

		b.Run("eager-"+benchLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n) * 16)
			for b.Loop() {
				out, err := lf.Collect(ctx)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})

		b.Run("streaming-"+benchLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n) * 16)
			for b.Loop() {
				out, err := lf.Collect(ctx, lazy.WithStreaming())
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})

		df.Release()
	}
}

func benchLabel(n int) string {
	switch {
	case n >= 1<<20:
		return formatMi(n)
	case n >= 1<<10:
		return formatKi(n)
	}
	return formatN(n)
}

func formatMi(n int) string {
	return itoa(n>>20) + "Mi"
}

func formatKi(n int) string {
	return itoa(n>>10) + "Ki"
}

func formatN(n int) string { return itoa(n) }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func BenchmarkLazyStreamingWorkers(b *testing.B) {
	ctx := context.Background()
	df := benchFilterProjectDF(1 << 20)
	defer df.Release()

	lf := lazy.FromDataFrame(df).
		Filter(expr.Col("a").GtLit(int64(1<<18))).
		WithColumns(expr.Col("a").Add(expr.Col("b")).Alias("s")).
		Select(expr.Col("a"), expr.Col("s"))

	for _, w := range []int{1, 2, 4, 8} {
		b.Run("workers="+itoa(w), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(1<<20) * 16)
			for b.Loop() {
				out, err := lf.Collect(ctx,
					lazy.WithStreaming(),
					lazy.WithStreamingMorselRows(16*1024),
					lazy.WithStreamingWorkers(w))
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}
