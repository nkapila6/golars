package dataframe_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// BenchmarkGroupBySumInt64 varies the row count and the number of distinct
// groups. Low group-count = many rows per group (cheap per-group aggs, more
// sort work); high group-count = fewer rows per group (more per-group
// overhead).
func BenchmarkGroupBySumInt64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 14, 1 << 18} {
		for _, groups := range []int{8, 1024} {
			name := fmt.Sprintf("n=%d,groups=%d", n, groups)
			b.Run(name, func(b *testing.B) {
				keys := make([]int64, n)
				vals := make([]int64, n)
				for i := range keys {
					keys[i] = int64(i % groups)
					vals[i] = int64(i)
				}
				k, _ := series.FromInt64("k", keys, nil)
				v, _ := series.FromInt64("v", vals, nil)
				df, _ := dataframe.New(k, v)
				defer df.Release()

				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(n) * 16)
				for b.Loop() {
					out, err := df.GroupBy("k").Agg(ctx,
						[]expr.Expr{expr.Col("v").Sum().Alias("sum")})
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
	}
}
