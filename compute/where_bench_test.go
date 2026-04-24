package compute_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// BenchmarkWhereInt64Direct exercises the no-null fast path of
// compute.Where at several sizes, isolating the blend kernel (and
// our parallel fan-out) from the surrounding expr / lazy-frame
// machinery that dominates benchWhenThen in cmd/bench.
func BenchmarkWhereInt64Direct(b *testing.B) {
	for _, rows := range []int{16 << 10, 64 << 10, 256 << 10, 1 << 20} {
		b.Run(sizeLabel(rows), func(b *testing.B) {
			r := rand.New(rand.NewPCG(42, 43))
			vals := make([]int64, rows)
			bs := make([]bool, rows)
			for i := range vals {
				vals[i] = r.Int64N(1 << 20)
				bs[i] = r.IntN(2) == 0
			}
			a, _ := series.FromInt64("a", vals, nil)
			bvals, _ := series.FromInt64("b", vals, nil)
			cond, _ := series.FromBool("c", bs, nil)
			defer a.Release()
			defer bvals.Release()
			defer cond.Release()

			ctx := context.Background()
			b.SetBytes(int64(rows) * 8)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := compute.Where(ctx, cond, a, bvals)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

func sizeLabel(n int) string {
	switch n {
	case 16 << 10:
		return "16Ki"
	case 64 << 10:
		return "64Ki"
	case 256 << 10:
		return "256Ki"
	case 1 << 20:
		return "1Mi"
	}
	return ""
}
