package series_test

import (
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/series"
)

func buildFNInt64(n int) *series.Series {
	vals := make([]int64, n)
	valid := make([]bool, n)
	r := rand.New(rand.NewPCG(7, 8))
	for i := range vals {
		vals[i] = int64(i)
		valid[i] = r.Float64() >= 0.3
	}
	s, _ := series.FromInt64("x", vals, valid)
	return s
}

func BenchmarkFillNullInt64_1M(b *testing.B) {
	s := buildFNInt64(1 << 20)
	defer s.Release()
	b.ReportAllocs()
	b.SetBytes(int64((1 << 20) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := s.FillNull(int64(0))
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkFillNullInt64_262k(b *testing.B) {
	s := buildFNInt64(262144)
	defer s.Release()
	b.ReportAllocs()
	b.SetBytes(int64(262144 * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := s.FillNull(int64(0))
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
