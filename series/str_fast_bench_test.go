package series_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// buildStringBench builds a string Series of size n with mostly-ASCII
// data of average byte-length meanLen. Deterministic per call: same
// seed -> same data.
func buildStringBench(n, meanLen int) *series.Series {
	r := rand.New(rand.NewPCG(42, 17))
	alphabet := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ,.-")
	values := make([]string, n)
	var buf []byte
	for i := range n {
		l := 1 + r.IntN(2*meanLen)
		buf = buf[:0]
		for range l {
			buf = append(buf, alphabet[r.IntN(len(alphabet))])
		}
		values[i] = string(buf)
	}
	s, err := series.FromString("s", values, nil)
	if err != nil {
		panic(err)
	}
	return s
}

var sink any

func BenchmarkStrContainsFast(b *testing.B) {
	for _, n := range []int{1024, 1 << 14, 1 << 20} {
		s := buildStringBench(n, 32)
		defer s.Release()
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			alloc := memory.NewGoAllocator()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r, err := s.Str().Contains("abc", series.WithAllocator(alloc))
				if err != nil {
					b.Fatal(err)
				}
				sink = r
				r.Release()
			}
		})
	}
}

func BenchmarkStrStartsWithFast(b *testing.B) {
	s := buildStringBench(1<<20, 32)
	defer s.Release()
	alloc := memory.NewGoAllocator()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r, err := s.Str().StartsWith("a", series.WithAllocator(alloc))
		if err != nil {
			b.Fatal(err)
		}
		sink = r
		r.Release()
	}
}

func BenchmarkStrLowerFast(b *testing.B) {
	for _, n := range []int{1024, 1 << 14, 1 << 20} {
		s := buildStringBench(n, 32)
		defer s.Release()
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			alloc := memory.NewGoAllocator()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r, err := s.Str().Lower(series.WithAllocator(alloc))
				if err != nil {
					b.Fatal(err)
				}
				sink = r
				r.Release()
			}
		})
	}
}

func BenchmarkStrLenBytesFast(b *testing.B) {
	s := buildStringBench(1<<20, 32)
	defer s.Release()
	alloc := memory.NewGoAllocator()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r, err := s.Str().LenBytes(series.WithAllocator(alloc))
		if err != nil {
			b.Fatal(err)
		}
		sink = r
		r.Release()
	}
}

func BenchmarkStrLike(b *testing.B) {
	s := buildStringBench(1<<20, 32)
	defer s.Release()
	patterns := []string{
		"%abc%",       // contains
		"abc%",        // starts_with
		"%abc",        // ends_with
		"%abc%def%",   // multi-segment
		"a_c%",        // with single-char wildcard
	}
	for _, pat := range patterns {
		b.Run(pat, func(b *testing.B) {
			alloc := memory.NewGoAllocator()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r, err := s.Str().Like(pat, series.WithAllocator(alloc))
				if err != nil {
					b.Fatal(err)
				}
				sink = r
				r.Release()
			}
		})
	}
}

// Baseline: mimic the old []bool-intermediate path explicitly so we
// can measure the delta vs the direct-buffer fast path.
func BenchmarkStrContainsBaseline(b *testing.B) {
	s := buildStringBench(1<<20, 32)
	defer s.Release()
	alloc := memory.NewGoAllocator()
	needle := "abc"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		arr := s.Chunk(0)
		stringArr := arr.(interface {
			Len() int
			Value(int) string
			IsValid(int) bool
			NullN() int
		})
		n := stringArr.Len()
		buf := make([]bool, n)
		for i := range n {
			if !stringArr.IsValid(i) {
				continue
			}
			buf[i] = strings.Contains(stringArr.Value(i), needle)
		}
		r, err := series.FromBool(s.Name(), buf, nil, series.WithAllocator(alloc))
		if err != nil {
			b.Fatal(err)
		}
		sink = r
		r.Release()
	}
}
