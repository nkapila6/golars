package compute_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestWhereInt64Correctness(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	rows := 1003 // intentionally odd to exercise the tail
	r := rand.New(rand.NewPCG(1, 2))
	a := make([]int64, rows)
	b := make([]int64, rows)
	c := make([]bool, rows)
	for i := range a {
		a[i] = r.Int64N(1_000_000)
		b[i] = -r.Int64N(1_000_000) - 1
		c[i] = i%3 == 0
	}
	aS, _ := series.FromInt64("a", a, nil, series.WithAllocator(alloc))
	bS, _ := series.FromInt64("b", b, nil, series.WithAllocator(alloc))
	cS, _ := series.FromBool("c", c, nil, series.WithAllocator(alloc))
	defer aS.Release()
	defer bS.Release()
	defer cS.Release()

	out, err := compute.Where(context.Background(), cS, aS, bS, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	for i := range rows {
		want := b[i]
		if c[i] {
			want = a[i]
		}
		if arr.Value(i) != want {
			t.Fatalf("row %d: got %d want %d (cond=%v)", i, arr.Value(i), want, c[i])
		}
	}
}

func TestWhereFloat64Correctness(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	rows := 67
	a := make([]float64, rows)
	b := make([]float64, rows)
	c := make([]bool, rows)
	for i := range a {
		a[i] = float64(i)
		b[i] = -float64(i) - 0.5
		c[i] = i%2 == 0
	}
	aS, _ := series.FromFloat64("a", a, nil, series.WithAllocator(alloc))
	bS, _ := series.FromFloat64("b", b, nil, series.WithAllocator(alloc))
	cS, _ := series.FromBool("c", c, nil, series.WithAllocator(alloc))
	defer aS.Release()
	defer bS.Release()
	defer cS.Release()

	out, err := compute.Where(context.Background(), cS, aS, bS, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	for i := range rows {
		want := b[i]
		if c[i] {
			want = a[i]
		}
		if arr.Value(i) != want {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), want)
		}
	}
}
