// profiled-bench runs a subset of the compare4 workloads with a CPU
// profile attached so we can see where real time goes across the
// benchmark suite, not just one workload in isolation.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"runtime/pprof"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	cpuProfile := flag.String("cpuprofile", "/tmp/bench.pprof", "cpu profile output")
	iters := flag.Int("iters", 1000, "iterations per workload")
	only := flag.String("only", "", "run only this workload")
	flag.Parse()

	f, err := os.Create(*cpuProfile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	ctx := context.Background()

	type workload struct {
		name string
		fn   func()
	}

	workloads := []workload{
		{"SortFloat64_16K", makeSortFloat64(ctx, 16384)},
		{"SortFloat64_262K", makeSortFloat64(ctx, 262144)},
		{"SortInt64_16K", makeSortInt64(ctx, 16384)},
		{"SortInt64_262K", makeSortInt64(ctx, 262144)},
		{"SortTwoKeys_16K", makeSortTwoKeys(ctx, 16384)},
		{"Take_16K", makeTake(ctx, 16384)},
		{"Take_262K", makeTake(ctx, 262144)},
		{"FilterInt64_262K", makeFilter(ctx, 262144)},
		{"SumOverGroup_262K", makeSumOverGroup(ctx, 262144)},
	}

	for _, w := range workloads {
		if *only != "" && w.name != *only {
			continue
		}
		fmt.Printf("=== %s ===\n", w.name)
		for i := 0; i < *iters; i++ {
			w.fn()
		}
	}
}

func makeSortFloat64(ctx context.Context, n int) func() {
	r := rand.New(rand.NewPCG(42, 43))
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = r.Float64() * 1e6
	}
	s, _ := series.FromFloat64("a", vals, nil)
	return func() {
		out, _ := compute.Sort(ctx, s, compute.SortOptions{})
		out.Release()
	}
}

func makeSortInt64(ctx context.Context, n int) func() {
	r := rand.New(rand.NewPCG(42, 43))
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = r.Int64N(1 << 20)
	}
	s, _ := series.FromInt64("a", vals, nil)
	return func() {
		out, _ := compute.Sort(ctx, s, compute.SortOptions{})
		out.Release()
	}
}

func makeSortTwoKeys(ctx context.Context, n int) func() {
	r := rand.New(rand.NewPCG(42, 43))
	k1 := make([]int64, n)
	k2 := make([]int64, n)
	for i := range k1 {
		k1[i] = r.Int64N(8)
		k2[i] = r.Int64N(1 << 20)
	}
	s1, _ := series.FromInt64("k1", k1, nil)
	s2, _ := series.FromInt64("k2", k2, nil)
	df, _ := dataframe.New(s1, s2)
	return func() {
		out, _ := df.SortBy(ctx, []string{"k1", "k2"}, []compute.SortOptions{{}, {}})
		out.Release()
	}
}

func makeTake(ctx context.Context, n int) func() {
	r := rand.New(rand.NewPCG(42, 43))
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = r.Int64N(1 << 20)
	}
	s, _ := series.FromInt64("a", vals, nil)
	perm := r.Perm(n)[:n/2]
	return func() {
		out, _ := compute.Take(ctx, s, perm)
		out.Release()
	}
}

func makeFilter(ctx context.Context, n int) func() {
	r := rand.New(rand.NewPCG(42, 43))
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = r.Int64N(1 << 20)
	}
	mask := make([]bool, n)
	for i := range mask {
		mask[i] = r.IntN(2) == 0
	}
	s, _ := series.FromInt64("a", vals, nil)
	m, _ := series.FromBool("m", mask, nil)
	return func() {
		out, _ := compute.Filter(ctx, s, m)
		out.Release()
	}
}

func makeSumOverGroup(ctx context.Context, n int) func() {
	r := rand.New(rand.NewPCG(42, 43))
	keys := make([]int64, n)
	vals := make([]int64, n)
	for i := range keys {
		keys[i] = r.Int64N(64)
		vals[i] = r.Int64N(1 << 20)
	}
	k, _ := series.FromInt64("k", keys, nil)
	v, _ := series.FromInt64("v", vals, nil)
	df, _ := dataframe.New(k, v)
	_ = expr.Col
	return func() {
		// Just do the groupby agg: shows similar hot path without lazy engine.
		out, _ := df.GroupBy("k").Agg(ctx, []expr.Expr{expr.Col("v").Sum().Alias("s")})
		out.Release()
	}
}
