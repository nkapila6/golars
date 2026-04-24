package dataframe

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// Sample returns n rows drawn from df. When withReplacement is false
// (the typical case) sampling is without replacement and n must be
// <= Height. seed is deterministic when non-zero; zero draws a fresh
// seed from crypto-less global rand.
//
// Selected rows keep their original relative order (ascending index).
// For a random permutation, follow Sample with an explicit shuffle
// (not yet implemented). Mirrors polars DataFrame.sample(n, with_replacement, seed).
func (df *DataFrame) Sample(ctx context.Context, n int, withReplacement bool, seed uint64) (*DataFrame, error) {
	if n < 0 {
		return nil, fmt.Errorf("dataframe: Sample n=%d must be non-negative", n)
	}
	h := df.Height()
	if n == 0 {
		return df.Head(0), nil
	}
	if !withReplacement && n > h {
		return nil, fmt.Errorf("dataframe: Sample n=%d exceeds height %d without replacement", n, h)
	}
	rng := newSampleRNG(seed)
	var indices []int
	if withReplacement {
		indices = make([]int, n)
		for i := range indices {
			indices[i] = rng.IntN(h)
		}
	} else {
		indices = pickDistinct(rng, h, n)
	}
	insertionSortInts(indices)
	return gatherRows(ctx, df, indices)
}

func newSampleRNG(seed uint64) *rand.Rand {
	if seed == 0 {
		return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return rand.New(rand.NewPCG(seed, ^seed))
}

// pickDistinct returns k distinct indices in [0, n), chosen uniformly
// at random. Reservoir for sparse samples (k << n) to avoid the n-sized
// allocation; Fisher-Yates when the sample is a large fraction of the
// population.
func pickDistinct(rng *rand.Rand, n, k int) []int {
	if k*4 < n {
		out := make([]int, k)
		for i := range k {
			out[i] = i
		}
		for i := k; i < n; i++ {
			j := rng.IntN(i + 1)
			if j < k {
				out[j] = i
			}
		}
		return out
	}
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	for i := n - 1; i > 0; i-- {
		j := rng.IntN(i + 1)
		idx[i], idx[j] = idx[j], idx[i]
	}
	return idx[:k]
}

func insertionSortInts(s []int) {
	for i := 1; i < len(s); i++ {
		v := s[i]
		j := i - 1
		for j >= 0 && s[j] > v {
			s[j+1] = s[j]
			j--
		}
		s[j+1] = v
	}
}

// gatherRows materializes a new DataFrame by calling compute.Take per
// column. On any error all previously-built columns are released.
func gatherRows(ctx context.Context, df *DataFrame, indices []int) (*DataFrame, error) {
	out := make([]*series.Series, 0, len(df.cols))
	for _, c := range df.cols {
		taken, err := compute.Take(ctx, c, indices)
		if err != nil {
			for _, p := range out {
				p.Release()
			}
			return nil, err
		}
		out = append(out, taken)
	}
	return New(out...)
}
