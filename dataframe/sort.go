package dataframe

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// SortOption configures DataFrame.Sort and SortBy.
type SortOption func(*sortConfig)

type sortConfig struct {
	alloc memory.Allocator
}

func resolveSort(opts []SortOption) sortConfig {
	c := sortConfig{alloc: memory.DefaultAllocator}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithSortAllocator overrides the allocator used for intermediate arrays.
func WithSortAllocator(alloc memory.Allocator) SortOption {
	return func(c *sortConfig) { c.alloc = alloc }
}

// Sort returns a new DataFrame sorted by one column. Stable sort.
func (df *DataFrame) Sort(ctx context.Context, by string, desc bool, opts ...SortOption) (*DataFrame, error) {
	return df.SortBy(ctx,
		[]string{by},
		[]compute.SortOptions{{Descending: desc}},
		opts...)
}

// SortBy returns a new DataFrame sorted by the given columns with per-column
// options.
func (df *DataFrame) SortBy(ctx context.Context, keys []string, so []compute.SortOptions, opts ...SortOption) (*DataFrame, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("dataframe: SortBy requires at least one key")
	}
	if len(so) == 0 {
		so = make([]compute.SortOptions, len(keys))
	}
	if len(so) != len(keys) {
		return nil, fmt.Errorf("dataframe: SortBy got %d keys but %d options", len(keys), len(so))
	}
	cfg := resolveSort(opts)

	keyCols := make([]*series.Series, len(keys))
	for i, k := range keys {
		col, err := df.Column(k)
		if err != nil {
			return nil, err
		}
		keyCols[i] = col
	}

	idx, err := compute.SortIndicesMulti(ctx, keyCols, so, compute.WithAllocator(cfg.alloc))
	if err != nil {
		return nil, err
	}

	outCols := make([]*series.Series, df.Width())
	for i, c := range df.cols {
		out, err := compute.Take(ctx, c, idx, compute.WithAllocator(cfg.alloc))
		if err != nil {
			for _, r := range outCols[:i] {
				if r != nil {
					r.Release()
				}
			}
			return nil, err
		}
		outCols[i] = out
	}
	return &DataFrame{sch: df.sch, cols: outCols, height: df.height}, nil
}
