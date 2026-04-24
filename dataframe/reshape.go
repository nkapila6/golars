package dataframe

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// TopK returns a DataFrame containing the k rows with the largest
// values in col. Ties are broken by input order (stable sort). k <= 0
// returns an empty frame. Mirrors polars' DataFrame.top_k(k, by=col).
func (df *DataFrame) TopK(ctx context.Context, k int, col string) (*DataFrame, error) {
	return df.topKImpl(ctx, k, col, true)
}

// BottomK returns the k rows with the smallest values in col.
func (df *DataFrame) BottomK(ctx context.Context, k int, col string) (*DataFrame, error) {
	return df.topKImpl(ctx, k, col, false)
}

func (df *DataFrame) topKImpl(ctx context.Context, k int, col string, descending bool) (*DataFrame, error) {
	if k <= 0 {
		return Empty(df.sch), nil
	}
	sorted, err := df.Sort(ctx, col, descending)
	if err != nil {
		return nil, err
	}
	defer sorted.Release()
	if k > sorted.Height() {
		k = sorted.Height()
	}
	return sorted.Slice(0, k)
}

// PartitionBy splits df into one DataFrame per distinct combination
// of values in keys. The returned slice preserves input order. Every
// DataFrame in the result must be Released by the caller. Mirrors
// polars' DataFrame.partition_by(keys).
func (df *DataFrame) PartitionBy(ctx context.Context, keys ...string) ([]*DataFrame, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("dataframe.PartitionBy: at least one key required")
	}
	height := df.Height()
	if height == 0 {
		return nil, nil
	}
	// Resolve the key columns so we can hash rows.
	keyCols := make([]*series.Series, len(keys))
	for i, k := range keys {
		c, err := df.Column(k)
		if err != nil {
			return nil, err
		}
		keyCols[i] = c
	}
	// Build per-row partition keys as a formatted string. This is
	// simple and dtype-agnostic; for performance-sensitive callers
	// future work can specialise on single-int / single-string keys.
	partitions := map[string][]int{}
	order := []string{}
	for row := range height {
		buf := make([]byte, 0, 32)
		for i, c := range keyCols {
			if i > 0 {
				buf = append(buf, 0x1f)
			}
			buf = append(buf, formatCell(c.Chunk(0), row)...)
		}
		key := string(buf)
		if _, ok := partitions[key]; !ok {
			order = append(order, key)
		}
		partitions[key] = append(partitions[key], row)
	}
	out := make([]*DataFrame, 0, len(order))
	for _, k := range order {
		rows := partitions[k]
		taken, err := gatherRows(ctx, df, rows)
		if err != nil {
			for _, p := range out {
				p.Release()
			}
			return nil, err
		}
		out = append(out, taken)
	}
	return out, nil
}

// Pipe chains a caller-provided function onto df. Useful for building
// pipelines without repeatedly unpacking (df, err). Mirrors polars'
// df.pipe(fn).
func (df *DataFrame) Pipe(fn func(*DataFrame) (*DataFrame, error)) (*DataFrame, error) {
	return fn(df)
}

// formatCell returns the stable string form of cell i in chunk, used
// by PartitionBy for its grouping key. Nulls become the empty string
// (polars' partition_by puts nulls in their own bucket, which this
// collapses to a single null bucket as well).
func formatCell(chunk any, i int) []byte {
	switch a := chunk.(type) {
	case *array.Int64:
		if !a.IsValid(i) {
			return []byte{0xff, 'n'}
		}
		return strconv.AppendInt(nil, a.Value(i), 10)
	case *array.Int32:
		if !a.IsValid(i) {
			return []byte{0xff, 'n'}
		}
		return strconv.AppendInt(nil, int64(a.Value(i)), 10)
	case *array.Float64:
		if !a.IsValid(i) {
			return []byte{0xff, 'n'}
		}
		return strconv.AppendFloat(nil, a.Value(i), 'g', -1, 64)
	case *array.Float32:
		if !a.IsValid(i) {
			return []byte{0xff, 'n'}
		}
		return strconv.AppendFloat(nil, float64(a.Value(i)), 'g', -1, 32)
	case *array.String:
		if !a.IsValid(i) {
			return []byte{0xff, 'n'}
		}
		return []byte(a.Value(i))
	case *array.Boolean:
		if !a.IsValid(i) {
			return []byte{0xff, 'n'}
		}
		if a.Value(i) {
			return []byte{'1'}
		}
		return []byte{'0'}
	}
	return nil
}

// _ keeps series used in sig even if PartitionBy specialises later.
var _ = series.FromInt64
