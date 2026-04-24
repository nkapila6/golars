package dataframe

import (
	"context"
	"fmt"
	"math"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// SumAll returns a one-row DataFrame where each numeric column's value
// is the sum of that column in df. Non-numeric columns are dropped
// (matching polars' DataFrame.sum() which keeps only numerics). Mirrors
// polars' pl.DataFrame.sum().
func (df *DataFrame) SumAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceAll(ctx, "sum", func(s *series.Series) (float64, error) {
		return s.Sum()
	})
}

// MeanAll is SumAll for arithmetic means.
func (df *DataFrame) MeanAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceAll(ctx, "mean", func(s *series.Series) (float64, error) {
		return s.Mean()
	})
}

// MinAll returns a one-row DataFrame of column-wise minima (numeric
// columns only).
func (df *DataFrame) MinAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceAll(ctx, "min", func(s *series.Series) (float64, error) {
		return s.Min()
	})
}

// MaxAll is MinAll for maxima.
func (df *DataFrame) MaxAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceAll(ctx, "max", func(s *series.Series) (float64, error) {
		return s.Max()
	})
}

// StdAll returns a one-row DataFrame of column-wise sample standard
// deviations (ddof=1), matching polars' default.
func (df *DataFrame) StdAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceAll(ctx, "std", func(s *series.Series) (float64, error) {
		return s.Std()
	})
}

// VarAll returns a one-row DataFrame of column-wise sample variances
// (ddof=1).
func (df *DataFrame) VarAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceAll(ctx, "var", func(s *series.Series) (float64, error) {
		return s.Var()
	})
}

// MedianAll returns a one-row DataFrame of column-wise medians.
func (df *DataFrame) MedianAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceAll(ctx, "median", func(s *series.Series) (float64, error) {
		return s.Median()
	})
}

// CountAll returns a one-row DataFrame where each column's value is
// the count of non-null rows. Unlike Sum/Mean/Min/Max, Count is
// defined for every dtype and so every column of df appears in the
// output.
func (df *DataFrame) CountAll(_ context.Context) (*DataFrame, error) {
	cols := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		v := int64(c.Len() - c.NullCount())
		out, err := series.FromInt64(c.Name(), []int64{v}, nil)
		if err != nil {
			for _, prev := range cols[:i] {
				if prev != nil {
					prev.Release()
				}
			}
			return nil, err
		}
		cols[i] = out
	}
	return New(cols...)
}

// NullCountAll returns a one-row DataFrame of per-column null counts.
func (df *DataFrame) NullCountAll(_ context.Context) (*DataFrame, error) {
	cols := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		v := int64(c.NullCount())
		out, err := series.FromInt64(c.Name(), []int64{v}, nil)
		if err != nil {
			for _, prev := range cols[:i] {
				if prev != nil {
					prev.Release()
				}
			}
			return nil, err
		}
		cols[i] = out
	}
	return New(cols...)
}

// reduceAll applies reduce to every numeric column in df, building a
// one-row Float64 DataFrame. NaN propagates unchanged (polars matches
// this for empty/NaN inputs). Non-numeric columns are silently
// excluded.
func (df *DataFrame) reduceAll(_ context.Context, op string, reduce func(*series.Series) (float64, error)) (*DataFrame, error) {
	out := make([]*series.Series, 0, len(df.cols))
	for _, c := range df.cols {
		if !c.DType().IsNumeric() {
			continue
		}
		v, err := reduce(c)
		if err != nil {
			for _, prev := range out {
				prev.Release()
			}
			return nil, fmt.Errorf("dataframe.%sAll: column %q: %w", op, c.Name(), err)
		}
		valid := []bool{!math.IsNaN(v) || c.DType().IsFloating()}
		// For non-float inputs NaN cannot arise from the reduce
		// itself; Mean/Std/Median on an all-null int column returns
		// NaN by convention so we mark it null in the output.
		if math.IsNaN(v) && !c.DType().IsFloating() {
			valid[0] = false
		}
		s, err := series.FromFloat64(c.Name(), []float64{v}, valid)
		if err != nil {
			for _, prev := range out {
				prev.Release()
			}
			return nil, err
		}
		out = append(out, s)
	}
	return New(out...)
}

// Ensure dtype import stays referenced.
var _ = dtype.Float64
