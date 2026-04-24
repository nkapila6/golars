package dataframe

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/series"
)

// Corr returns a k-by-k Pearson correlation matrix over the numeric
// columns of df. The output DataFrame has one row per column,
// columns matching the input names, plus a leading "" column naming
// each row. Mirrors polars DataFrame.corr() shape.
func (df *DataFrame) Corr(_ context.Context) (*DataFrame, error) {
	return df.matrixPair("corr", func(a, b *series.Series) (float64, error) {
		return a.PearsonCorr(b)
	})
}

// Cov returns the k-by-k sample covariance matrix (ddof=1) over the
// numeric columns. Layout matches Corr.
func (df *DataFrame) Cov(_ context.Context, ddof int) (*DataFrame, error) {
	return df.matrixPair("cov", func(a, b *series.Series) (float64, error) {
		return a.Covariance(b, ddof)
	})
}

func (df *DataFrame) matrixPair(op string, pairFn func(a, b *series.Series) (float64, error)) (*DataFrame, error) {
	var numeric []*series.Series
	for _, c := range df.cols {
		if c.DType().IsNumeric() {
			numeric = append(numeric, c)
		}
	}
	if len(numeric) == 0 {
		return nil, fmt.Errorf("dataframe.%s: no numeric columns", op)
	}
	k := len(numeric)
	// Build the label column listing each row's column name.
	labels := make([]string, k)
	for i, c := range numeric {
		labels[i] = c.Name()
	}
	labelCol, err := series.FromString("", labels, nil)
	if err != nil {
		return nil, err
	}
	outCols := make([]*series.Series, 0, k+1)
	outCols = append(outCols, labelCol)
	// Precompute the k*k matrix.
	for j, b := range numeric {
		vals := make([]float64, k)
		valid := make([]bool, k)
		for i, a := range numeric {
			v, err := pairFn(a, b)
			if err != nil {
				for _, prev := range outCols {
					prev.Release()
				}
				return nil, err
			}
			vals[i] = v
			valid[i] = !isNaN(v)
		}
		col, err := series.FromFloat64(numeric[j].Name(), vals, valid)
		if err != nil {
			for _, prev := range outCols {
				prev.Release()
			}
			return nil, err
		}
		outCols = append(outCols, col)
	}
	return New(outCols...)
}

func isNaN(v float64) bool { return v != v }
