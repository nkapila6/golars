package dataframe

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/series"
)

// FromMap builds a DataFrame from a map of column-name to Go slice.
// Supported slice types: []int64, []int32, []float64, []float32,
// []string, []bool. This mirrors polars' `pl.DataFrame({"a": [1,2,3]})`
// ergonomic; Go's map iteration is unordered, so the resulting column
// order is determined by the `order` argument. Pass nil for
// insertion-order-agnostic use (keys are sorted lexicographically).
//
// Example:
//
//	df, _ := dataframe.FromMap(map[string]any{
//	    "id":    []int64{1, 2, 3},
//	    "name":  []string{"a", "b", "c"},
//	    "value": []float64{1.5, 2.5, 3.5},
//	}, []string{"id", "name", "value"})
func FromMap(data map[string]any, order []string) (*DataFrame, error) {
	if order == nil {
		order = make([]string, 0, len(data))
		for k := range data {
			order = append(order, k)
		}
		sortStrings(order)
	}
	cols := make([]*series.Series, 0, len(order))
	for _, name := range order {
		v, ok := data[name]
		if !ok {
			// Release what we've built.
			for _, s := range cols {
				s.Release()
			}
			return nil, fmt.Errorf("dataframe.FromMap: missing key %q in data", name)
		}
		s, err := seriesFromAny(name, v)
		if err != nil {
			for _, s := range cols {
				s.Release()
			}
			return nil, err
		}
		cols = append(cols, s)
	}
	return New(cols...)
}

func seriesFromAny(name string, v any) (*series.Series, error) {
	switch x := v.(type) {
	case []int64:
		return series.FromInt64(name, x, nil)
	case []int:
		out := make([]int64, len(x))
		for i, v := range x {
			out[i] = int64(v)
		}
		return series.FromInt64(name, out, nil)
	case []int32:
		return series.FromInt32(name, x, nil)
	case []float64:
		return series.FromFloat64(name, x, nil)
	case []float32:
		return series.FromFloat32(name, x, nil)
	case []string:
		return series.FromString(name, x, nil)
	case []bool:
		return series.FromBool(name, x, nil)
	case *series.Series:
		return x.Clone(), nil
	}
	return nil, fmt.Errorf("dataframe.FromMap: unsupported slice type for %q: %T", name, v)
}

// sortStrings is a tiny in-place sort to avoid pulling sort into this
// file's imports. The caller rarely has more than a handful of keys.
func sortStrings(s []string) {
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
