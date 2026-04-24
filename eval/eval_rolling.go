package eval

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// rollingOptsFrom unpacks the two-int Params (window, min_periods)
// attached by expr.Rolling*.
func rollingOptsFrom(n expr.FunctionNode) series.RollingOptions {
	var opts series.RollingOptions
	if len(n.Params) >= 1 {
		if w, ok := n.Params[0].(int); ok {
			opts.WindowSize = w
		}
	}
	if len(n.Params) >= 2 {
		if mp, ok := n.Params[1].(int); ok {
			opts.MinPeriods = mp
		}
	}
	return opts
}

// dispatchRolling routes by kernel name.
func dispatchRolling(name string, s *series.Series, opts series.RollingOptions) (*series.Series, error) {
	switch name {
	case "rolling_sum":
		return s.RollingSum(opts)
	case "rolling_mean":
		return s.RollingMean(opts)
	case "rolling_min":
		return s.RollingMin(opts)
	case "rolling_max":
		return s.RollingMax(opts)
	case "rolling_std":
		return s.RollingStd(opts)
	case "rolling_var":
		return s.RollingVar(opts)
	}
	return nil, fmt.Errorf("eval: unknown rolling op %q", name)
}
