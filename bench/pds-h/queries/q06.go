package queries

import (
	"path/filepath"

	"github.com/Gaurav-Gosain/golars"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q6: forecasting revenue change.
//
// Standard TPC-H:
//
//	SELECT SUM(l_extendedprice * l_discount) AS revenue
//	FROM lineitem
//	WHERE l_shipdate >= DATE '1994-01-01'
//	  AND l_shipdate <  DATE '1995-01-01'
//	  AND l_discount BETWEEN 0.05 AND 0.07
//	  AND l_quantity < 24;
//
// Same date-literal caveat as Q1: we drop the shipdate range predicate
// until LitDate lands. The remaining predicates (discount BETWEEN
// 0.05 AND 0.07, quantity < 24) still exercise the scan-side
// predicate pushdown path.
func Q6(dataDir string) (lazy.LazyFrame, error) {
	lineitem := golars.ScanParquet(filepath.Join(dataDir, "lineitem.parquet"))

	discount := expr.Col("l_discount")
	quantity := expr.Col("l_quantity")

	predicate := discount.Ge(expr.LitFloat64(0.05)).
		And(discount.Le(expr.LitFloat64(0.07))).
		And(quantity.Lt(expr.LitFloat64(24)))

	out := lineitem.
		Filter(predicate).
		Select(
			expr.Col("l_extendedprice").
				Mul(discount).
				Sum().
				Alias("revenue"),
		)

	return out, nil
}
