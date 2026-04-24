package expr

import (
	"fmt"
	"strings"
)

// OverNode wraps an inner expression with a list of partition keys,
// turning `pl.col("x").sum()` into `pl.col("x").sum().over("k")`:
// the reduction is computed per-group and broadcast back to every
// row of the original frame.
type OverNode struct {
	Inner Expr
	Keys  []string
}

func (OverNode) isNode() {}

func (o OverNode) String() string {
	return fmt.Sprintf("over(%s, [%s])", o.Inner, strings.Join(o.Keys, ","))
}

// Over wraps e so it is evaluated separately per group defined by
// keys, then broadcast back to the original row order. Mirrors
// polars' expr.over(*keys).
func (e Expr) Over(keys ...string) Expr {
	return Expr{OverNode{Inner: e, Keys: keys}}
}
