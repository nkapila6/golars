package lazy

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
)

// LazyFrame is a lazily-evaluated DataFrame. Every operation appends a node
// to its logical plan; Collect runs the optimizer and executor.
type LazyFrame struct {
	plan Node
}

// FromDataFrame wraps an in-memory DataFrame as a LazyFrame source.
func FromDataFrame(df *dataframe.DataFrame) LazyFrame {
	return LazyFrame{plan: DataFrameScan{Source: df, Length: -1}}
}

// Plan returns the current (unoptimized) logical plan.
func (lf LazyFrame) Plan() Node { return lf.plan }

// Schema returns the best-effort output schema without executing the plan.
func (lf LazyFrame) Schema() (*schema.Schema, error) { return lf.plan.Schema() }

// Select returns a LazyFrame whose output is the listed expressions.
func (lf LazyFrame) Select(exprs ...expr.Expr) LazyFrame {
	return LazyFrame{plan: Projection{Input: lf.plan, Exprs: exprs}}
}

// WithColumns returns a LazyFrame extended with the given computed columns.
func (lf LazyFrame) WithColumns(exprs ...expr.Expr) LazyFrame {
	return LazyFrame{plan: WithColumns{Input: lf.plan, Exprs: exprs}}
}

// Filter returns a LazyFrame restricted to rows where pred is true.
func (lf LazyFrame) Filter(pred expr.Expr) LazyFrame {
	return LazyFrame{plan: Filter{Input: lf.plan, Predicate: pred}}
}

// Sort sorts by one column.
func (lf LazyFrame) Sort(by string, desc bool) LazyFrame {
	return LazyFrame{plan: Sort{
		Input:   lf.plan,
		Keys:    []string{by},
		Options: []compute.SortOptions{{Descending: desc}},
	}}
}

// SortBy sorts by many columns with per-column options.
func (lf LazyFrame) SortBy(keys []string, opts []compute.SortOptions) LazyFrame {
	return LazyFrame{plan: Sort{Input: lf.plan, Keys: keys, Options: opts}}
}

// Slice restricts to a row range.
func (lf LazyFrame) Slice(offset, length int) LazyFrame {
	return LazyFrame{plan: SliceNode{Input: lf.plan, Offset: offset, Length: length}}
}

// Head is a shortcut for Slice(0, n).
func (lf LazyFrame) Head(n int) LazyFrame { return lf.Slice(0, n) }

// Limit is an alias for Head.
func (lf LazyFrame) Limit(n int) LazyFrame { return lf.Slice(0, n) }

// Rename renames a single column.
func (lf LazyFrame) Rename(oldName, newName string) LazyFrame {
	return LazyFrame{plan: Rename{Input: lf.plan, Old: oldName, New: newName}}
}

// Drop removes columns from the output.
func (lf LazyFrame) Drop(cols ...string) LazyFrame {
	return LazyFrame{plan: Drop{Input: lf.plan, Columns: cols}}
}

// LazyGroupBy is a pending group-by on a LazyFrame. Call Agg to turn it into
// a LazyFrame.
type LazyGroupBy struct {
	input Node
	keys  []string
}

// GroupBy starts a group-by on the given keys.
func (lf LazyFrame) GroupBy(keys ...string) LazyGroupBy {
	return LazyGroupBy{input: lf.plan, keys: keys}
}

// Agg closes the group-by.
//
// Polars lets `col(a).Mul(col(b)).Sum()` run directly inside an agg
// block because its planner auto-hoists the arithmetic into an
// implicit WithColumns. Our executor wants the agg input to be a
// bare column ref, so we rewrite the plan here: for each complex
// agg input we stage it as a synthetic column (`__agg_i`), insert a
// WithColumns above the Aggregate, and rewrite the agg to reference
// the staged column. Aliases are preserved so the user-visible
// output name is unchanged.
//
// Bare-column inputs (the fast path) pass through unchanged.
func (g LazyGroupBy) Agg(exprs ...expr.Expr) LazyFrame {
	hoisted := make([]expr.Expr, 0, len(exprs))
	rewritten := make([]expr.Expr, len(exprs))
	nextID := 0
	input := g.input
	for i, e := range exprs {
		re, hoist, ok := rewriteAggInput(e, &nextID)
		rewritten[i] = re
		if ok {
			hoisted = append(hoisted, hoist)
		}
	}
	if len(hoisted) > 0 {
		input = WithColumns{Input: input, Exprs: hoisted}
	}
	return LazyFrame{plan: Aggregate{Input: input, Keys: g.keys, Aggs: rewritten}}
}

// rewriteAggInput returns (rewritten agg expr, hoisted WithColumns
// expr, needs_hoist). When the agg's input is already a bare column
// reference, we return the original expr unchanged and
// needs_hoist=false. Otherwise we synthesise a column name, build a
// WithColumns expression that materialises the complex input under
// that name, and rewrite the agg to read from it.
//
// Supports `col.op()` and `col.op().alias("x")`. Filtered aggs
// (`col.filter(...).op()`) and other shapes pass through untouched;
// the downstream validator will reject them if the executor can't
// handle them.
func rewriteAggInput(e expr.Expr, nextID *int) (expr.Expr, expr.Expr, bool) {
	node := e.Node()
	var outerAlias string
	if alias, ok := node.(expr.AliasNode); ok {
		outerAlias = alias.Name
		node = alias.Inner.Node()
	}
	agg, ok := node.(expr.AggNode)
	if !ok {
		// Not an aggregation; pass through. dataframe.parseAggs will
		// surface a clear error if this is unsupported.
		return e, expr.Expr{}, false
	}
	// Already a bare column: nothing to hoist.
	if _, isCol := agg.Inner.Node().(expr.ColNode); isCol {
		return e, expr.Expr{}, false
	}
	// Hoist the inner expression.
	synth := fmt.Sprintf("__agg_%d", *nextID)
	*nextID++
	hoistExpr := agg.Inner.Alias(synth)
	// Rebuild the Agg via the public methods so we don't reach into
	// the unexported expr.Expr shape.
	newAgg, ok := rebuildAgg(agg.Op, expr.Col(synth))
	if !ok {
		// Unknown AggOp: can't hoist safely. Fall back to the
		// original shape; parseAggs will report the real error.
		return e, expr.Expr{}, false
	}
	if outerAlias != "" {
		newAgg = newAgg.Alias(outerAlias)
	} else {
		// Preserve the visible output name of the original agg. Polars
		// uses the inner expression's root name; a grouped sum of
		// `price*(1-disc)` shows up as `price` without this alias.
		newAgg = newAgg.Alias(expr.OutputName(e))
	}
	return newAgg, hoistExpr, true
}

// rebuildAgg reconstructs an aggregation of the given op applied to
// inner, using the public expr constructors so we don't touch Expr's
// unexported node field. Returns (expr, true) on success or
// (zero-value, false) for ops we don't know how to build.
func rebuildAgg(op expr.AggOp, inner expr.Expr) (expr.Expr, bool) {
	switch op {
	case expr.AggSum:
		return inner.Sum(), true
	case expr.AggMean:
		return inner.Mean(), true
	case expr.AggMin:
		return inner.Min(), true
	case expr.AggMax:
		return inner.Max(), true
	case expr.AggCount:
		return inner.Count(), true
	case expr.AggNullCount:
		return inner.NullCount(), true
	case expr.AggFirst:
		return inner.First(), true
	case expr.AggLast:
		return inner.Last(), true
	}
	return expr.Expr{}, false
}

// Join returns a LazyFrame that joins this frame with other on the given
// key columns.
func (lf LazyFrame) Join(other LazyFrame, on []string, how dataframe.JoinType) LazyFrame {
	return LazyFrame{plan: Join{Left: lf.plan, Right: other.plan, On: on, How: how}}
}

// keep dataframe import referenced via JoinType.
var _ dataframe.JoinType

// Collect runs the optimizer and executor and returns the resulting
// DataFrame. The returned DataFrame owns its buffers; the caller must
// Release.
//
// With the WithStreaming option, streaming-friendly prefixes of the plan
// run through the morsel-driven executor; pipeline breakers (Sort,
// Aggregate, Join) evaluate their upstream via streaming and then fall
// back to eager kernels.
func (lf LazyFrame) Collect(ctx context.Context, opts ...ExecOption) (*dataframe.DataFrame, error) {
	optimized, _, err := DefaultOptimizer().Optimize(lf.plan)
	if err != nil {
		return nil, err
	}
	cfg := resolveExec(opts)
	return executeMaybeStreaming(ctx, cfg, optimized)
}

// CollectUnoptimized bypasses the optimizer. Useful for testing that the
// plan is semantically correct before optimization.
func (lf LazyFrame) CollectUnoptimized(ctx context.Context, opts ...ExecOption) (*dataframe.DataFrame, error) {
	return Execute(ctx, lf.plan, opts...)
}

// Explain returns a three-section report: logical plan, optimizer pass log,
// optimized plan. The plans are rendered with the indented form used by
// polars' .explain(). For the box-drawn tree form, use ExplainTree.
func (lf LazyFrame) Explain() (string, error) { return ExplainFull(lf.plan) }

// ExplainString is Explain but panics on error, suitable for printing in
// examples and tests.
func (lf LazyFrame) ExplainString() string {
	out, err := lf.Explain()
	if err != nil {
		return "explain error: " + err.Error()
	}
	return out
}

// ExplainTree is Explain rendered as a box-drawing tree. Each child
// sits under its parent with ├── / └── connectors so nesting is
// easier to follow at a glance.
func (lf LazyFrame) ExplainTree() (string, error) { return ExplainTreeFull(lf.plan) }

// ExplainTreeString is ExplainTree but returns the error text inline
// rather than as a second value. Intended for printing from examples.
func (lf LazyFrame) ExplainTreeString() string {
	out, err := lf.ExplainTree()
	if err != nil {
		return "explain error: " + err.Error()
	}
	return out
}
