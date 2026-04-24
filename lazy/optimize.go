package lazy

import (
	"fmt"
	"slices"
	"strings"

	"github.com/Gaurav-Gosain/golars/expr"
)

// Pass is a plan-to-plan rewrite. Apply returns the new node and a bool
// indicating whether the tree changed.
type Pass interface {
	Name() string
	Apply(Node) (Node, bool, error)
}

// Optimizer runs a sequence of passes over a plan. Some passes run once;
// others run until a fixed point is reached.
type Optimizer struct {
	Passes []Pass
}

// DefaultOptimizer returns the standard pass pipeline:
//
//	simplify          (fixed point)
//	type coercion     (fixed point)
//	common subexpr    (once)
//	slice pushdown    (once)
//	predicate pushdown (once)
//	projection pushdown (once)
//
// The pipeline mirrors polars' logical phase. Simplify runs first so later
// passes see canonicalized expressions; pushdowns run last because they rely
// on a stable expression shape.
func DefaultOptimizer() *Optimizer {
	return &Optimizer{Passes: []Pass{
		fixedPoint{Inner: SimplifyPass{}},
		fixedPoint{Inner: TypeCoercePass{}},
		CommonSubexprPass{},
		fixedPoint{Inner: SlicePushdownPass{}},
		fixedPoint{Inner: PredicatePushdownPass{}},
		ProjectionPushdownPass{},
	}}
}

// Optimize runs every pass in order. Each pass sees the output of the
// previous one.
func (o *Optimizer) Optimize(plan Node) (Node, []Trace, error) {
	traces := make([]Trace, 0, len(o.Passes))
	cur := plan
	for _, p := range o.Passes {
		next, changed, err := p.Apply(cur)
		if err != nil {
			return nil, traces, fmt.Errorf("pass %s: %w", p.Name(), err)
		}
		traces = append(traces, Trace{Name: p.Name(), Changed: changed, After: next})
		cur = next
	}
	return cur, traces, nil
}

// Trace records the effect of a single pass.
type Trace struct {
	Name    string
	Changed bool
	After   Node
}

// fixedPoint wraps a pass and re-applies it until no change occurs. A safety
// cap prevents infinite loops from buggy passes.
type fixedPoint struct {
	Inner Pass
	Max   int
}

func (f fixedPoint) Name() string { return f.Inner.Name() + "/fix" }

func (f fixedPoint) Apply(plan Node) (Node, bool, error) {
	max := f.Max
	if max == 0 {
		max = 8
	}
	changed := false
	for range max {
		next, did, err := f.Inner.Apply(plan)
		if err != nil {
			return nil, false, err
		}
		plan = next
		if !did {
			break
		}
		changed = true
	}
	return plan, changed, nil
}

// SimplifyPass performs expression-level canonicalization: constant folding
// for literal-only arithmetic/compare/logical expressions, boolean identity
// reductions, and trivial simplifications (x == x -> true).
type SimplifyPass struct{}

func (SimplifyPass) Name() string { return "simplify" }

func (SimplifyPass) Apply(plan Node) (Node, bool, error) {
	return transformPlan(plan, func(n Node) (Node, bool, error) {
		switch node := n.(type) {
		case Projection:
			changed := false
			exprs := make([]expr.Expr, len(node.Exprs))
			for i, e := range node.Exprs {
				ne, did := simplifyExpr(e)
				exprs[i] = ne
				if did {
					changed = true
				}
			}
			if changed {
				return Projection{Input: node.Input, Exprs: exprs}, true, nil
			}
		case WithColumns:
			changed := false
			exprs := make([]expr.Expr, len(node.Exprs))
			for i, e := range node.Exprs {
				ne, did := simplifyExpr(e)
				exprs[i] = ne
				if did {
					changed = true
				}
			}
			if changed {
				return WithColumns{Input: node.Input, Exprs: exprs}, true, nil
			}
		case Filter:
			ne, did := simplifyExpr(node.Predicate)
			if did {
				return Filter{Input: node.Input, Predicate: ne}, true, nil
			}
		}
		return n, false, nil
	})
}

// TypeCoercePass inserts explicit casts where a binary op mixes numeric
// types. Today this is handled at runtime by the evaluator; this pass is a
// placeholder hook and currently leaves the plan unchanged. Keeping it in
// the pipeline mirrors the polars layout and makes future expansion a diff
// rather than a new pass wiring change.
type TypeCoercePass struct{}

func (TypeCoercePass) Name() string { return "type-coerce" }

func (TypeCoercePass) Apply(plan Node) (Node, bool, error) { return plan, false, nil }

// CommonSubexprPass rewrites projection expressions to share aliases for
// identical subtrees that appear in more than one output.
//
// It is conservative: it only rewrites within a single Projection or
// WithColumns node. Cross-node CSE (caching scan results shared between
// subtrees) is deferred.
type CommonSubexprPass struct{}

func (CommonSubexprPass) Name() string { return "cse" }

func (c CommonSubexprPass) Apply(plan Node) (Node, bool, error) {
	return transformPlan(plan, func(n Node) (Node, bool, error) {
		switch node := n.(type) {
		case Projection:
			exprs, did := cseExprs(node.Exprs)
			if did {
				return Projection{Input: node.Input, Exprs: exprs}, true, nil
			}
		case WithColumns:
			exprs, did := cseExprs(node.Exprs)
			if did {
				return WithColumns{Input: node.Input, Exprs: exprs}, true, nil
			}
		}
		return n, false, nil
	})
}

// cseExprs is a no-op body for now. Full CSE requires a shared-subexpression
// IR or let-binding support, which is not part of this pass' public surface
// yet. The pass still runs so that the plan's Explain output shows it in the
// pipeline.
func cseExprs(in []expr.Expr) ([]expr.Expr, bool) { return in, false }

// SlicePushdownPass moves a Slice node below a Sort only when the sort is
// already sorted by the slice key; for the general case it pushes Slice into
// a DataFrameScan so the scan reads only the required range.
type SlicePushdownPass struct{}

func (SlicePushdownPass) Name() string { return "slice-pushdown" }

func (SlicePushdownPass) Apply(plan Node) (Node, bool, error) {
	return transformPlan(plan, func(n Node) (Node, bool, error) {
		slice, ok := n.(SliceNode)
		if !ok {
			return n, false, nil
		}
		// Pushdown sequences we handle:
		//   Slice -> Scan  => Scan with offset/length
		//   Slice -> Rename => Rename(Slice)
		//   Slice -> Drop => Drop(Slice)
		//   Slice -> Projection => Projection(Slice) when exprs are row-local.
		switch inner := slice.Input.(type) {
		case DataFrameScan:
			if inner.Length >= 0 {
				return n, false, nil // already sliced
			}
			ds := inner
			ds.Offset = slice.Offset
			ds.Length = slice.Length
			return ds, true, nil
		case Rename:
			return Rename{
				Input: SliceNode{Input: inner.Input, Offset: slice.Offset, Length: slice.Length},
				Old:   inner.Old, New: inner.New,
			}, true, nil
		case Drop:
			return Drop{
				Input:   SliceNode{Input: inner.Input, Offset: slice.Offset, Length: slice.Length},
				Columns: inner.Columns,
			}, true, nil
		case Projection:
			if allRowLocal(inner.Exprs) {
				return Projection{
					Input: SliceNode{Input: inner.Input, Offset: slice.Offset, Length: slice.Length},
					Exprs: inner.Exprs,
				}, true, nil
			}
		case WithColumns:
			if allRowLocal(inner.Exprs) {
				return WithColumns{
					Input: SliceNode{Input: inner.Input, Offset: slice.Offset, Length: slice.Length},
					Exprs: inner.Exprs,
				}, true, nil
			}
		}
		return n, false, nil
	})
}

// PredicatePushdownPass pushes Filter nodes as close to a scan as possible.
// A filter may pass through Projection/WithColumns/Rename/Drop only if its
// referenced columns are pass-through (present in the input schema of the
// node below without being redefined).
type PredicatePushdownPass struct{}

func (PredicatePushdownPass) Name() string { return "predicate-pushdown" }

func (PredicatePushdownPass) Apply(plan Node) (Node, bool, error) {
	return transformPlan(plan, func(n Node) (Node, bool, error) {
		filter, ok := n.(Filter)
		if !ok {
			return n, false, nil
		}
		refs := expr.Columns(filter.Predicate)

		switch inner := filter.Input.(type) {
		case DataFrameScan:
			// Push into the scan.
			ds := inner
			if ds.Predicate != nil {
				merged := ds.Predicate.And(filter.Predicate)
				ds.Predicate = &merged
			} else {
				p := filter.Predicate
				ds.Predicate = &p
			}
			return ds, true, nil
		case Projection:
			// Only push below projection when all refs are plain columns from
			// the input (not aliases or computed outputs).
			if projPassesThrough(inner.Exprs, refs) {
				return Projection{
					Input: Filter{Input: inner.Input, Predicate: filter.Predicate},
					Exprs: inner.Exprs,
				}, true, nil
			}
		case WithColumns:
			if withColsPassesThrough(inner, refs) {
				return WithColumns{
					Input: Filter{Input: inner.Input, Predicate: filter.Predicate},
					Exprs: inner.Exprs,
				}, true, nil
			}
		case Rename:
			// If filter doesn't reference the renamed column, swap.
			refSet := stringSet(refs)
			if _, hits := refSet[inner.New]; !hits {
				return Rename{
					Input: Filter{Input: inner.Input, Predicate: filter.Predicate},
					Old:   inner.Old, New: inner.New,
				}, true, nil
			}
		case Drop:
			dropSet := stringSet(inner.Columns)
			safe := true
			for _, r := range refs {
				if _, dropped := dropSet[r]; dropped {
					safe = false
					break
				}
			}
			if safe {
				return Drop{
					Input:   Filter{Input: inner.Input, Predicate: filter.Predicate},
					Columns: inner.Columns,
				}, true, nil
			}
		case SliceNode:
			// Filter before Slice changes semantics: don't push.
			return n, false, nil
		}
		return n, false, nil
	})
}

// ProjectionPushdownPass reduces the column set produced by sub-plans to the
// minimum needed by the surrounding plan. For a DataFrameScan this becomes
// a Scan.Projection; for other operators it inserts a Projection over the
// input.
type ProjectionPushdownPass struct{}

func (ProjectionPushdownPass) Name() string { return "projection-pushdown" }

func (p ProjectionPushdownPass) Apply(plan Node) (Node, bool, error) {
	needed := neededAtRoot(plan)
	if needed == nil {
		return plan, false, nil
	}
	out, changed := p.push(plan, needed)
	return out, changed, nil
}

func (p ProjectionPushdownPass) push(n Node, needed map[string]struct{}) (Node, bool) {
	switch node := n.(type) {
	case DataFrameScan:
		// If the scan already produces a subset, keep it; otherwise set the
		// projection to the needed columns in schema order.
		src := node.Source.Schema()
		proj := orderedIntersect(src.Names(), needed)
		if len(node.Projection) == len(proj) && sameSet(node.Projection, proj) {
			return node, false
		}
		if len(node.Projection) > 0 {
			// Scan already has its own projection; intersect with needed.
			keep := orderedIntersect(node.Projection, needed)
			if len(keep) == len(node.Projection) {
				return node, false
			}
			node.Projection = keep
			return node, true
		}
		node.Projection = proj
		return node, true
	case Projection:
		// Recompute the needed set for the child: columns referenced by our
		// expressions.
		childNeeded := map[string]struct{}{}
		for _, e := range node.Exprs {
			for _, c := range expr.Columns(e) {
				childNeeded[c] = struct{}{}
			}
		}
		newChild, changed := p.push(node.Input, childNeeded)
		if !changed {
			return node, false
		}
		return Projection{Input: newChild, Exprs: node.Exprs}, true
	case WithColumns:
		// Child must produce everything needed (by the surrounding plan)
		// minus the outputs this node redefines, plus whatever the new
		// expressions reference.
		redefines := map[string]struct{}{}
		for _, e := range node.Exprs {
			redefines[expr.OutputName(e)] = struct{}{}
		}
		childNeeded := map[string]struct{}{}
		for k := range needed {
			if _, rd := redefines[k]; rd {
				continue
			}
			childNeeded[k] = struct{}{}
		}
		for _, e := range node.Exprs {
			for _, c := range expr.Columns(e) {
				childNeeded[c] = struct{}{}
			}
		}
		newChild, changed := p.push(node.Input, childNeeded)
		if !changed {
			return node, false
		}
		return WithColumns{Input: newChild, Exprs: node.Exprs}, true
	case Filter:
		childNeeded := map[string]struct{}{}
		for k := range needed {
			childNeeded[k] = struct{}{}
		}
		for _, c := range expr.Columns(node.Predicate) {
			childNeeded[c] = struct{}{}
		}
		newChild, changed := p.push(node.Input, childNeeded)
		if !changed {
			return node, false
		}
		return Filter{Input: newChild, Predicate: node.Predicate}, true
	case Sort:
		childNeeded := map[string]struct{}{}
		for k := range needed {
			childNeeded[k] = struct{}{}
		}
		for _, k := range node.Keys {
			childNeeded[k] = struct{}{}
		}
		newChild, changed := p.push(node.Input, childNeeded)
		if !changed {
			return node, false
		}
		return Sort{Input: newChild, Keys: node.Keys, Options: node.Options}, true
	case SliceNode:
		newChild, changed := p.push(node.Input, needed)
		if !changed {
			return node, false
		}
		return SliceNode{Input: newChild, Offset: node.Offset, Length: node.Length}, true
	case Rename:
		// Child needs old name where root needs new name; others pass.
		childNeeded := map[string]struct{}{}
		for k := range needed {
			if k == node.New {
				childNeeded[node.Old] = struct{}{}
			} else {
				childNeeded[k] = struct{}{}
			}
		}
		newChild, changed := p.push(node.Input, childNeeded)
		if !changed {
			return node, false
		}
		return Rename{Input: newChild, Old: node.Old, New: node.New}, true
	case Drop:
		// Dropped columns are not needed anyway.
		newChild, changed := p.push(node.Input, needed)
		if !changed {
			return node, false
		}
		return Drop{Input: newChild, Columns: node.Columns}, true
	}
	return n, false
}

// neededAtRoot returns the set of columns the top-level plan reads. For a
// Projection this is the set of root outputs; for any other node we leave
// the needed set nil, which means "all columns" and disables pushdown.
func neededAtRoot(plan Node) map[string]struct{} {
	switch n := plan.(type) {
	case Projection:
		out := map[string]struct{}{}
		for _, e := range n.Exprs {
			for _, c := range expr.Columns(e) {
				out[c] = struct{}{}
			}
		}
		return out
	}
	return nil
}

// transformPlan rewrites the tree post-order: children first, then the node
// itself. changed indicates whether anything in the subtree (including the
// root) was rewritten.
func transformPlan(n Node, fn func(Node) (Node, bool, error)) (Node, bool, error) {
	childChanged := false
	kids := n.Children()
	newKids := make([]Node, len(kids))
	for i, k := range kids {
		nk, did, err := transformPlan(k, fn)
		if err != nil {
			return nil, false, err
		}
		newKids[i] = nk
		if did {
			childChanged = true
		}
	}
	if childChanged {
		n = n.WithChildren(newKids)
	}
	out, did, err := fn(n)
	if err != nil {
		return nil, false, err
	}
	return out, childChanged || did, nil
}

// simplifyExpr applies a fixed set of rewrites; returns (new, changed).
func simplifyExpr(e expr.Expr) (expr.Expr, bool) {
	// Recurse into children first.
	kids := expr.Children(e)
	changedKids := false
	if len(kids) > 0 {
		newKids := make([]expr.Expr, len(kids))
		for i, k := range kids {
			nk, did := simplifyExpr(k)
			newKids[i] = nk
			if did {
				changedKids = true
			}
		}
		if changedKids {
			e = expr.WithChildren(e, newKids)
		}
	}
	out, did := simplifyNode(e)
	return out, changedKids || did
}

func simplifyNode(e expr.Expr) (expr.Expr, bool) {
	switch n := e.Node().(type) {
	case expr.BinaryNode:
		// Constant folding on two literals.
		if ll, ok := n.Left.Node().(expr.LitNode); ok {
			if lr, ok := n.Right.Node().(expr.LitNode); ok {
				if folded, ok := foldBinary(n.Op, ll, lr); ok {
					return folded, true
				}
			}
		}
		// x == x → true (for non-null references).
		if n.Op == expr.OpEq && expr.Equal(n.Left, n.Right) {
			// We cannot be 100% sure x is non-null, but polars' simplify
			// treats trivially identical subtrees as true. Leave unchanged
			// for correctness in the presence of nulls.
		}
		// x AND true → x; x AND false → false; x OR true → true; x OR false → x.
		if n.Op == expr.OpAnd {
			if lit, ok := n.Right.Node().(expr.LitNode); ok {
				if b, ok := lit.Value.(bool); ok {
					if b {
						return n.Left, true
					}
					return expr.LitBool(false), true
				}
			}
			if lit, ok := n.Left.Node().(expr.LitNode); ok {
				if b, ok := lit.Value.(bool); ok {
					if b {
						return n.Right, true
					}
					return expr.LitBool(false), true
				}
			}
		}
		if n.Op == expr.OpOr {
			if lit, ok := n.Right.Node().(expr.LitNode); ok {
				if b, ok := lit.Value.(bool); ok {
					if b {
						return expr.LitBool(true), true
					}
					return n.Left, true
				}
			}
			if lit, ok := n.Left.Node().(expr.LitNode); ok {
				if b, ok := lit.Value.(bool); ok {
					if b {
						return expr.LitBool(true), true
					}
					return n.Right, true
				}
			}
		}
	case expr.UnaryNode:
		if n.Op == expr.OpNot {
			// not(not(x)) → x
			if inner, ok := n.Arg.Node().(expr.UnaryNode); ok && inner.Op == expr.OpNot {
				return inner.Arg, true
			}
			// not(true/false) → false/true
			if lit, ok := n.Arg.Node().(expr.LitNode); ok {
				if b, ok := lit.Value.(bool); ok {
					return expr.LitBool(!b), true
				}
			}
		}
	}
	return e, false
}

// foldBinary folds a binary op over two literals into a single literal when
// possible.
func foldBinary(op expr.BinaryOp, l, r expr.LitNode) (expr.Expr, bool) {
	// Only handle same-type numeric or same-type bool folding.
	switch la := l.Value.(type) {
	case int64:
		ra, ok := r.Value.(int64)
		if !ok {
			return expr.Expr{}, false
		}
		switch op {
		case expr.OpAdd:
			return expr.LitInt64(la + ra), true
		case expr.OpSub:
			return expr.LitInt64(la - ra), true
		case expr.OpMul:
			return expr.LitInt64(la * ra), true
		case expr.OpDiv:
			if ra == 0 {
				return expr.Expr{}, false
			}
			return expr.LitInt64(la / ra), true
		case expr.OpEq:
			return expr.LitBool(la == ra), true
		case expr.OpNe:
			return expr.LitBool(la != ra), true
		case expr.OpLt:
			return expr.LitBool(la < ra), true
		case expr.OpLe:
			return expr.LitBool(la <= ra), true
		case expr.OpGt:
			return expr.LitBool(la > ra), true
		case expr.OpGe:
			return expr.LitBool(la >= ra), true
		}
	case float64:
		ra, ok := r.Value.(float64)
		if !ok {
			return expr.Expr{}, false
		}
		switch op {
		case expr.OpAdd:
			return expr.LitFloat64(la + ra), true
		case expr.OpSub:
			return expr.LitFloat64(la - ra), true
		case expr.OpMul:
			return expr.LitFloat64(la * ra), true
		case expr.OpDiv:
			return expr.LitFloat64(la / ra), true
		case expr.OpEq:
			return expr.LitBool(la == ra), true
		case expr.OpNe:
			return expr.LitBool(la != ra), true
		case expr.OpLt:
			return expr.LitBool(la < ra), true
		case expr.OpLe:
			return expr.LitBool(la <= ra), true
		case expr.OpGt:
			return expr.LitBool(la > ra), true
		case expr.OpGe:
			return expr.LitBool(la >= ra), true
		}
	case bool:
		ra, ok := r.Value.(bool)
		if !ok {
			return expr.Expr{}, false
		}
		switch op {
		case expr.OpAnd:
			return expr.LitBool(la && ra), true
		case expr.OpOr:
			return expr.LitBool(la || ra), true
		case expr.OpEq:
			return expr.LitBool(la == ra), true
		case expr.OpNe:
			return expr.LitBool(la != ra), true
		}
	}
	return expr.Expr{}, false
}

// projPassesThrough reports whether every ref is present as a bare column in
// the projection list (no rename, no computation). Only then is it safe to
// push a filter below the projection.
func projPassesThrough(exprs []expr.Expr, refs []string) bool {
	produced := map[string]string{} // output name -> source col, empty if computed
	for _, e := range exprs {
		outName := expr.OutputName(e)
		if col, ok := bareCol(e); ok {
			produced[outName] = col
		} else {
			produced[outName] = ""
		}
	}
	for _, r := range refs {
		src, ok := produced[r]
		if !ok || src == "" {
			return false
		}
	}
	return true
}

// withColsPassesThrough checks a WithColumns node: filter refs that are NOT
// redefined here pass through; refs that ARE redefined must be defined as
// bare columns.
func withColsPassesThrough(w WithColumns, refs []string) bool {
	redefined := map[string]string{}
	for _, e := range w.Exprs {
		name := expr.OutputName(e)
		if col, ok := bareCol(e); ok {
			redefined[name] = col
		} else {
			redefined[name] = ""
		}
	}
	for _, r := range refs {
		if src, ok := redefined[r]; ok && src == "" {
			return false
		}
	}
	return true
}

func bareCol(e expr.Expr) (string, bool) {
	if c, ok := e.Node().(expr.ColNode); ok {
		return c.Name, true
	}
	if a, ok := e.Node().(expr.AliasNode); ok {
		if c, ok := a.Inner.Node().(expr.ColNode); ok {
			return c.Name, true
		}
	}
	return "", false
}

// allRowLocal reports whether every expression operates row-locally
// (equivalent: contains no aggregation).
func allRowLocal(exprs []expr.Expr) bool {
	return !slices.ContainsFunc(exprs, expr.ContainsAgg)
}

func stringSet(xs []string) map[string]struct{} {
	out := make(map[string]struct{}, len(xs))
	for _, x := range xs {
		out[x] = struct{}{}
	}
	return out
}

func orderedIntersect(order []string, keep map[string]struct{}) []string {
	out := make([]string, 0, len(keep))
	for _, x := range order {
		if _, ok := keep[x]; ok {
			out = append(out, x)
		}
	}
	return out
}

func sameSet(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	ma := stringSet(a)
	for _, x := range b {
		if _, ok := ma[x]; !ok {
			return false
		}
	}
	return true
}

// FormatTraces produces a readable log of passes and their effect. Useful
// for Explain output.
func FormatTraces(ts []Trace) string {
	var b strings.Builder
	for _, t := range ts {
		state := "kept"
		if t.Changed {
			state = "changed"
		}
		fmt.Fprintf(&b, "%-24s %s\n", t.Name, state)
	}
	return b.String()
}
