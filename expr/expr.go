// Package expr defines the golars expression AST.
//
// Expressions are immutable values. Users build them through package-level
// constructors and fluent methods:
//
//	expr.Col("price").Mul(expr.Col("qty")).Alias("revenue")
//	expr.Col("x").GtLit(int64(10)).And(expr.Col("y").IsNotNull())
//
// Expressions describe a computation on a DataFrame. They are executed either
// eagerly through Eval or indirectly through the lazy planner. The same AST
// drives both paths.
package expr

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// BinaryOp is the set of binary operators the AST carries.
type BinaryOp uint8

const (
	OpAdd BinaryOp = iota
	OpSub
	OpMul
	OpDiv
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAnd
	OpOr
)

func (o BinaryOp) symbol() string {
	switch o {
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpEq:
		return "=="
	case OpNe:
		return "!="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	case OpAnd:
		return "and"
	case OpOr:
		return "or"
	}
	return "?"
}

// UnaryOp is the set of unary operators the AST carries.
type UnaryOp uint8

const (
	OpNot UnaryOp = iota
	OpNeg
)

func (o UnaryOp) symbol() string {
	switch o {
	case OpNot:
		return "not"
	case OpNeg:
		return "-"
	}
	return "?"
}

// AggOp names an aggregation.
type AggOp uint8

const (
	AggSum AggOp = iota
	AggMin
	AggMax
	AggMean
	AggCount
	AggNullCount
	AggFirst
	AggLast
)

// String returns the polars-style short name for the aggregation.
func (o AggOp) String() string {
	return o.symbol()
}

func (o AggOp) symbol() string {
	switch o {
	case AggSum:
		return "sum"
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	case AggMean:
		return "mean"
	case AggCount:
		return "count"
	case AggNullCount:
		return "null_count"
	case AggFirst:
		return "first"
	case AggLast:
		return "last"
	}
	return "?"
}

// Node is the tag interface for AST nodes. Users do not construct Nodes
// directly; they work with Expr values returned by constructors.
type Node interface {
	isNode()
	fmt.Stringer
}

// Expr is the public expression value. It wraps an internal Node and carries
// fluent builder methods.
type Expr struct{ node Node }

// Node returns the underlying AST node for pattern matching by internal
// consumers (the evaluator and the optimizer).
func (e Expr) Node() Node { return e.node }

// String returns a polars-style repr: col("a"), col("a") + 1, col("a").sum().
func (e Expr) String() string {
	if e.node == nil {
		return "<nil>"
	}
	return e.node.String()
}

// Hash returns a stable 64-bit hash of the expression structure. Used for
// common subexpression elimination.
func (e Expr) Hash() uint64 { return hashExpr(e) }

// ColNode is a reference to a named column.
type ColNode struct{ Name string }

func (ColNode) isNode()          {}
func (c ColNode) String() string { return fmt.Sprintf("col(%q)", c.Name) }

// LitNode is a typed literal value.
type LitNode struct {
	DType dtype.DType
	// Value is a typed Go value. Supported types: int64, float64, bool,
	// string. Use the Lit* constructors to build a LitNode safely.
	Value any
}

func (LitNode) isNode() {}
func (l LitNode) String() string {
	switch v := l.Value.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
	return fmt.Sprintf("%v", l.Value)
}

// BinaryNode combines two sub-expressions with an operator.
type BinaryNode struct {
	Op    BinaryOp
	Left  Expr
	Right Expr
}

func (BinaryNode) isNode() {}
func (b BinaryNode) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, b.Op.symbol(), b.Right)
}

// UnaryNode applies a unary operator.
type UnaryNode struct {
	Op  UnaryOp
	Arg Expr
}

func (UnaryNode) isNode() {}
func (u UnaryNode) String() string {
	return fmt.Sprintf("(%s %s)", u.Op.symbol(), u.Arg)
}

// AliasNode attaches a name to an expression.
type AliasNode struct {
	Inner Expr
	Name  string
}

func (AliasNode) isNode() {}
func (a AliasNode) String() string {
	return fmt.Sprintf("%s.alias(%q)", a.Inner, a.Name)
}

// CastNode coerces an expression to a target dtype.
type CastNode struct {
	Inner Expr
	To    dtype.DType
}

func (CastNode) isNode() {}
func (c CastNode) String() string {
	return fmt.Sprintf("%s.cast(%s)", c.Inner, c.To)
}

// AggNode aggregates an expression to a single scalar or one value per group.
type AggNode struct {
	Op    AggOp
	Inner Expr
}

func (AggNode) isNode() {}
func (a AggNode) String() string {
	return fmt.Sprintf("%s.%s()", a.Inner, a.Op.symbol())
}

// IsNullNode tests for null, or (with Negate) non-null.
type IsNullNode struct {
	Inner  Expr
	Negate bool
}

func (IsNullNode) isNode() {}
func (n IsNullNode) String() string {
	if n.Negate {
		return fmt.Sprintf("%s.is_not_null()", n.Inner)
	}
	return fmt.Sprintf("%s.is_null()", n.Inner)
}

// WhenThenNode is a conditional: when(pred).then(a).otherwise(b).
type WhenThenNode struct {
	Pred      Expr
	Then      Expr
	Otherwise Expr
}

func (WhenThenNode) isNode() {}
func (w WhenThenNode) String() string {
	return fmt.Sprintf("when(%s).then(%s).otherwise(%s)", w.Pred, w.Then, w.Otherwise)
}

// Constructors ---------------------------------------------------------------

// Col returns a reference to the named column.
func Col(name string) Expr { return Expr{ColNode{Name: name}} }

// LitInt64 returns an int64 literal.
func LitInt64(v int64) Expr { return Expr{LitNode{DType: dtype.Int64(), Value: v}} }

// LitFloat64 returns a float64 literal.
func LitFloat64(v float64) Expr { return Expr{LitNode{DType: dtype.Float64(), Value: v}} }

// LitBool returns a bool literal.
func LitBool(v bool) Expr { return Expr{LitNode{DType: dtype.Bool(), Value: v}} }

// LitString returns a string literal.
func LitString(v string) Expr { return Expr{LitNode{DType: dtype.String(), Value: v}} }

// LitNull returns a typed null literal.
func LitNull(dt dtype.DType) Expr { return Expr{LitNode{DType: dt, Value: nil}} }

// Lit is a type-inferring convenience. It panics on unsupported types.
func Lit(v any) Expr {
	switch x := v.(type) {
	case int:
		return LitInt64(int64(x))
	case int32:
		return LitInt64(int64(x))
	case int64:
		return LitInt64(x)
	case float32:
		return LitFloat64(float64(x))
	case float64:
		return LitFloat64(x)
	case bool:
		return LitBool(x)
	case string:
		return LitString(x)
	case nil:
		return Expr{LitNode{DType: dtype.Null(), Value: nil}}
	}
	panic(fmt.Sprintf("expr.Lit: unsupported literal type %T", v))
}

// When starts a conditional expression. Follow with Then(...).Otherwise(...).
func When(pred Expr) WhenBuilder { return WhenBuilder{pred: pred} }

// WhenBuilder captures a predicate awaiting Then().
type WhenBuilder struct{ pred Expr }

// Then records the value taken when the predicate is true.
func (w WhenBuilder) Then(v Expr) WhenThenBuilder {
	return WhenThenBuilder{pred: w.pred, then: v}
}

// WhenThenBuilder captures pred+then awaiting Otherwise().
type WhenThenBuilder struct{ pred, then Expr }

// Otherwise closes the conditional and returns an Expr.
func (w WhenThenBuilder) Otherwise(v Expr) Expr {
	return Expr{WhenThenNode{Pred: w.pred, Then: w.then, Otherwise: v}}
}

// Fluent methods on Expr -----------------------------------------------------

// Add is arithmetic addition. Nulls propagate.
func (e Expr) Add(other Expr) Expr { return binary(OpAdd, e, other) }

// AddLit is sugar for e.Add(Lit(v)).
func (e Expr) AddLit(v any) Expr { return binary(OpAdd, e, Lit(v)) }

// Sub is arithmetic subtraction.
func (e Expr) Sub(other Expr) Expr { return binary(OpSub, e, other) }

// SubLit is sugar for e.Sub(Lit(v)).
func (e Expr) SubLit(v any) Expr { return binary(OpSub, e, Lit(v)) }

// Mul is arithmetic multiplication.
func (e Expr) Mul(other Expr) Expr { return binary(OpMul, e, other) }

// MulLit is sugar for e.Mul(Lit(v)).
func (e Expr) MulLit(v any) Expr { return binary(OpMul, e, Lit(v)) }

// Div is arithmetic division.
func (e Expr) Div(other Expr) Expr { return binary(OpDiv, e, other) }

// DivLit is sugar for e.Div(Lit(v)).
func (e Expr) DivLit(v any) Expr { return binary(OpDiv, e, Lit(v)) }

// Eq is the equality comparator.
func (e Expr) Eq(other Expr) Expr  { return binary(OpEq, e, other) }
func (e Expr) EqLit(v any) Expr    { return binary(OpEq, e, Lit(v)) }
func (e Expr) Ne(other Expr) Expr  { return binary(OpNe, e, other) }
func (e Expr) NeLit(v any) Expr    { return binary(OpNe, e, Lit(v)) }
func (e Expr) Lt(other Expr) Expr  { return binary(OpLt, e, other) }
func (e Expr) LtLit(v any) Expr    { return binary(OpLt, e, Lit(v)) }
func (e Expr) Le(other Expr) Expr  { return binary(OpLe, e, other) }
func (e Expr) LeLit(v any) Expr    { return binary(OpLe, e, Lit(v)) }
func (e Expr) Gt(other Expr) Expr  { return binary(OpGt, e, other) }
func (e Expr) GtLit(v any) Expr    { return binary(OpGt, e, Lit(v)) }
func (e Expr) Ge(other Expr) Expr  { return binary(OpGe, e, other) }
func (e Expr) GeLit(v any) Expr    { return binary(OpGe, e, Lit(v)) }
func (e Expr) And(other Expr) Expr { return binary(OpAnd, e, other) }
func (e Expr) Or(other Expr) Expr  { return binary(OpOr, e, other) }

// Not is the logical negation.
func (e Expr) Not() Expr { return Expr{UnaryNode{Op: OpNot, Arg: e}} }

// Neg is the arithmetic negation.
func (e Expr) Neg() Expr { return Expr{UnaryNode{Op: OpNeg, Arg: e}} }

// Alias renames the expression output.
func (e Expr) Alias(name string) Expr {
	// Collapse double alias: col("a").alias("b").alias("c") == col("a").alias("c")
	if a, ok := e.node.(AliasNode); ok {
		return Expr{AliasNode{Inner: a.Inner, Name: name}}
	}
	return Expr{AliasNode{Inner: e, Name: name}}
}

// Cast coerces the expression result to the target dtype.
func (e Expr) Cast(to dtype.DType) Expr {
	// Collapse redundant casts of the same dtype.
	if c, ok := e.node.(CastNode); ok && c.To.Equal(to) {
		return e
	}
	return Expr{CastNode{Inner: e, To: to}}
}

// IsNull returns a boolean expression that is true where the input is null.
func (e Expr) IsNull() Expr { return Expr{IsNullNode{Inner: e, Negate: false}} }

// IsNotNull returns a boolean expression that is true where the input is not null.
func (e Expr) IsNotNull() Expr { return Expr{IsNullNode{Inner: e, Negate: true}} }

// Sum aggregates the expression with sum.
func (e Expr) Sum() Expr { return Expr{AggNode{Op: AggSum, Inner: e}} }

// Min aggregates with min.
func (e Expr) Min() Expr { return Expr{AggNode{Op: AggMin, Inner: e}} }

// Max aggregates with max.
func (e Expr) Max() Expr { return Expr{AggNode{Op: AggMax, Inner: e}} }

// Mean aggregates with arithmetic mean.
func (e Expr) Mean() Expr { return Expr{AggNode{Op: AggMean, Inner: e}} }

// Count returns the number of non-null values.
func (e Expr) Count() Expr { return Expr{AggNode{Op: AggCount, Inner: e}} }

// NullCount returns the number of null values.
func (e Expr) NullCount() Expr { return Expr{AggNode{Op: AggNullCount, Inner: e}} }

// First returns the first value (aggregation).
func (e Expr) First() Expr { return Expr{AggNode{Op: AggFirst, Inner: e}} }

// Last returns the last value (aggregation).
func (e Expr) Last() Expr { return Expr{AggNode{Op: AggLast, Inner: e}} }

// Walk visits every sub-expression depth-first. Returning false from fn
// prunes the subtree.
func Walk(e Expr, fn func(Expr) bool) {
	if !fn(e) {
		return
	}
	for _, c := range Children(e) {
		Walk(c, fn)
	}
}

// Children returns the direct sub-expressions of e, in a stable order.
func Children(e Expr) []Expr {
	switch n := e.node.(type) {
	case BinaryNode:
		return []Expr{n.Left, n.Right}
	case UnaryNode:
		return []Expr{n.Arg}
	case AliasNode:
		return []Expr{n.Inner}
	case CastNode:
		return []Expr{n.Inner}
	case AggNode:
		return []Expr{n.Inner}
	case IsNullNode:
		return []Expr{n.Inner}
	case WhenThenNode:
		return []Expr{n.Pred, n.Then, n.Otherwise}
	case FunctionNode:
		return append([]Expr(nil), n.Args...)
	case OverNode:
		return []Expr{n.Inner}
	}
	return nil
}

// WithChildren constructs a new expression with the given children replacing
// the originals. Panics if len(children) does not match the expected arity of
// the node.
func WithChildren(e Expr, children []Expr) Expr {
	switch n := e.node.(type) {
	case ColNode, LitNode:
		if len(children) != 0 {
			panic(fmt.Sprintf("expr: %T has no children", n))
		}
		return e
	case BinaryNode:
		mustArity(n, children, 2)
		return Expr{BinaryNode{Op: n.Op, Left: children[0], Right: children[1]}}
	case UnaryNode:
		mustArity(n, children, 1)
		return Expr{UnaryNode{Op: n.Op, Arg: children[0]}}
	case AliasNode:
		mustArity(n, children, 1)
		return Expr{AliasNode{Inner: children[0], Name: n.Name}}
	case CastNode:
		mustArity(n, children, 1)
		return Expr{CastNode{Inner: children[0], To: n.To}}
	case AggNode:
		mustArity(n, children, 1)
		return Expr{AggNode{Op: n.Op, Inner: children[0]}}
	case IsNullNode:
		mustArity(n, children, 1)
		return Expr{IsNullNode{Inner: children[0], Negate: n.Negate}}
	case WhenThenNode:
		mustArity(n, children, 3)
		return Expr{WhenThenNode{Pred: children[0], Then: children[1], Otherwise: children[2]}}
	case FunctionNode:
		// Arity fixed by number of Args; Params unchanged.
		mustArity(n, children, len(n.Args))
		newArgs := append([]Expr(nil), children...)
		return Expr{FunctionNode{Name: n.Name, Args: newArgs, Params: n.Params}}
	case OverNode:
		mustArity(n, children, 1)
		return Expr{OverNode{Inner: children[0], Keys: n.Keys}}
	}
	panic(fmt.Sprintf("expr: WithChildren on unknown node %T", e.node))
}

// Columns returns the set of distinct column names referenced by e.
// OverNode partition keys count as references even though they don't
// appear as ColNode children.
func Columns(e Expr) []string {
	seen := map[string]struct{}{}
	var order []string
	add := func(name string) {
		if _, dup := seen[name]; dup {
			return
		}
		seen[name] = struct{}{}
		order = append(order, name)
	}
	Walk(e, func(x Expr) bool {
		switch n := x.node.(type) {
		case ColNode:
			add(n.Name)
		case OverNode:
			for _, k := range n.Keys {
				add(k)
			}
		}
		return true
	})
	return order
}

// OutputName returns the inferred name of the expression's output column.
// An Alias defines the name explicitly; otherwise the first referenced
// column name is used (mirroring polars).
func OutputName(e Expr) string {
	if e.node == nil {
		return ""
	}
	if a, ok := e.node.(AliasNode); ok {
		return a.Name
	}
	cols := Columns(e)
	if len(cols) > 0 {
		return cols[0]
	}
	// For literals, use the literal value as the default name.
	if l, ok := e.node.(LitNode); ok {
		return fmt.Sprintf("literal[%s]", l.DType)
	}
	return "expr"
}

// ContainsAgg reports whether e contains an AggNode anywhere in its subtree.
func ContainsAgg(e Expr) bool {
	found := false
	Walk(e, func(x Expr) bool {
		if _, ok := x.node.(AggNode); ok {
			found = true
			return false
		}
		return true
	})
	return found
}

// Equal reports whether two expressions are structurally identical.
func Equal(a, b Expr) bool {
	if a.node == nil || b.node == nil {
		return a.node == b.node
	}
	if a.String() != b.String() {
		// Quick short-circuit: different repr => different expressions.
		// (String is structural in this package.)
		return false
	}
	// Deep compare for safety.
	return a.Hash() == b.Hash()
}

func binary(op BinaryOp, l, r Expr) Expr {
	return Expr{BinaryNode{Op: op, Left: l, Right: r}}
}

func mustArity(n Node, children []Expr, want int) {
	if len(children) != want {
		panic(fmt.Sprintf("expr: %T expects %d children, got %d", n, want, len(children)))
	}
}

func hashExpr(e Expr) uint64 {
	h := fnv.New64a()
	writeExprHash(h, e)
	return h.Sum64()
}

func writeExprHash(h interface {
	Write(p []byte) (n int, err error)
}, e Expr) {
	if e.node == nil {
		h.Write([]byte("nil"))
		return
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%T|", e.node)
	switch n := e.node.(type) {
	case ColNode:
		sb.WriteString(n.Name)
	case LitNode:
		sb.WriteString(n.DType.String())
		sb.WriteString("=")
		sb.WriteString(n.String())
	case BinaryNode:
		sb.WriteByte(byte(n.Op))
	case UnaryNode:
		sb.WriteByte(byte(n.Op))
	case AliasNode:
		sb.WriteString(n.Name)
	case CastNode:
		sb.WriteString(n.To.String())
	case AggNode:
		sb.WriteByte(byte(n.Op))
	case IsNullNode:
		if n.Negate {
			sb.WriteByte('!')
		}
	case WhenThenNode:
		// no extra scalar fields
	}
	h.Write([]byte(sb.String()))
	for _, c := range Children(e) {
		h.Write([]byte{'['})
		writeExprHash(h, c)
		h.Write([]byte{']'})
	}
}
