package eval

import (
	"context"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalBinaryLiteralFast routes binary nodes with one scalar literal
// side to the matching *Lit kernel in compute. Returns took=true when
// the branch was taken (result or err is valid), took=false to fall
// through to the generic evalBinary path.
//
// Matching patterns:
//
//	col OP lit   -> kernel OP direct
//	lit OP col   -> flipped kernel (e.g. 5 < col == col > 5)
func evalBinaryLiteralFast(
	ctx context.Context, ec EvalContext, n expr.BinaryNode, df *dataframe.DataFrame,
) (*series.Series, bool, error) {
	leftLit, rightLit, which := isLiteralBinary(n)
	if which == litNone {
		return nil, false, nil
	}
	var colExpr expr.Expr
	var lit expr.LitNode
	var op compareOp
	if which == litRight {
		colExpr = n.Left
		lit = rightLit
		op = toCompareOp(n.Op, false)
	} else {
		colExpr = n.Right
		lit = leftLit
		op = toCompareOp(n.Op, true) // flip sense
	}
	// Only comparison operators have Lit fast paths.
	if op == opInvalid {
		return nil, false, nil
	}
	colSer, err := evalNode(ctx, ec, colExpr, df)
	if err != nil {
		return nil, true, err
	}
	defer colSer.Release()
	var out *series.Series
	opts := kernelOpts(ec)
	switch op {
	case opGt:
		out, err = compute.GtLit(ctx, colSer, lit.Value, opts...)
	case opGe:
		out, err = compute.GeLit(ctx, colSer, lit.Value, opts...)
	case opLt:
		out, err = compute.LtLit(ctx, colSer, lit.Value, opts...)
	case opLe:
		out, err = compute.LeLit(ctx, colSer, lit.Value, opts...)
	case opEq:
		out, err = compute.EqLit(ctx, colSer, lit.Value, opts...)
	case opNe:
		out, err = compute.NeLit(ctx, colSer, lit.Value, opts...)
	case opAdd:
		out, err = compute.AddLit(ctx, colSer, lit.Value, opts...)
	case opSub:
		// `lit - col` is not commutative; only route `col - lit`
		// through SubLit. Flip was handled by toCompareOp for
		// comparisons, but arithmetic requires the original sense.
		if which != litRight {
			return nil, false, nil
		}
		out, err = compute.SubLit(ctx, colSer, lit.Value, opts...)
	case opMul:
		out, err = compute.MulLit(ctx, colSer, lit.Value, opts...)
	case opDiv:
		if which != litRight {
			return nil, false, nil
		}
		out, err = compute.DivLit(ctx, colSer, lit.Value, opts...)
	default:
		return nil, false, nil
	}
	return out, true, err
}

// isLiteralBinary returns the LitNode (if any) on either side.
func isLiteralBinary(n expr.BinaryNode) (expr.LitNode, expr.LitNode, litSide) {
	leftLit, lok := n.Left.Node().(expr.LitNode)
	rightLit, rok := n.Right.Node().(expr.LitNode)
	switch {
	case lok && rok:
		// Both literals: let the generic path fold - literal-literal
		// comparisons are rare and the planner can CSE them.
		return leftLit, rightLit, litNone
	case rok:
		return expr.LitNode{}, rightLit, litRight
	case lok:
		return leftLit, expr.LitNode{}, litLeft
	}
	return expr.LitNode{}, expr.LitNode{}, litNone
}

type litSide int

const (
	litNone litSide = iota
	litLeft
	litRight
)

type compareOp int

const (
	opInvalid compareOp = iota
	opGt
	opGe
	opLt
	opLe
	opEq
	opNe
	opAdd
	opSub
	opMul
	opDiv
)

// toCompareOp maps an expr.BinaryOp to the matching compareOp,
// optionally flipping the sense (for `lit OP col` the operator
// relative to the column is the mirror of OP).
func toCompareOp(op expr.BinaryOp, flip bool) compareOp {
	if flip {
		switch op {
		case expr.OpGt:
			return opLt
		case expr.OpGe:
			return opLe
		case expr.OpLt:
			return opGt
		case expr.OpLe:
			return opGe
		case expr.OpEq:
			return opEq
		case expr.OpNe:
			return opNe
		case expr.OpAdd:
			return opAdd // commutative
		case expr.OpMul:
			return opMul // commutative
			// opSub / opDiv aren't symmetric; caller checks litRight.
		}
		return opInvalid
	}
	switch op {
	case expr.OpGt:
		return opGt
	case expr.OpGe:
		return opGe
	case expr.OpLt:
		return opLt
	case expr.OpLe:
		return opLe
	case expr.OpEq:
		return opEq
	case expr.OpNe:
		return opNe
	case expr.OpAdd:
		return opAdd
	case expr.OpSub:
		return opSub
	case expr.OpMul:
		return opMul
	case expr.OpDiv:
		return opDiv
	}
	return opInvalid
}
