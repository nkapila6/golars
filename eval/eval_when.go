package eval

import (
	"context"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalWhenThen materialises when(pred).then(a).otherwise(b) by
// evaluating every branch then delegating to compute.Where.
func evalWhenThen(ctx context.Context, ec EvalContext, n expr.WhenThenNode, df *dataframe.DataFrame) (*series.Series, error) {
	pred, err := evalNode(ctx, ec, n.Pred, df)
	if err != nil {
		return nil, err
	}
	defer pred.Release()
	ifTrue, err := evalNode(ctx, ec, n.Then, df)
	if err != nil {
		return nil, err
	}
	defer ifTrue.Release()
	ifFalse, err := evalNode(ctx, ec, n.Otherwise, df)
	if err != nil {
		return nil, err
	}
	defer ifFalse.Release()
	// Promote branches to a common dtype (e.g. int then + float
	// otherwise becomes float). Reuse the same helper the binary
	// evaluator uses.
	lhs, rhs, err := promoteBinary(ctx, ec, ifTrue, ifFalse)
	if err != nil {
		return nil, err
	}
	if lhs != ifTrue {
		defer lhs.Release()
	}
	if rhs != ifFalse {
		defer rhs.Release()
	}
	return compute.Where(ctx, pred, lhs, rhs, kernelOpts(ec)...)
}
