package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalListFunction dispatches FunctionNodes whose Name starts with
// "list." to the matching series.ListOps kernel. Params are
// positional; Get takes int, Contains takes any, Join takes string.
func evalListFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	arg0, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer arg0.Release()
	lst := arg0.List()

	switch n.Name {
	case "list.len":
		return lst.Len()
	case "list.sum":
		return lst.Sum()
	case "list.mean":
		return lst.Mean()
	case "list.min":
		return lst.Min()
	case "list.max":
		return lst.Max()
	case "list.first":
		return lst.First()
	case "list.last":
		return lst.Last()
	case "list.get":
		if len(n.Params) < 1 {
			return nil, fmt.Errorf("eval: list.get requires an index")
		}
		idx, err := intParam(n.Params[0])
		if err != nil {
			return nil, fmt.Errorf("eval: list.get: %w", err)
		}
		return lst.Get(idx)
	case "list.contains":
		if len(n.Params) < 1 {
			return nil, fmt.Errorf("eval: list.contains requires a needle")
		}
		return lst.Contains(n.Params[0])
	case "list.join":
		if len(n.Params) < 1 {
			return nil, fmt.Errorf("eval: list.join requires a separator")
		}
		sep, ok := n.Params[0].(string)
		if !ok {
			return nil, fmt.Errorf("eval: list.join separator must be string, got %T", n.Params[0])
		}
		return lst.Join(sep)
	}
	return nil, fmt.Errorf("eval: unknown list op %q", n.Name)
}

// intParam coerces a positional Param to int. Lit's switch accepts
// int / int32 / int64; accept the same here.
func intParam(p any) (int, error) {
	switch v := p.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	}
	return 0, fmt.Errorf("expected integer, got %T", p)
}
