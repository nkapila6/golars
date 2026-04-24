package lazy

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// --- FillNanNode -------------------------------------------------------
//
// Frame-level fill_nan: replace NaN with value in every float column.
// Integer/string columns pass through unchanged. Mirrors polars'
// LazyFrame.fill_nan(value).

// FillNanNode is the plan node for lf.FillNan(v).
type FillNanNode struct {
	Input Node
	Value float64
}

func (FillNanNode) isLogicalNode() {}

func (f FillNanNode) Children() []Node { return []Node{f.Input} }

func (f FillNanNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: FillNanNode takes one child")
	}
	return FillNanNode{Input: children[0], Value: f.Value}
}

func (f FillNanNode) Schema() (*schema.Schema, error) { return f.Input.Schema() }
func (f FillNanNode) String() string                  { return fmt.Sprintf("FILL_NAN value=%v", f.Value) }

// FillNan returns a LazyFrame that replaces NaN with v in every float
// column; non-float columns are passed through unchanged.
func (lf LazyFrame) FillNan(value float64) LazyFrame {
	return LazyFrame{plan: FillNanNode{Input: lf.plan, Value: value}}
}

// --- ForwardFillNode ---------------------------------------------------

// ForwardFillNode forward-fills every column; limit caps the number of
// consecutive fills (0 = unlimited). Mirrors polars' lf.fill_null(
// strategy="forward", limit=...).
type ForwardFillNode struct {
	Input Node
	Limit int
}

func (ForwardFillNode) isLogicalNode() {}

func (f ForwardFillNode) Children() []Node { return []Node{f.Input} }

func (f ForwardFillNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: ForwardFillNode takes one child")
	}
	return ForwardFillNode{Input: children[0], Limit: f.Limit}
}

func (f ForwardFillNode) Schema() (*schema.Schema, error) { return f.Input.Schema() }
func (f ForwardFillNode) String() string                  { return fmt.Sprintf("FORWARD_FILL limit=%d", f.Limit) }

// ForwardFill returns a LazyFrame that forward-fills every column.
func (lf LazyFrame) ForwardFill(limit int) LazyFrame {
	return LazyFrame{plan: ForwardFillNode{Input: lf.plan, Limit: limit}}
}

// --- BackwardFillNode --------------------------------------------------

// BackwardFillNode is ForwardFillNode in reverse.
type BackwardFillNode struct {
	Input Node
	Limit int
}

func (BackwardFillNode) isLogicalNode() {}

func (b BackwardFillNode) Children() []Node { return []Node{b.Input} }

func (b BackwardFillNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: BackwardFillNode takes one child")
	}
	return BackwardFillNode{Input: children[0], Limit: b.Limit}
}

func (b BackwardFillNode) Schema() (*schema.Schema, error) { return b.Input.Schema() }
func (b BackwardFillNode) String() string                  { return fmt.Sprintf("BACKWARD_FILL limit=%d", b.Limit) }

// BackwardFill returns a LazyFrame that backward-fills every column.
func (lf LazyFrame) BackwardFill(limit int) LazyFrame {
	return LazyFrame{plan: BackwardFillNode{Input: lf.plan, Limit: limit}}
}

// --- Executors ---------------------------------------------------------

func executeFillNan(ctx context.Context, cfg execConfig, f FillNanNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, f.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.Apply(func(s *series.Series) (*series.Series, error) {
		return s.FillNan(f.Value)
	})
}

func executeForwardFill(ctx context.Context, cfg execConfig, f ForwardFillNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, f.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.Apply(func(s *series.Series) (*series.Series, error) {
		return s.ForwardFill(f.Limit)
	})
}

func executeBackwardFill(ctx context.Context, cfg execConfig, b BackwardFillNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, b.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.Apply(func(s *series.Series) (*series.Series, error) {
		return s.BackwardFill(b.Limit)
	})
}
