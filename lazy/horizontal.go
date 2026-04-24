package lazy

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// horizontalOp identifies which row-wise reduction a HorizontalNode
// applies. Names match polars' sum_horizontal / min_horizontal /
// max_horizontal / mean_horizontal / all_horizontal / any_horizontal.
type horizontalOp int

const (
	horizSumOp horizontalOp = iota
	horizMeanOp
	horizMinOp
	horizMaxOp
	horizAllOp
	horizAnyOp
)

func (h horizontalOp) String() string {
	switch h {
	case horizSumOp:
		return "sum_horizontal"
	case horizMeanOp:
		return "mean_horizontal"
	case horizMinOp:
		return "min_horizontal"
	case horizMaxOp:
		return "max_horizontal"
	case horizAllOp:
		return "all_horizontal"
	case horizAnyOp:
		return "any_horizontal"
	}
	return "horizontal"
}

// HorizontalNode appends a single output column equal to the row-wise
// reduction of Cols (or every compatible column when Cols is empty).
// Equivalent to `lf.with_columns(pl.sum_horizontal(...).alias(Out))`
// in polars.
type HorizontalNode struct {
	Input Node
	Op    horizontalOp
	Cols  []string
	Out   string
}

func (HorizontalNode) isLogicalNode() {}

func (h HorizontalNode) Children() []Node { return []Node{h.Input} }

func (h HorizontalNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: HorizontalNode takes one child")
	}
	return HorizontalNode{Input: children[0], Op: h.Op, Cols: h.Cols, Out: h.Out}
}

func (h HorizontalNode) Schema() (*schema.Schema, error) {
	in, err := h.Input.Schema()
	if err != nil {
		return nil, err
	}
	fields := make([]schema.Field, 0, in.Len()+1)
	for i := range in.Len() {
		fields = append(fields, in.Field(i))
	}
	outDtype := h.outputDType(in)
	fields = append(fields, schema.Field{Name: h.Out, DType: outDtype})
	return schema.New(fields...)
}

// outputDType infers the result dtype following polars' rules. For
// sum/min/max: same-dtype input keeps that dtype; mixed numeric inputs
// collapse to Float64. For mean: always Float64. For all/any: Boolean.
func (h HorizontalNode) outputDType(in *schema.Schema) dtype.DType {
	if h.Op == horizAllOp || h.Op == horizAnyOp {
		return dtype.Bool()
	}
	if h.Op == horizMeanOp {
		return dtype.Float64()
	}
	var picked dtype.DType
	any := false
	for i := range in.Len() {
		f := in.Field(i)
		if len(h.Cols) > 0 && !slices.Contains(h.Cols, f.Name) {
			continue
		}
		if !f.DType.IsNumeric() {
			continue
		}
		if !any {
			picked = f.DType
			any = true
			continue
		}
		if picked.ID() != f.DType.ID() {
			return dtype.Float64()
		}
	}
	if !any {
		return dtype.Float64()
	}
	return picked
}

func (h HorizontalNode) String() string {
	cols := "*"
	if len(h.Cols) > 0 {
		cols = strings.Join(h.Cols, ",")
	}
	return fmt.Sprintf("HORIZONTAL %s cols=[%s] as %q", h.Op, cols, h.Out)
}

// SumHorizontal appends a row-wise sum column named out. The reduction
// spans cols (all numeric when cols is empty). Nulls are ignored per
// polars' default.
func (lf LazyFrame) SumHorizontal(out string, cols ...string) LazyFrame {
	return LazyFrame{plan: HorizontalNode{Input: lf.plan, Op: horizSumOp, Cols: cols, Out: out}}
}

// MeanHorizontal appends a row-wise mean column.
func (lf LazyFrame) MeanHorizontal(out string, cols ...string) LazyFrame {
	return LazyFrame{plan: HorizontalNode{Input: lf.plan, Op: horizMeanOp, Cols: cols, Out: out}}
}

// MinHorizontal appends a row-wise min column.
func (lf LazyFrame) MinHorizontal(out string, cols ...string) LazyFrame {
	return LazyFrame{plan: HorizontalNode{Input: lf.plan, Op: horizMinOp, Cols: cols, Out: out}}
}

// MaxHorizontal appends a row-wise max column.
func (lf LazyFrame) MaxHorizontal(out string, cols ...string) LazyFrame {
	return LazyFrame{plan: HorizontalNode{Input: lf.plan, Op: horizMaxOp, Cols: cols, Out: out}}
}

// AllHorizontal appends a boolean AND column.
func (lf LazyFrame) AllHorizontal(out string, cols ...string) LazyFrame {
	return LazyFrame{plan: HorizontalNode{Input: lf.plan, Op: horizAllOp, Cols: cols, Out: out}}
}

// AnyHorizontal appends a boolean OR column.
func (lf LazyFrame) AnyHorizontal(out string, cols ...string) LazyFrame {
	return LazyFrame{plan: HorizontalNode{Input: lf.plan, Op: horizAnyOp, Cols: cols, Out: out}}
}

func executeHorizontal(ctx context.Context, cfg execConfig, h HorizontalNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, h.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()

	reduced, err := reduceHorizontal(ctx, input, h)
	if err != nil {
		return nil, err
	}
	// reduced is owned by us; dataframe.New consumes on success.
	out, err := dataframe.New(reduced.Rename(h.Out))
	reduced.Release()
	if err != nil {
		return nil, err
	}
	defer out.Release()
	return input.HStack(out)
}

func reduceHorizontal(ctx context.Context, df *dataframe.DataFrame, h HorizontalNode) (*series.Series, error) {
	switch h.Op {
	case horizSumOp:
		return df.SumHorizontal(ctx, dataframe.IgnoreNulls, h.Cols...)
	case horizMeanOp:
		return df.MeanHorizontal(ctx, dataframe.IgnoreNulls, h.Cols...)
	case horizMinOp:
		return df.MinHorizontal(ctx, dataframe.IgnoreNulls, h.Cols...)
	case horizMaxOp:
		return df.MaxHorizontal(ctx, dataframe.IgnoreNulls, h.Cols...)
	case horizAllOp:
		return df.AllHorizontal(ctx, dataframe.IgnoreNulls, h.Cols...)
	case horizAnyOp:
		return df.AnyHorizontal(ctx, dataframe.IgnoreNulls, h.Cols...)
	}
	return nil, fmt.Errorf("lazy: unknown horizontal op %v", h.Op)
}
