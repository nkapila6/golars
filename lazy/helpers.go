package lazy

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
)

// WithColumn returns a LazyFrame extended with a single computed
// column. Convenience around WithColumns(single).
func (lf LazyFrame) WithColumn(e expr.Expr) LazyFrame {
	return lf.WithColumns(e)
}

// Tail restricts the lazy output to the last n rows. Requires
// materialising the upstream plan (no streaming shortcut: reversal
// by definition needs the full input).
func (lf LazyFrame) Tail(n int) LazyFrame {
	return LazyFrame{plan: TailNode{Input: lf.plan, N: n}}
}

// Reverse runs the lazy pipeline and reverses the result. Like Tail,
// this materialises through Collect; the optimiser can't express a
// reverse during streaming without buffering the entire input.
func (lf LazyFrame) Reverse() LazyFrame {
	return LazyFrame{plan: ReverseNode{Input: lf.plan}}
}

// TailNode is a blocking stage: buffer all upstream output, emit last N.
type TailNode struct {
	Input Node
	N     int
}

func (TailNode) isLogicalNode() {}

func (t TailNode) Children() []Node { return []Node{t.Input} }

func (t TailNode) WithChildren(ch []Node) Node {
	if len(ch) != 1 {
		panic("lazy: TailNode takes one child")
	}
	return TailNode{Input: ch[0], N: t.N}
}

func (t TailNode) Schema() (*schema.Schema, error) { return t.Input.Schema() }

func (t TailNode) String() string { return fmt.Sprintf("TAIL n=%d", t.N) }

// ReverseNode reverses upstream output in place.
type ReverseNode struct{ Input Node }

func (ReverseNode) isLogicalNode() {}

func (r ReverseNode) Children() []Node { return []Node{r.Input} }

func (r ReverseNode) WithChildren(ch []Node) Node {
	if len(ch) != 1 {
		panic("lazy: ReverseNode takes one child")
	}
	return ReverseNode{Input: ch[0]}
}

func (r ReverseNode) Schema() (*schema.Schema, error) { return r.Input.Schema() }

func (r ReverseNode) String() string { return "REVERSE" }
