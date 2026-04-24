package lazy

import (
	"fmt"
	"strings"
)

// Explain renders a plan with one node per line, indenting children. Children
// appear below and indented from their parent.
func Explain(n Node) string {
	var b strings.Builder
	explainInto(&b, n, 0)
	return b.String()
}

func explainInto(b *strings.Builder, n Node, depth int) {
	indent := strings.Repeat("  ", depth)
	fmt.Fprintf(b, "%s%s\n", indent, n.String())
	for _, c := range n.Children() {
		explainInto(b, c, depth+1)
	}
}

// ExplainFull runs the default optimizer against the plan (without executing
// it) and produces a three-section report: logical plan, pass log, and
// optimized plan. This matches polars' .explain() output.
func ExplainFull(n Node) (string, error) {
	opt := DefaultOptimizer()
	optimized, traces, err := opt.Optimize(n)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("== Logical plan ==\n")
	b.WriteString(Explain(n))
	b.WriteString("\n== Optimizer ==\n")
	b.WriteString(FormatTraces(traces))
	b.WriteString("\n== Optimized plan ==\n")
	b.WriteString(Explain(optimized))
	return b.String(), nil
}
