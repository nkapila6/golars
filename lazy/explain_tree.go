package lazy

import (
	"fmt"
	"strings"
)

// TreeStyle controls the glyphs ExplainTree uses when drawing the
// plan. Switch to ASCIITreeStyle when the terminal cannot render
// box-drawing characters.
type TreeStyle struct {
	// Branch is printed before a child that has siblings below it.
	// Defaults to "├── ".
	Branch string
	// LastBranch is printed before the final child at each depth.
	// Defaults to "└── ".
	LastBranch string
	// Vertical extends a parent branch past a child that has
	// siblings below. Defaults to "│   ".
	Vertical string
	// Space extends a parent branch past the final child.
	// Defaults to "    ".
	Space string
}

// UnicodeTreeStyle is the default box-drawing style.
var UnicodeTreeStyle = TreeStyle{
	Branch:     "├── ",
	LastBranch: "└── ",
	Vertical:   "│   ",
	Space:      "    ",
}

// ASCIITreeStyle is the fallback style for terminals that do not
// render box-drawing characters.
var ASCIITreeStyle = TreeStyle{
	Branch:     "|-- ",
	LastBranch: "`-- ",
	Vertical:   "|   ",
	Space:      "    ",
}

// ExplainTree renders a plan as a tree with box-drawing connectors.
// The root is flush-left and each child is drawn under its parent
// with ├── / └── glyphs so nesting is easier to follow than the
// indented-only form produced by Explain.
//
//	SORT [total desc]
//	└── AGG keys=[dept] aggs=[...]
//	    └── FILTER (col("salary") > 75)
//	        └── SCAN df
func ExplainTree(n Node) string {
	return ExplainTreeWith(n, UnicodeTreeStyle)
}

// ExplainTreeASCII is ExplainTree with the ASCII fallback style.
// Prefer ExplainTree; use this only when the output target cannot
// render Unicode box-drawing characters.
func ExplainTreeASCII(n Node) string {
	return ExplainTreeWith(n, ASCIITreeStyle)
}

// ExplainTreeWith renders a plan using the caller-supplied style.
// Useful when embedding the output in a larger formatter that picks
// its own glyph set.
func ExplainTreeWith(n Node, style TreeStyle) string {
	var b strings.Builder
	fmt.Fprintln(&b, n.String())
	children := n.Children()
	for i, c := range children {
		last := i == len(children)-1
		explainTreeInto(&b, c, style, "", last)
	}
	return b.String()
}

func explainTreeInto(b *strings.Builder, n Node, style TreeStyle, prefix string, last bool) {
	branch := style.Branch
	next := style.Vertical
	if last {
		branch = style.LastBranch
		next = style.Space
	}
	// Multi-line node headers (rare) are split so each continuation
	// line receives the same indentation as the first, making the
	// tree readable even when an aggregate dumps many exprs.
	head := n.String()
	lines := strings.Split(strings.TrimRight(head, "\n"), "\n")
	fmt.Fprintf(b, "%s%s%s\n", prefix, branch, lines[0])
	for _, cont := range lines[1:] {
		fmt.Fprintf(b, "%s%s%s\n", prefix, next, cont)
	}
	childPrefix := prefix + next
	children := n.Children()
	for i, c := range children {
		explainTreeInto(b, c, style, childPrefix, i == len(children)-1)
	}
}

// ExplainTreeFull is the tree analogue of ExplainFull: runs the
// default optimizer (without execution) and returns a three-section
// report with the tree-drawn logical and optimized plans.
func ExplainTreeFull(n Node) (string, error) {
	opt := DefaultOptimizer()
	optimized, traces, err := opt.Optimize(n)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("== Logical plan ==\n")
	b.WriteString(ExplainTree(n))
	b.WriteString("\n== Optimizer ==\n")
	b.WriteString(FormatTraces(traces))
	b.WriteString("\n== Optimized plan ==\n")
	b.WriteString(ExplainTree(optimized))
	return b.String(), nil
}
