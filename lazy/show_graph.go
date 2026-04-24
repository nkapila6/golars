package lazy

import (
	"fmt"
	"strings"
)

// MermaidGraph renders a plan as a Mermaid flowchart definition.
// Feed the returned string to `mermaid-cli` or any Markdown viewer
// that supports Mermaid (GitHub, mkdocs, Notion, Fumadocs) to see a
// visual plan diagram.
//
// Node labels mirror the text form produced by Node.String(). Each
// node is keyed by its depth-first ordinal so shared children are
// rendered as shared nodes in the graph.
func MermaidGraph(n Node) string {
	var b strings.Builder
	// `graph TD` instead of `flowchart TD`: both are valid Mermaid
	// but `graph` is the older keyword accepted by every renderer,
	// including ASCII tools like mermaid-ascii. flowchart-only
	// syntax is a superset we don't need.
	b.WriteString("graph TD\n")
	var next int
	var walk func(Node) int
	walk = func(cur Node) int {
		id := next
		next++
		label := mermaidEscape(cur.String())
		fmt.Fprintf(&b, "    n%d[\"%s\"]\n", id, label)
		for _, c := range cur.Children() {
			cid := walk(c)
			// Child feeds its parent, so the edge points from child
			// (producer) to parent (consumer). Mirrors Mermaid's
			// convention for dataflow diagrams.
			fmt.Fprintf(&b, "    n%d --> n%d\n", cid, id)
		}
		return id
	}
	walk(n)
	return b.String()
}

// mermaidEscape makes a plan's String() payload safe to embed
// inside a `["..."]` rectangle label. Mermaid's label parser
// breaks on bare double quotes, newlines, and any literal `[` /
// `]` (the outer brackets delimit the label, so inner brackets
// confuse strict parsers like mermaid-ascii). We also drop pipe
// characters that some renderers treat as edge labels.
func mermaidEscape(s string) string {
	s = strings.ReplaceAll(s, `"`, `'`)
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "[", "(")
	s = strings.ReplaceAll(s, "]", ")")
	s = strings.ReplaceAll(s, "|", "/")
	return s
}

// ShowGraph is the convenience wrapper on LazyFrame.
func (lf LazyFrame) ShowGraph() string {
	return MermaidGraph(lf.plan)
}
