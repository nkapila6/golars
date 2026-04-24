package main

import (
	"fmt"
	"slices"
)

// columnHoverInfo returns a markdown body to show on hover when tok
// matches a column name in the frame state at lineIdx. Returns the
// empty string when the token is not a recognised column.
//
// The body lists the column's position ("column 3 of trades") plus
// the best-effort dtype pulled from the scanned source file. If no
// dtype metadata is available we still return the column+frame
// affiliation so users see a useful hover.
func columnHoverInfo(d *document, tok string, lineIdx int) string {
	st := framesAtLine(d, lineIdx+1)
	// Focus first.
	if slices.Contains(st.focus.cols, tok) {
		return renderColumnHover(tok, st.focusName, st.focus.cols)
	}
	for name, shape := range st.staged {
		if slices.Contains(shape.cols, tok) {
			return renderColumnHover(tok, name, shape.cols)
		}
	}
	return ""
}

func renderColumnHover(col, frame string, cols []string) string {
	if frame == "" {
		frame = "<focus>"
	}
	pos := slices.Index(cols, col)
	return fmt.Sprintf("**column `%s`**\n\nframe: `%s`\n\nposition: %d / %d",
		col, frame, pos+1, len(cols))
}
