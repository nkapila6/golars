package main

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/Gaurav-Gosain/golars/script"
)

// textDocument/definition
//
// Supports two jump kinds inside a .glr script:
//
//  1. `use NAME`, `drop_frame NAME`, `stash NAME`, and `join NAME on KEY`
//     jump to the `load ... as NAME` line that introduced NAME. Polars
//     users recognise this as "go to the declaration of the frame".
//  2. Column names on the right-hand side of a command jump to the
//     left-most command that defined them (the load for data columns,
//     or the groupby/agg_spec that coined a synthetic column). Only
//     the first match wins so the editor lands on a single line.

type definitionParams struct {
	TextDocument textDocumentIdentifier `json:"textDocument"`
	Position     position               `json:"position"`
}

type locationLink struct {
	URI   string   `json:"uri"`
	Range lspRange `json:"range"`
}

func (s *server) handleDefinition(msg *rawMessage) {
	var p definitionParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		s.reply(msg, nil)
		return
	}
	doc := s.docs.get(p.TextDocument.URI)
	if doc == nil {
		s.reply(msg, nil)
		return
	}

	lineIdx := int(p.Position.Line)
	line := doc.lineAt(lineIdx)
	if line == "" {
		s.reply(msg, nil)
		return
	}
	col := byteCol(line, int(p.Position.Character))
	tok, _, _ := tokenAt(line, col)
	if tok == "" {
		s.reply(msg, nil)
		return
	}

	// Scan every line looking for a `load PATH as NAME` whose NAME ==
	// tok. Return the first match.
	parts := strings.Fields(script.Normalize(line))
	// Only treat the hover token as a frame name when the cursor is on
	// a position where a frame name is expected. We keep this simple:
	// any single-word identifier tok that matches a staged frame name
	// in the document is a definition target. Column definitions are
	// returned only if no frame matches.
	if loc := findLoadAsLine(doc, tok); loc != nil {
		s.reply(msg, []locationLink{*loc})
		return
	}
	// Column definition fallback: first `load ... as NAME` or the load
	// line itself whose CSV header introduced this column.
	if loc := findColumnDefinition(doc, tok); loc != nil {
		s.reply(msg, []locationLink{*loc})
		return
	}
	// If the user is on `join NAME on KEY`, the KEY token may match a
	// column. Above search handles that.
	_ = parts
	s.reply(msg, nil)
}

// findLoadAsLine scans doc for `load PATH as NAME` (NAME == target)
// and returns the location of NAME in that line.
func findLoadAsLine(d *document, target string) *locationLink {
	for li, raw := range d.lines {
		stmt := script.Normalize(raw)
		if stmt == "" {
			continue
		}
		parts := strings.Fields(stmt)
		if len(parts) < 4 {
			continue
		}
		if !strings.EqualFold(strings.TrimPrefix(parts[0], "."), "load") {
			continue
		}
		if !strings.EqualFold(parts[2], "as") {
			continue
		}
		if parts[3] != target {
			continue
		}
		// Find the column span of NAME in the raw line.
		c := indexOfToken(raw, target)
		return &locationLink{
			URI: d.uri,
			Range: lspRange{
				Start: position{Line: uint32(li), Character: uint32(c)},
				End:   position{Line: uint32(li), Character: uint32(c + len(target))},
			},
		}
	}
	return nil
}

// findColumnDefinition returns the first load line that brought a
// column named target into scope. When there is no schema metadata we
// fall back to the first load line in the file so users have a sane
// anchor to jump to.
func findColumnDefinition(d *document, target string) *locationLink {
	for li, raw := range d.lines {
		stmt := script.Normalize(raw)
		parts := strings.Fields(stmt)
		if len(parts) < 2 {
			continue
		}
		if !strings.EqualFold(strings.TrimPrefix(parts[0], "."), "load") {
			continue
		}
		// Peek the CSV header via our file cache.
		dir := docDir(d)
		abs := resolvePath(parts[1], dir)
		stats := readFileStats(abs)
		if slices.Contains(stats.cols, target) {
			c := indexOfToken(raw, parts[1])
			return &locationLink{
				URI: d.uri,
				Range: lspRange{
					Start: position{Line: uint32(li), Character: uint32(c)},
					End:   position{Line: uint32(li), Character: uint32(c + len(parts[1]))},
				},
			}
		}
	}
	return nil
}
