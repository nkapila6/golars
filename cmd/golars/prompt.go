package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/Gaurav-Gosain/golars/repl"
)

// newCLIPrompt builds a repl.Prompt wired for golars' REPL. The
// Suggester is closured over the live *state so column names and cwd
// are always current for context-aware completions.
func newCLIPrompt(s *state) *repl.Prompt {
	historyFile := ""
	if home, err := os.UserHomeDir(); err == nil {
		historyFile = filepath.Join(home, ".golars_history")
	}
	return repl.New(repl.Options{
		PromptFunc:  prompt,
		HistoryPath: historyFile,
		Suggester:   repl.SuggesterFunc(s.suggest),
	})
}

// golarsCommands lists every dot-command the REPL accepts. Kept in one
// place so completion, .help, and command dispatch stay in sync.
var golarsCommands = []string{
	".help", ".exit", ".quit", ".load", ".save", ".show", ".schema",
	".head", ".tail", ".select", ".drop", ".filter", ".sort",
	".limit", ".describe", ".explain", ".collect", ".reset",
	".timing", ".info", ".clear", ".groupby", ".join", ".source",
	".use", ".stash", ".frames", ".drop_frame",
	".reverse", ".sample", ".shuffle", ".unique",
	".null_count", ".glimpse", ".size",
	".cast", ".fill_null", ".drop_null", ".rename",
	".sum", ".mean", ".avg", ".min", ".max", ".median", ".std",
	".write", ".with_row_index",
	".pwd", ".ls", ".cd",
	".sum_horizontal", ".mean_horizontal", ".min_horizontal",
	".max_horizontal", ".all_horizontal", ".any_horizontal",
	".sum_all", ".mean_all", ".min_all", ".max_all",
	".std_all", ".var_all", ".median_all",
	".count_all", ".null_count_all",
	".scan_csv", ".scan_parquet", ".scan_ipc", ".scan_arrow",
	".scan_json", ".scan_ndjson", ".scan_jsonl", ".scan_auto",
	".fill_nan", ".forward_fill", ".ff", ".backward_fill", ".bf",
	".top_k", ".bottom_k", ".transpose", ".unpivot", ".melt",
	".partition_by",
	".skew", ".kurtosis", ".approx_n_unique", ".corr", ".cov",
	".pivot",
}

// suggest wires the repl.Suggester callback to golars-specific context.
// Returns a ghost to append on Tab/Right-arrow and an optional right-
// side hint annotation.
func (s *state) suggest(line string) (string, string) {
	if line == "" {
		return "", "type .help for commands, tab to accept suggestions"
	}
	parts, trailingSpace := repl.SplitFields(line)
	// No space yet: completing the command itself.
	if !trailingSpace && len(parts) == 1 && strings.HasPrefix(parts[0], ".") {
		return repl.CompletePrefix(parts[0], golarsCommands), ""
	}
	if len(parts) == 0 {
		return "", ""
	}
	cmd := strings.ToLower(parts[0])
	var current string
	if !trailingSpace && len(parts) > 1 {
		current = parts[len(parts)-1]
	}
	cols := s.currentColumns()
	switch cmd {
	case ".select", ".drop", ".sort", ".describe":
		if g := repl.CompleteFromList(current, cols, ','); g != "" {
			return g, ""
		}
		if current == "" {
			return "", "expects column name"
		}
	case ".filter":
		if !strings.ContainsAny(line, "<>=!") {
			if g := repl.CompleteFromList(current, cols, ','); g != "" {
				return g, "then: op (>, <, ==, etc.) value"
			}
		}
	case ".load", ".save", ".join", ".source":
		cwd, _ := os.Getwd()
		if g := repl.CompletePath(current, cwd); g != "" {
			return g, ""
		}
	case ".head", ".tail", ".limit":
		if current == "" {
			return "", "row count (default 10)"
		}
	}
	return "", ""
}

// currentColumns returns the live schema column names for completion.
func (s *state) currentColumns() []string {
	if s.df == nil {
		return nil
	}
	return s.df.Schema().Names()
}
