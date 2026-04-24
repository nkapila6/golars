package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/script"
)

// newLintCmd reports likely bugs in a .glr script without running it.
// Detects unknown commands, unused stashes, use-before-load, and
// unbalanced quotes in filter predicates.
func newLintCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "lint FILE.glr [FILE.glr...]",
		Short:   "report common .glr mistakes without running the script",
		Example: "golars lint pipeline.glr",
		Args:    cobra.MinimumNArgs(1),
	}
	cmd.ValidArgsFunction = glrFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		warnings := 0
		for _, path := range args {
			src, err := os.ReadFile(path)
			if err != nil {
				fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
				return errSubcommandFailed
			}
			for _, m := range lintGlr(string(src)) {
				fmt.Printf("%s:%d: %s\n", path, m.line, m.msg)
				warnings++
			}
		}
		if warnings > 0 {
			fmt.Fprintf(os.Stderr, "%d lint warning(s)\n", warnings)
			return errSubcommandFailed
		}
		return nil
	}
	return cmd
}

type lintMsg struct {
	line int
	msg  string
}

// lintGlr walks the script and returns a slice of warnings. Line
// numbers are 1-based.
func lintGlr(src string) []lintMsg {
	var out []lintMsg
	stashed := map[string]int{} // name -> line where stashed
	loaded := map[string]int{}
	used := map[string]struct{}{}
	for i, raw := range strings.Split(src, "\n") {
		lineNo := i + 1
		stripped := strings.TrimSpace(raw)
		// Strip inline comment so `# comment` doesn't count.
		if j := strings.IndexByte(stripped, '#'); j >= 0 {
			stripped = strings.TrimSpace(stripped[:j])
		}
		if stripped == "" {
			continue
		}
		// Accept both "cmd" and ".cmd".
		stripped = strings.TrimPrefix(stripped, ".")
		parts := splitKeepQuoted(stripped)
		if len(parts) == 0 {
			continue
		}
		cmd := strings.ToLower(parts[0])
		if script.FindCommand(cmd) == nil {
			out = append(out, lintMsg{line: lineNo, msg: fmt.Sprintf("unknown command %q", cmd)})
			continue
		}
		switch cmd {
		case "stash":
			if len(parts) < 2 {
				out = append(out, lintMsg{line: lineNo, msg: "stash requires a NAME"})
				break
			}
			stashed[parts[1]] = lineNo
		case "use":
			if len(parts) < 2 {
				out = append(out, lintMsg{line: lineNo, msg: "use requires a NAME"})
				break
			}
			name := parts[1]
			used[name] = struct{}{}
			if _, okStash := stashed[name]; !okStash {
				if _, okLoad := loaded[name]; !okLoad {
					out = append(out, lintMsg{
						line: lineNo,
						msg:  fmt.Sprintf("use %q with no prior stash or load as", name),
					})
				}
			}
		case "load":
			// Detect `load PATH as NAME` form.
			if idx := findAs(parts); idx > 0 && idx+1 < len(parts) {
				loaded[parts[idx+1]] = lineNo
			}
		case "filter":
			if stripped := strings.Join(parts[1:], " "); countRune(stripped, '"')%2 != 0 {
				out = append(out, lintMsg{line: lineNo, msg: "filter has unbalanced quotes"})
			}
		}
	}
	for name, line := range stashed {
		if _, ok := used[name]; !ok {
			out = append(out, lintMsg{
				line: line,
				msg:  fmt.Sprintf("stash %q is never used", name),
			})
		}
	}
	return out
}

func findAs(parts []string) int {
	for i, p := range parts {
		if strings.EqualFold(p, "as") {
			return i
		}
	}
	return -1
}

func countRune(s string, r rune) int {
	n := 0
	for _, c := range s {
		if c == r {
			n++
		}
	}
	return n
}
