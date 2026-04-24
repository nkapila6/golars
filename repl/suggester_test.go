package repl_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/repl"
)

func TestSuggesterFunc(t *testing.T) {
	s := repl.SuggesterFunc(func(v string) (string, string) { return "g-" + v, "h-" + v })
	g, h := s.Suggest("x")
	if g != "g-x" || h != "h-x" {
		t.Fatalf("SuggesterFunc forwarded wrong: got (%q, %q)", g, h)
	}
}

func TestCompletePrefix(t *testing.T) {
	cands := []string{".select", ".sort", ".show", ".head"}
	cases := []struct {
		partial, want string
	}{
		{".se", "lect"},
		{".sh", "ow"},
		{".s", "elect"},  // first match wins
		{".z", ""},       // no match
		{"", ""},         // empty partial
		{".select", ""},  // exact match: no extension
		{".selectx", ""}, // too-long partial
	}
	for _, c := range cases {
		t.Run(c.partial, func(t *testing.T) {
			if g := repl.CompletePrefix(c.partial, cands); g != c.want {
				t.Fatalf("CompletePrefix(%q) = %q, want %q", c.partial, g, c.want)
			}
		})
	}
}

func TestCompletePrefixUnicode(t *testing.T) {
	cands := []string{"αβγ", "αβγδ", "emoji-🎉"}
	if g := repl.CompletePrefix("αβ", cands); g != "γ" {
		t.Fatalf("unicode prefix: got %q", g)
	}
	if g := repl.CompletePrefix("emoji-", cands); g != "🎉" {
		t.Fatalf("emoji suffix: got %q", g)
	}
}

func TestCompleteFromListComma(t *testing.T) {
	cols := []string{"age", "agility", "city", "country"}
	cases := []struct {
		current, want string
	}{
		{"ag", "e"},       // first alphabetical after "ag" is "age"
		{"a,b,ag", "e"},   // after comma, we're completing "ag"
		{"age,ci", "ty"},  // post-comma completion
		{"age,", ""},      // empty post-comma prefix → no ghost
		{"age, ci", "ty"}, // space after comma trimmed
		{"", ""},          // empty
		{"z", ""},         // no match
	}
	for _, c := range cases {
		t.Run(c.current, func(t *testing.T) {
			if g := repl.CompleteFromList(c.current, cols, ','); g != c.want {
				t.Fatalf("CompleteFromList(%q) = %q, want %q", c.current, g, c.want)
			}
		})
	}
}

func TestCompleteFromListNoSep(t *testing.T) {
	items := []string{"apple", "banana"}
	if g := repl.CompleteFromList("app", items, 0); g != "le" {
		t.Fatalf("no-sep: got %q", g)
	}
}

func TestCompletePath(t *testing.T) {
	dir := t.TempDir()
	mustMkdir(t, filepath.Join(dir, "subdir"))
	mustTouch(t, filepath.Join(dir, "data.csv"))
	mustTouch(t, filepath.Join(dir, "data.parquet"))
	mustTouch(t, filepath.Join(dir, "other.txt"))

	// Relative-to-cwd: current "da" against dir
	if g := repl.CompletePath("da", dir); g != "ta.csv" {
		t.Fatalf("relative data: got %q", g)
	}
	// Directory entries get trailing slash
	if g := repl.CompletePath("sub", dir); g != "dir/" {
		t.Fatalf("directory: got %q", g)
	}
	// Absolute path: dir as prefix
	abs := filepath.Join(dir, "da")
	if g := repl.CompletePath(abs, ""); g != "ta.csv" {
		t.Fatalf("absolute: got %q", g)
	}
	// Nonexistent directory → ""
	if g := repl.CompletePath("/nonexistent-a1b2c3/", ""); g != "" {
		t.Fatalf("nonexistent: got %q", g)
	}
	// Empty current → ""
	if g := repl.CompletePath("", dir); g != "" {
		t.Fatalf("empty: got %q", g)
	}
}

func TestCompletePathRelativeSubdir(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	mustMkdir(t, sub)
	mustTouch(t, filepath.Join(sub, "nested.csv"))
	if g := repl.CompletePath("sub/nes", dir); g != "ted.csv" {
		t.Fatalf("nested: got %q", g)
	}
}

func TestSplitFields(t *testing.T) {
	cases := []struct {
		line    string
		parts   []string
		trailWS bool
	}{
		{"", nil, false},
		{"one", []string{"one"}, false},
		{"one ", []string{"one"}, true},
		{" one  two ", []string{"one", "two"}, true},
		{"\tfoo\tbar\t", []string{"foo", "bar"}, true},
		{"nowsafterthis", []string{"nowsafterthis"}, false},
	}
	for _, c := range cases {
		t.Run(c.line, func(t *testing.T) {
			p, tws := repl.SplitFields(c.line)
			if !strSliceEq(p, c.parts) || tws != c.trailWS {
				t.Fatalf("SplitFields(%q) = (%v, %v), want (%v, %v)", c.line, p, tws, c.parts, c.trailWS)
			}
		})
	}
}

// Edge case: completion helpers must tolerate a candidate that's an
// empty string. Documented behavior: treat as no-match (cannot extend
// a partial using a zero-length candidate).
func TestCompletionWithEmptyCandidate(t *testing.T) {
	if g := repl.CompletePrefix("a", []string{""}); g != "" {
		t.Fatalf("empty candidate: got %q", g)
	}
	if g := repl.CompleteFromList("a", []string{""}, ','); g != "" {
		t.Fatalf("empty candidate list: got %q", g)
	}
}

// Edge case: current equal to a candidate must produce "" (no
// "extension" possible without appending nothing).
func TestCompletionExactMatch(t *testing.T) {
	if g := repl.CompletePrefix(".help", []string{".help", ".helpful"}); g != "ful" {
		t.Fatalf("should skip exact and land on .helpful: got %q", g)
	}
	if g := repl.CompletePrefix(".help", []string{".help"}); g != "" {
		t.Fatalf("only exact match: got %q", g)
	}
}

func strSliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustTouch(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}

// Sanity check: the completers should never panic on adversarial input
// (nil slices, empty strings, long strings, control chars).
func TestCompletionResilience(t *testing.T) {
	weird := []string{
		"", " ", "\x00", "\x00\x01\x02",
		strings.Repeat("a", 4096),
		"日本語", "🎉🎊", "abc\ndef",
	}
	for _, w := range weird {
		_ = repl.CompletePrefix(w, nil)
		_ = repl.CompletePrefix(w, weird)
		_ = repl.CompleteFromList(w, weird, ',')
		_ = repl.CompleteFromList(w, nil, ',')
		_, _ = repl.SplitFields(w)
	}
}
