package repl_test

import (
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Gaurav-Gosain/golars/repl"
)

// FuzzCompletePrefix asserts the fundamental invariants of
// CompletePrefix across arbitrary input: the returned ghost, appended
// to the partial, always reconstructs one of the candidates.
func FuzzCompletePrefix(f *testing.F) {
	f.Add(".se", ".select,.sort,.show")
	f.Add("", ".foo,.bar")
	f.Add("x", "")
	f.Add("日本", "日本語,日常")
	f.Add("😀", "😀🎉,😀🎊,other")
	f.Fuzz(func(t *testing.T, partial, candidatesCSV string) {
		cands := strings.Split(candidatesCSV, ",")
		g := repl.CompletePrefix(partial, cands)
		if g == "" {
			return
		}
		// Invariant 1: partial+g must equal some candidate.
		full := partial + g
		found := slices.Contains(cands, full)
		if !found {
			t.Fatalf("CompletePrefix(%q, %v) returned %q; %q not in candidates", partial, cands, g, full)
		}
		// Invariant 2: ghost is strictly non-empty when returned.
		if g == "" {
			t.Fatal("non-empty ghost invariant")
		}
		// Invariant 3: returned ghost, when appended, keeps UTF-8 valid
		// IF both partial and the matched candidate were valid UTF-8.
		if utf8.ValidString(partial) && utf8.ValidString(full) && !utf8.ValidString(g) {
			t.Fatalf("ghost %q is invalid UTF-8 despite valid inputs", g)
		}
	})
}

func FuzzCompleteFromList(f *testing.F) {
	f.Add("a,b,c", "age,city,country")
	f.Add("", "anything,")
	f.Add(",", "one,two")
	f.Add("日,aβ", "αβγ,abc")
	f.Fuzz(func(t *testing.T, current, itemsCSV string) {
		items := strings.Split(itemsCSV, ",")
		// Must not panic.
		_ = repl.CompleteFromList(current, items, ',')
		_ = repl.CompleteFromList(current, items, 0)
	})
}

func FuzzCompletePath(f *testing.F) {
	// Focus on panic-freedom; we can't assert filesystem shape but
	// the function must tolerate any input without crashing.
	f.Add("foo", "/tmp")
	f.Add("", "")
	f.Add("/", "/")
	f.Add("../../", "/home")
	f.Add("\x00/etc", "/")
	f.Fuzz(func(t *testing.T, current, cwd string) {
		_ = repl.CompletePath(current, cwd)
	})
}

func FuzzSplitFields(f *testing.F) {
	f.Add("")
	f.Add(" \t \n")
	f.Add("one two three")
	f.Add("leading trailing ")
	f.Add("日本 語 ")
	f.Fuzz(func(t *testing.T, line string) {
		parts, tws := repl.SplitFields(line)
		// Invariant: reconstructed join with single space must produce
		// a string whose Fields() equals parts. (Roundtrip via
		// whitespace collapse.)
		joined := strings.Join(parts, " ")
		if back := strings.Fields(joined); !strSliceEq(back, parts) {
			t.Fatalf("SplitFields roundtrip failed: %v -> %q -> %v", parts, joined, back)
		}
		// Invariant: trailingSpace is set iff the line has trailing WS.
		expect := line != "" && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t')
		if tws != expect {
			t.Fatalf("trailing-space mismatch for %q: got %v, want %v", line, tws, expect)
		}
	})
}
