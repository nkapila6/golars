package repl_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/repl"
)

// newPlain builds a Prompt rigged to use the non-TTY fallback (by
// supplying a non-nil Input). Lets tests drive ReadLine deterministically.
func newPlain(t *testing.T, in string, opts repl.Options) *repl.Prompt {
	t.Helper()
	if opts.Input == nil {
		opts.Input = strings.NewReader(in)
	}
	if opts.Output == nil {
		opts.Output = io.Discard
	}
	return repl.New(opts)
}

func TestReadLinePlainSingleLine(t *testing.T) {
	p := newPlain(t, "hello\n", repl.Options{})
	line, err := p.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if line != "hello" {
		t.Fatalf("got %q", line)
	}
}

func TestReadLinePlainEmptyIsEOF(t *testing.T) {
	p := newPlain(t, "", repl.Options{})
	_, err := p.ReadLine()
	if !errors.Is(err, repl.ErrEOF) {
		t.Fatalf("want ErrEOF, got %v", err)
	}
}

func TestReadLinePlainMultipleLinesAndEOF(t *testing.T) {
	p := newPlain(t, "one\ntwo\nthree\n", repl.Options{})
	got := []string{}
	for {
		line, err := p.ReadLine()
		if errors.Is(err, repl.ErrEOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, line)
	}
	want := []string{"one", "two", "three"}
	if !strSliceEq(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReadLinePlainAddsHistory(t *testing.T) {
	p := newPlain(t, "first\nsecond\nsecond\n\n", repl.Options{})
	for {
		if _, err := p.ReadLine(); errors.Is(err, repl.ErrEOF) {
			break
		}
	}
	h := p.History()
	// Blank lines skipped; consecutive duplicate collapsed.
	want := []string{"first", "second"}
	if !strSliceEq(h, want) {
		t.Fatalf("got %v, want %v", h, want)
	}
}

func TestReadLinePlainVeryLongLine(t *testing.T) {
	long := strings.Repeat("x", 512*1024)
	p := newPlain(t, long+"\n", repl.Options{})
	line, err := p.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if len(line) != len(long) || line != long {
		t.Fatalf("long line length mismatch: got %d want %d", len(line), len(long))
	}
}

func TestReadLinePlainSuggesterNotInvoked(t *testing.T) {
	// In non-TTY mode the Suggester should not be consulted at all.
	called := false
	p := newPlain(t, "foo\n", repl.Options{
		Suggester: repl.SuggesterFunc(func(string) (string, string) {
			called = true
			return "", ""
		}),
	})
	if _, err := p.ReadLine(); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("suggester called in non-TTY mode")
	}
}

func TestAddHistoryDeduplication(t *testing.T) {
	p := newPlain(t, "", repl.Options{MaxHistory: 5})
	p.AddHistory("a")
	p.AddHistory("a") // consecutive duplicate dropped
	p.AddHistory("b")
	p.AddHistory("a")   // non-consecutive allowed
	p.AddHistory("")    // blank dropped
	p.AddHistory("   ") // whitespace-only dropped
	h := p.History()
	want := []string{"a", "b", "a"}
	if !strSliceEq(h, want) {
		t.Fatalf("got %v, want %v", h, want)
	}
}

func TestAddHistoryCaps(t *testing.T) {
	p := newPlain(t, "", repl.Options{MaxHistory: 3})
	for i, s := range []string{"a", "b", "c", "d", "e"} {
		p.AddHistory(s)
		_ = i
	}
	h := p.History()
	want := []string{"c", "d", "e"}
	if !strSliceEq(h, want) {
		t.Fatalf("got %v, want %v", h, want)
	}
}

func TestHistoryRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hist")

	p1 := newPlain(t, "", repl.Options{HistoryPath: path})
	p1.AddHistory("one")
	p1.AddHistory("two")
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	p2 := newPlain(t, "", repl.Options{HistoryPath: path})
	h := p2.History()
	want := []string{"one", "two"}
	if !strSliceEq(h, want) {
		t.Fatalf("persisted history: got %v, want %v", h, want)
	}
}

func TestHistoryMissingFileIsEmpty(t *testing.T) {
	p := newPlain(t, "", repl.Options{HistoryPath: filepath.Join(t.TempDir(), "nope")})
	if len(p.History()) != 0 {
		t.Fatal("missing file should yield empty history")
	}
}

func TestHistorySaveNoopNoPath(t *testing.T) {
	p := newPlain(t, "", repl.Options{})
	p.AddHistory("x")
	if err := p.Save(); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryCappedOnLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hist")
	var lines []string
	for i := range 20 {
		lines = append(lines, string(rune('a'+i)))
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := newPlain(t, "", repl.Options{HistoryPath: path, MaxHistory: 5})
	h := p.History()
	want := lines[15:20]
	if !strSliceEq(h, want) {
		t.Fatalf("capped load: got %v, want %v", h, want)
	}
}

func TestHistoryCRLFTolerated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hist")
	if err := os.WriteFile(path, []byte("one\r\ntwo\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := newPlain(t, "", repl.Options{HistoryPath: path})
	h := p.History()
	if !strSliceEq(h, []string{"one", "two"}) {
		t.Fatalf("CRLF: got %v", h)
	}
}

func TestHistoryMutationIsolation(t *testing.T) {
	p := newPlain(t, "", repl.Options{})
	p.AddHistory("secret")
	h := p.History()
	h[0] = "tampered"
	if p.History()[0] != "secret" {
		t.Fatal("History() must return a defensive copy")
	}
}

func TestDefaultsApplied(t *testing.T) {
	p := newPlain(t, "", repl.Options{})
	// MaxHistory 0 becomes DefaultMaxHistory; add many lines and verify
	// nothing was trimmed before 1000.
	for i := range 900 {
		p.AddHistory(letters(i))
	}
	if len(p.History()) != 900 {
		t.Fatalf("default MaxHistory did not apply: len=%d", len(p.History()))
	}
}

// Adversarial inputs for the plain reader: ensure no panic on binary
// bytes. Scanner returns the full line minus the '\n' terminator.
func TestReadLinePlainBinarySafe(t *testing.T) {
	in := bytes.NewReader([]byte{0x00, 0x01, 0x7f, 0x80, 0xff, '\n'})
	p := repl.New(repl.Options{Input: in, Output: io.Discard})
	line, err := p.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if len(line) != 5 {
		t.Fatalf("binary line length: got %d, want 5", len(line))
	}
}

func letters(i int) string {
	if i == 0 {
		return "a"
	}
	var b [8]byte
	n := 0
	for ; i > 0; i /= 26 {
		b[n] = byte('a' + i%26)
		n++
	}
	return string(b[:n])
}
