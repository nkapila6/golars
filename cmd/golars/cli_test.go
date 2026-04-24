package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestRootCommandTree smoke-tests that every advertised subcommand is
// actually registered. Catches typos and missing AddCommand entries
// after a rewrite.
func TestRootCommandTree(t *testing.T) {
	t.Parallel()
	root := newRootCmd()
	want := []string{
		"browse", "cat", "convert", "diff", "doctor", "explain", "fmt",
		"head", "lint", "peek", "repl", "run", "sample", "schema",
		"sql", "stats", "tail",
	}
	got := map[string]bool{}
	for _, c := range root.Commands() {
		got[c.Name()] = true
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("subcommand %q not registered", name)
		}
	}
}

// TestSqlAliasResolves checks that `golars query` routes to the sql
// command (we advertise `query` as an alias in docs).
func TestSqlAliasResolves(t *testing.T) {
	t.Parallel()
	root := newRootCmd()
	c, _, err := root.Find([]string{"query"})
	if err != nil {
		t.Fatalf("find query alias: %v", err)
	}
	if c.Name() != "sql" {
		t.Errorf("query alias should resolve to sql, got %q", c.Name())
	}
}

// TestFormatFlagShorthandsWin checks that the bool shorthands (--csv,
// --json, ...) take precedence over --format.
func TestFormatFlagShorthandsWin(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(*formatFlags)
		want  outputFormat
	}{
		{"default-table", func(ff *formatFlags) {}, fmtTable},
		{"explicit-format-csv", func(ff *formatFlags) { ff.format = "csv" }, fmtCSV},
		{"json-shorthand", func(ff *formatFlags) { ff.json = true }, fmtJSON},
		{"ndjson-shorthand", func(ff *formatFlags) { ff.ndjson = true }, fmtNDJSON},
		{"tsv-shorthand", func(ff *formatFlags) { ff.tsv = true }, fmtTSV},
		{"markdown-shorthand", func(ff *formatFlags) { ff.markdown = true }, fmtMarkdown},
		{"parquet-shorthand", func(ff *formatFlags) { ff.parquet = true }, fmtParquet},
		{"arrow-shorthand", func(ff *formatFlags) { ff.arrow = true }, fmtArrow},
		{"shorthand-beats-format", func(ff *formatFlags) { ff.format = "csv"; ff.json = true }, fmtJSON},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ff := &formatFlags{format: "table"}
			tc.setup(ff)
			got, err := ff.resolve()
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got != tc.want {
				t.Errorf("want %q, got %q", tc.want, got)
			}
		})
	}
}

// TestFormatFlagUnknown rejects a bogus --format value.
func TestFormatFlagUnknown(t *testing.T) {
	t.Parallel()
	ff := &formatFlags{format: "pickle"}
	_, err := ff.resolve()
	if err == nil {
		t.Fatal("resolve should reject unknown format")
	}
	if !strings.Contains(err.Error(), "pickle") {
		t.Errorf("error should name the bad value, got: %v", err)
	}
}

// TestFormatFlagBindings confirms every subcommand that should accept
// format flags actually registered them. Guards against a copy-paste
// mistake where a new data-returning subcommand forgets bindFormatFlags.
func TestFormatFlagBindings(t *testing.T) {
	t.Parallel()
	cmd := &cobra.Command{Use: "x"}
	bindFormatFlags(cmd)
	for _, name := range []string{"format", "json", "ndjson", "csv", "tsv", "markdown", "parquet", "arrow"} {
		if cmd.Flag(name) == nil {
			t.Errorf("bindFormatFlags missed --%s", name)
		}
	}
	// Sanity-check the static supportedFormats list matches the resolver.
	for _, f := range supportedFormats {
		if !isKnownFormat(f) {
			t.Errorf("supportedFormats contains unknown format %q", f)
		}
	}
}

// TestReplBindsReplFlags checks that `golars repl` exposes the same
// --load / --run / --preview flags as the root. This regressed during
// the cobra migration.
func TestReplBindsReplFlags(t *testing.T) {
	t.Parallel()
	repl := newReplCmd()
	want := []string{"load", "run", "preview", "preview-rows", "timing"}
	for _, name := range want {
		if repl.Flag(name) == nil {
			t.Errorf("repl should bind --%s", name)
		}
	}
}

// TestHelpDoesNotPanic covers the bug where a subcommand with
// DisableFlagParsing crashed on --help. Every subcommand should print
// help without exiting with a parsing error.
func TestHelpDoesNotPanic(t *testing.T) {
	t.Parallel()
	names := []string{
		"schema", "sql", "head", "tail", "diff", "stats",
		"peek", "sample", "convert", "cat", "browse",
		"run", "fmt", "lint", "explain", "doctor",
	}
	for _, n := range names {
		t.Run(n, func(t *testing.T) {
			t.Parallel()
			root := newRootCmd()
			root.SetArgs([]string{n, "--help"})
			root.SetOut(&bytes.Buffer{})
			root.SetErr(&bytes.Buffer{})
			if err := root.Execute(); err != nil {
				t.Errorf("%s --help returned error: %v", n, err)
			}
		})
	}
}
