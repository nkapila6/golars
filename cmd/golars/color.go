package main

import (
	"os"
	"slices"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
)

// colorsEnabled reports whether golars should emit ANSI color
// escapes. The rules, in priority order:
//
//  1. `--no-color` anywhere in argv.
//  2. `NO_COLOR` env var set (https://no-color.org convention).
//  3. Stdout is not a TTY.
//  4. Otherwise on.
//
// The TTY probe relies on os.Stdout's file-mode character-device
// bit. Zero-dependency and matches the convention used by every
// modern CLI (gh, fd, ripgrep, etc).
func colorsEnabled(args []string) bool {
	if slices.Contains(args, "--no-color") {
		return false
	}
	if _, ok := os.LookupEnv("NO_COLOR"); ok {
		return false
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// stripNoColorArg removes the --no-color flag from args so it doesn't
// leak into individual subcommand parsers.
func stripNoColorArg(args []string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		if a == "--no-color" {
			continue
		}
		out = append(out, a)
	}
	return out
}

// applyColorPolicy decides whether Render calls emit ANSI escapes.
// When color is off, the global style vars in main.go are replaced
// with empty styles so every Render returns its input unchanged.
// lipgloss.Writer's profile is also flipped so rare direct-writer
// callers get ASCII output too.
func applyColorPolicy(enable bool) {
	if enable {
		return
	}
	lipgloss.Writer.Profile = colorprofile.Ascii
	plain := lipgloss.NewStyle()
	logoStyle = plain
	promptStyle = plain
	errStyle = plain
	errMsgStyle = plain
	successStyle = plain
	infoStyle = plain
	dimStyle = plain
	cmdStyle = plain
	titleStyle = plain
	headerStyle = plain
}
