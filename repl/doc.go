// Package repl is a reusable building block for terminal REPLs with
// inline ghost-text completions, persistent history, and a non-TTY
// fallback for piped input and scripting.
//
// The core piece is Prompt: configure once, call ReadLine in a loop.
// Completions are driven by a user-supplied Suggester. For common
// shapes (prefix match, comma-separated item list, filesystem paths)
// the package ships CompletePrefix, CompleteFromList and CompletePath
// helpers that a Suggester can compose.
//
// Ghost rendering uses bubbletea's built-in suggestion engine so the
// block cursor overlays the first ghost character rather than
// clobbering it.
//
// Typical use:
//
//	p := repl.New(repl.Options{
//		Prompt:      "myapp » ",
//		HistoryPath: "~/.myapp_history",
//		Suggester: repl.SuggesterFunc(func(v string) (string, string) {
//			return repl.CompletePrefix(v, commands), ""
//		}),
//	})
//	defer p.Close()
//	for {
//		line, err := p.ReadLine()
//		if errors.Is(err, repl.ErrEOF) { return }
//		if errors.Is(err, repl.ErrCanceled) { continue }
//		handle(line)
//	}
package repl
