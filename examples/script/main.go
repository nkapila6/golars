// Shows how to drive a golars pipeline from a script file via the
// public script package. Equivalent to `golars run examples/script/demo.glr`
// but wired manually so you can see how to plug the script runner
// into any Executor.
package main

import (
	"fmt"
	"os"

	"github.com/Gaurav-Gosain/golars/script"
)

func main() {
	// A trivial Executor that just prints what it would do; swap in
	// any golars-driving handler (see cmd/golars/main.go's `handle`).
	runner := script.Runner{
		Exec: script.ExecutorFunc(func(line string) error {
			fmt.Println("→", line)
			return nil
		}),
		Trace: func(line string) { fmt.Println("▶", line) },
	}
	path := "examples/script/demo.glr"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	if err := runner.RunFile(path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
