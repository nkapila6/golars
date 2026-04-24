// Package script runs a very small pipe-style language against any
// backend that implements the Executor interface. A golars script is
// a plain-text file (suggested extension: .glr) where each line is
// one command applied to the frame's current pipeline.
//
//	# comments start with '#'
//	load data/trades.csv
//	filter volume > 100
//	groupby symbol amount:sum:total
//	sort total desc
//	limit 10
//	show
//
// The language is intentionally tiny: one line = one command. There
// are no variables, no control flow, no expression language beyond
// what filter/groupby/etc. already accept. The aim is "drop your
// REPL session into a file and it runs".
//
// Commands match the golars REPL dot-commands (.load, .filter, etc.)
// but the leading "." is optional in scripts. A line like "load X"
// and ".load X" are equivalent; pick whichever reads better.
//
// # Running
//
//	golars run my.glr          # run a script and exit
//	.source my.glr             # run a script from inside the REPL
//
// Hosts plug in by satisfying Executor.Run.
package script
