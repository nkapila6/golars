# CLAUDE.md

Claude Code-specific guidance for contributing to **golars**. Start
with [AGENTS.md](AGENTS.md) for the general project brief; this file
layers on top with Claude-specific workflow tips.

## Quick orientation

- Primary working directory: `/home/gaurav/dev/golars`
- Not a git repository from Claude Code's perspective, but the user
  commits manually - don't touch git state unless asked.
- `.claude/` lives under `~/.claude/projects/-home-gaurav-dev-golars/`;
  memory there persists across sessions.

## Tools that exist just for you

- **golars LSP** (`cmd/golars-lsp`) - for editing `.glr` files. Not
  usually relevant to code-writing sessions.
- **golars MCP** (`cmd/golars-mcp`) - expose golars as tools in
  Claude Desktop. If the user asks about integrating golars into a
  Claude workflow, point them here.
- **SKILLS.md** lists the user-invocable skills.

## Tasks that work well

1. **Add a new kernel**: skeleton in `series/`, expose in
   `expr/func.go`, wire `eval/eval_func.go`, add parity test against
   polars, rerun `bench/polars-compare/compare.py`.
2. **Close a perf gap**: profile with `go test -cpuprofile`, then
   inspect `go tool pprof -top`. Most wins come from eliminating an
   allocation in a hot loop or moving to a direct-buffer Build.
3. **Propagate a new scripting command**: see the checklist in
   `AGENTS.md`. Easy to miss a spot - always grep for one of the
   existing commands (`unique`, `cast`, `rename`) and touch every
   file that mentions it.

## Testing cadence

After any non-trivial change:

```sh
go test ./...
GOEXPERIMENT=simd go test ./...
```

If you touched `compute/`, `eval/`, `series/`, `dataframe/`, `lazy/`,
or `stream/`:

```sh
go test -race ./compute/ ./eval/ ./series/ ./dataframe/ ./lazy/
```

If you touched anything hot:

```sh
go build -o bin/golars-bench ./cmd/bench/
GOEXPERIMENT=simd go build -o bin/golars-bench-simd ./cmd/bench/
cd bench/polars-compare && uv run python compare.py
```

## Known perf sensitivities

- Allocation inside hot kernels: always check pprof for `mallocgc`.
- Per-goroutine overhead on small N: the bench shows parallel sort
  loses to serial below 64K for float64 and 256K for int64. Keep
  cutoffs in `parallelRadixCutoff` / `parallelFloatRadixCutoff`.
- The scan predicate is applied _before_ the projection (see
  `lazy/execute.go:executeScan`). If you change that ordering,
  predicate columns may disappear before the filter runs.

## Things to avoid

- Don't add `//go:generate` without asking - the codebase is
  hand-maintained.
- Don't pull in new dependencies without asking. Current deps are
  tight: arrow-go, bubbletea, lipgloss, textinput, cobra, fang, pgx
  (SQL driver demo), and the SIMD experiment's std `simd/archsimd`.
- Don't call `runtime.GC()` except in tests where the cache
  finalizer is the thing under test.
- Don't bypass `poolingMem` on allocations in hot kernels.
- Don't emit emojis or em-dashes in any code or doc the user will
  read. It's a hard rule.

## Memory policy

The user's auto-memory lives at
`~/.claude/projects/-home-gaurav-dev-golars/memory/MEMORY.md` and a
handful of typed notes. Honour it: the user has already written
preferences there.

Save new memories when the user reveals a non-obvious preference, a
project-level decision, or a reference to an external system. Skip
the obvious (file paths, style conventions already in AGENTS.md).
