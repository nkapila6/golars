# vhs/

[VHS](https://github.com/charmbracelet/vhs) tape scripts that generate
every GIF embedded in the root README.

## Prerequisites

```sh
brew install vhs jq     # or go install github.com/charmbracelet/vhs@latest
```

A JetBrains Mono Nerd Font (or any monospace Nerd Font) gives the tapes
the glyph coverage they need. Any monospace font will still render; you
just lose a handful of decorative characters.

## Build

```sh
make -C vhs            # builds binaries + every gif into vhs/out/
make -C vhs gif-sql    # one at a time
make -C vhs clean      # drop generated gifs
```

The Makefile compiles fresh `golars` and `golars-mcp` binaries into
`../bin` and prepends that directory to `$PATH` during each `vhs` run,
so the gifs always match the current source.

## Layout

```
vhs/
├── _common.tape       shared theme / font / size settings
├── fixtures/          tiny CSV files the tapes read from
│   ├── people.csv
│   ├── trades.csv
│   ├── trades-v2.csv
│   └── pipeline.glr
├── out/               generated .gif files (checked into the repo)
├── hero.tape          top-of-README headline demo (~20s)
├── sql.tape           SQL frontend + output format switching
├── inspect.tape       schema / peek / stats
├── browse.tape        TUI browser with sort, filter, freeze
├── pipes.tape         composability with jq / awk / parquet round-trip
├── diff.tape          golars diff --key
├── convert.tape       csv → parquet → ndjson → arrow
├── profile.tape       golars explain --profile
├── script.tape        .glr scripting with `golars run`
├── mcp.tape           JSON-RPC drive of golars-mcp
└── doctor.tape        environment diagnostic
```

## Adding a new tape

1. Copy an existing tape as a starting point.
2. Open it with `Source vhs/_common.tape` so theme/size/font stay in sync.
3. Keep the demo under ~30 seconds; a GIF longer than that gets skimmed.
4. Add it to the README's asset table so readers can find it.
5. Run `make -C vhs gif-<name>` to verify.

## Notes on the settings

`_common.tape` fixes the window size at 1240x720 with 14pt text and the
Tokyo Night palette. Those numbers happen to produce a ~1.8 MB GIF for a
20-second session, which is small enough to embed inline without hurting
the README's load time. Bumping the FontSize or PlaybackSpeed past 1.0
inflates files quickly, so prefer lower values when editing.
