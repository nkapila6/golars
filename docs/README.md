# golars design docs

This folder holds the architecture and design documentation for golars, a pure-Go DataFrame library modeled on [polars](https://github.com/pola-rs/polars).

These docs are for contributors. User-facing documentation lives at the repository root and on pkg.go.dev.

## Index

1. [Architecture overview](architecture.md). Layered component map and how data flows through the system.
2. [Roadmap](roadmap.md). Phased delivery plan, from eager MVP through streaming engine and SQL.
3. [Parallelism model](parallelism.md). How golars uses goroutines and channels for morsel-driven execution.
4. [Memory model](memory-model.md). Buffer ownership, reference counting, GC discipline.
5. [API design](api-design.md). Naming conventions and how polars idioms map to Go.

## Non-goals

- A drop-in replacement for polars' Python or Rust API. We borrow the shape of the API, not the literal surface.
- Bindings to the Rust polars runtime. golars is an independent implementation.
- cgo in any transitive dependency. Cross-compilation must remain a single command.
