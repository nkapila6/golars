# Roadmap

Phased delivery plan. Dates are deliberately absent: a phase ships
when it is tested, benchmarked, and documented.

Live throughput ratios vs polars 1.39 live in
[`bench/polars-compare/`](../bench/polars-compare/). Numbers here
would drift; the bench harness is authoritative.

## Status

| Phase | State |
|---|---|
| 0. Foundations | done |
| 1. Eager DataFrame MVP | done |
| 2. Expression API + lazy + optimizer | done |
| 3. Streaming engine | MVP done; parallel source and hash-exchange pending |
| 4. SQL and temporal | subset SQL shipped; temporal kernels pending |
| 5. Polish and ecosystem | not started |

Correctness gates checked on every change:

- `go test ./...`
- `go test -tags noasm ./...`
- `GOEXPERIMENT=simd go test ./...`
- `go test -race ./compute/ ./dataframe/ ./lazy/ ./stream/ ./series/ ./eval/`
- `go vet ./...`
- Cross-compiles to linux/amd64, linux/arm64, darwin/arm64, windows/amd64
- `CGO_ENABLED=0 go build ./...`

## Phase 0: Foundations

Package scaffolding, arrow-go dependency, dtype model, Schema, Series
skeleton, DataFrame skeleton, test infrastructure.

Exit criteria:

- Round-trip every primitive dtype through Series without data loss
- Golden-file test harness in place
- `go test -tags noasm ./...` green (arrow-go fallback path)

## Phase 1: Eager DataFrame MVP

Dtypes: bool, int8/16/32/64, uint8/16/32/64, float32/64, utf8, binary,
date, datetime (ms, us, ns), duration, null.

Kernels: arithmetic, comparison, logical, cast, fill_null, is_null,
is_not_null, unique, value_counts, n_unique.

DataFrame operations: Select, Filter, WithColumn(s), Rename, Drop,
Sort, SortBy, Head, Tail, Slice, Sample, HStack, VStack, Concat,
Unique.

Aggregations: Sum, Mean, Min, Max, Std, Var, Count, Median, Quantile,
First, Last, NUnique.

Group-by: hash groupby with partition-parallel aggregation and merge.
Primitive and utf8 keys, multiple keys, multiple aggregates.

Joins: inner, left, outer, cross, semi, anti, asof. Hash join with
parallel build and probe.

IO: CSV, Parquet, Arrow IPC, JSON/NDJSON read and write.

Parallelism: every op accepts chunked input and processes chunks
through a worker pool sized to `GOMAXPROCS`.

## Phase 2: Expression API and lazy execution

Expression AST: Col, Lit, Cast, Alias, BinaryOp, UnaryOp, FunctionNode
(string/temporal/list/math), Agg, When/Then/Otherwise, Over, Rolling.

Logical plan nodes: Scan, Projection, WithColumns, Filter, Aggregate,
Join, Sort, Slice, Rename, Drop, Union (partial), Distinct, Explode
(planned), Pivot, Unpivot.

Optimizer passes (all wired):

1. Simplify (constant folding, boolean simplification, algebraic id)
2. Type coercion
3. CSE
4. Slice pushdown
5. Predicate pushdown (into scan for Parquet predicate pushdown)
6. Projection pushdown

Pending: join reordering with cardinality stats.

Executor: pull-based eager evaluation, plus the streaming executor.
Both share kernels. `LazyFrame.ExplainString` and `ExplainTreeString`
expose the plan at each stage.

Typed-column facade (`expr.C[T]`, `expr.Int`, `expr.Float`, etc.)
lives alongside the untyped API and produces identical plans.

## Phase 3: Streaming engine

Morsel-driven parallel executor. See [parallelism.md](parallelism.md)
for the detailed design.

Features:

- Source, Stage, Sink operator kinds
- Morsels as `arrow.Record` batches with bounded row count
- Buffered channels between stages for back-pressure
- Order-preserving parallel map stage
- Hybrid execution: streaming-friendly prefix + pipeline breakers
- Cancellation via context

Pending:

- Parallel source for chunked-arrow files
- Hash exchange for partition-parallel group-by and join
- Spill-to-disk sort via external merge
- Out-of-core join

## Phase 4: SQL and temporal

Subset SQL shipped: SELECT (DISTINCT), FROM, WHERE, GROUP BY,
ORDER BY, LIMIT. See [scripting.md](scripting.md) for the grammar
and [mcp.md](mcp.md) for the MCP `sql` tool.

Pending:

- JOIN, HAVING, CTE, window functions, subqueries in SQL
- Temporal kernels: truncate, offset_by, date_range, timezone
  conversion (`time.LoadLocation`), strftime, strptime
- `df.upsample` ships with the scalar-interval path (`ns`..`w`);
  calendar units (`mo`, `y`) still pending

## Phase 5: Polish and ecosystem

- Nested Series runtime: broader kernel coverage on List / Struct /
  FixedList. `df.Explode` and `df.Unnest` ship with dtype detection
  and null propagation; follow-up work: expression-level list/struct
  ops (`list.sum`, `struct.field`).
- Categorical and Enum dtypes with shared dictionaries
- Object-store scans (S3, GCS, Azure) via `gocloud.dev/blob`
- Delta Lake and Iceberg read paths
- Excel/Avro adapters
- Expanded SIMD coverage beyond the current AVX2/NEON kernel set
- `database/sql` write path (read path exists)

## Deferred indefinitely

- GPU execution. Out of scope for a pure-Go project.
- Python bindings. If demand appears, a separate repo can wrap golars
  over gRPC or CFFI.
- Query languages other than SQL (Substrait consumer). Revisit after
  the SQL surface stabilises.

## Open design threads

Standalone decisions; pick, sequence, or defer independently.

### A. Memory model

Every `Series` / `DataFrame` currently requires `defer x.Release()`.
Correct, zero-leak, but heavy for REPL-style use. Three viable
directions:

1. Finalizer-backed auto-release via `runtime.AddCleanup`. No API
   change. Releases happen on GC, which leaks memory under allocation
   pressure.
2. Arena / session scope: `session := golars.NewSession(); defer
   session.Close()`. Every Series/DataFrame allocated through the
   session releases on close. Ports cleanly to REPL (one session per
   statement) and scripts (one session per `.glr` file).
3. Builder-to-owned-handle: remove `Release` from the public surface;
   wrap the refcount behind a handle only ops can decrement. Biggest
   change; cleanest resulting surface.

Current thinking: combine 2 with 1 as a safety net. Pick before
tackling nested dtypes.

### B. Dtype gaps vs polars

Shipped: bool, int8/16/32/64, uint8/16/32/64, float32/64, date,
datetime (s/ms/us/ns, optional tz), duration, time (repr only), utf8,
binary, list, fixed-list, struct, null.

Gaps: Float16, Decimal, Int128, Categorical, Enum, Object, Unknown;
and a `series.FromTime` constructor for the existing Time dtype.

Tractable next steps, ranked by effort:

- `series.FromTime` wraps `arrow.FixedWidthTypes.Time64ns`. Minutes.
- `series.FromDecimal128`: arrow-go has `Decimal128`; needs dtype
  constructor + format code + From builder. Hours.
- Categorical / Enum: dictionary-encoded; touches every kernel that
  reads `*array.String`. Warrants its own milestone.
- Int128, Float16: low value for DataFrame workloads. Defer.
- Object, Unknown: polars-internal escape hatches. Not needed.

### C. I/O gaps

Shipping: CSV, JSON, NDJSON, Parquet, Arrow IPC/Feather, SQL
(read path).

Gap inventory, ranked by usefulness:

- Avro (container + object-container). `github.com/linkedin/goavro`
  is pure-Go. Medium effort.
- Delta Lake. Big. `delta-go` covers readers.
- Iceberg. Pure-Go `iceberg-go` exists.
- Excel / ODS. `xuri/excelize` handles xlsx.
- PyArrow Datasets (partitioned Parquet directories): glob + concat
  over the existing Parquet reader.

### D. Distributed compute

Single-node streaming is Phase 3: done for eager paths, incomplete
for groupby/join parallel sources. Distributed compute is not
scoped. Two plausible shapes:

1. Shuffle-based. Split `Scan -> Shuffle -> Aggregate` across workers;
   Arrow IPC over TCP for shuffle payloads. Each worker runs the
   existing streaming engine on its partition.
2. Plan-serialization. `lazy.Node` + `expr.Expr` are exported structs
   already; encode to gob or protobuf and ship to worker pools. No
   new runtime, just a shipping protocol.

Neither is blocking for 1.0. Worth a dedicated RFC.

### E. Behavioural parity backlog

Polars tests ported: slice, shift, unique, rename, fill_null,
drop_nulls, take, describe, select, with_columns. Still queued:

- Predicates: `test_is_in`, `test_is_null`, `test_is_sorted`
- Reshape: `test_explode`, `test_pivot`, `test_unpivot`
- Join: `test_cross_join`, `test_join_asof` (asof blocked on sorted
  metadata)
- Aggregation: `test_value_counts`, `test_mode`
- Samplers: `test_sample`, `test_random`

Low-cost wins: `is_in`, `is_null`, `is_sorted`, `value_counts`,
`mode`. Each is a morning's work.
