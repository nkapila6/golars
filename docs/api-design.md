# API design

golars borrows the shape of polars' API. The surface is polars-style expressions with Go naming conventions. This document records the conventions so the API stays consistent as it grows.

## Naming

- Exported identifiers are `PascalCase`. Unexported are `camelCase`.
- Acronyms are capitalized consistently: `CSV`, `JSON`, `URL`, `ID`.
- Methods prefer verbs: `Select`, `Filter`, `Join`, `Sort`. Polars uses snake_case verbs; Go uses PascalCase verbs. Direct translation.
- Boolean predicates are prefixed `Is`: `IsNull`, `IsNotNull`, `IsUnique`, `IsInList`.
- Getters do not use `Get`: `df.Schema()`, `s.DType()`, `s.Len()`. This matches Go convention.

## Expressions

Polars in Python and Rust overloads operators. Go does not. We use method chaining:

| Polars (Python)                       | golars                                      |
|---------------------------------------|---------------------------------------------|
| `pl.col("a") + pl.col("b")`           | `expr.Col("a").Add(expr.Col("b"))`          |
| `pl.col("a") * 2`                     | `expr.Col("a").MulLit(2)`                   |
| `pl.col("a") > 3`                     | `expr.Col("a").GtLit(3)`                    |
| `pl.col("a").is_null()`               | `expr.Col("a").IsNull()`                    |
| `pl.col("a").alias("b")`              | `expr.Col("a").Alias("b")`                  |
| `pl.when(c).then(a).otherwise(b)`     | `expr.When(c).Then(a).Otherwise(b)`         |
| `pl.col("a").sum().over("g")`         | `expr.Col("a").Sum().Over("g")`             |

Arithmetic with literals uses a `Lit` suffix to keep method signatures concrete. Arithmetic between two expressions is the plain verb. This avoids the type-switching overhead of accepting `any`.

## Scalar kernels (`compute.*Lit`)

The `compute` package provides imperative kernels that work directly on `*series.Series`. The `*Lit` variants compare/arithmetic-with a scalar literal and skip the allocation a broadcast Series would require:

| Method                       | Behaviour                                          |
|-----------------------------|----------------------------------------------------|
| `compute.GtLit(ctx, s, 5)`   | mask where s > 5                                   |
| `compute.LtLit(ctx, s, 0)`   | mask where s < 0                                   |
| `compute.EqLit(ctx, s, 42)`  | mask where s == 42                                 |
| `compute.GeLit(ctx, s, 0.5)` | mask where s >= 0.5                                |

These accept any numeric Go literal type and coerce to the series dtype. Fast paths exist for int64 and float64; other dtypes fall back to a broadcast Series internally. Use them in hot loops where the expression compiler's overhead would dominate.

## Top-level re-exports

The root `golars` package re-exports the commonly used names so most user code imports a single package:

```go
import "github.com/Gaurav-Gosain/golars"

df, err := golars.ReadCSV(ctx, "data.csv")
out := df.Filter(golars.Col("x").GtLit(0)).
    GroupBy("k").
    Agg(golars.Col("v").Sum().Alias("v_sum"))
```

Deeper packages (`expr`, `lazy/plan`, `internal/hash`) are still importable for users who need them.

## Errors

- Every IO-bound or parse-bound operation returns `(T, error)`. No panics on user input.
- Pure-data operations that cannot fail given valid inputs return `T`. Invalid inputs (wrong dtype, nonexistent column) produce a descriptive error wrapped with `fmt.Errorf("golars: ...: %w", inner)`.
- We define sentinel errors for the common cases: `ErrColumnNotFound`, `ErrDTypeMismatch`, `ErrShapeMismatch`. User code can `errors.Is` on them.
- Expression build errors are deferred to `Collect()`. `expr.Col("a").Add(expr.Col("b"))` never fails at construction time, even if "a" does not exist in the target frame. The error surfaces when the plan is resolved.

## Context

Every operation that does IO, runs a plan, or might take non-trivial time accepts a `context.Context` as the first argument. Pure-data operations on already-materialized data do not. Examples:

- `df.ReadCSV(ctx, path)` takes ctx.
- `df.Filter(mask)` does not. Filter is in-process and fast.
- `lf.Collect(ctx)` takes ctx. Collect runs the plan.
- `lf.GroupBy("k")` does not. GroupBy is a plan builder.

The rule is: if a call can run arbitrary user-supplied IO, a plan, or a potentially long-running compute stage, it takes a context. Builder calls do not.

## Options

For operations with many optional parameters (ReadCSV, Join, GroupByDynamic), we use functional options:

```go
df, err := golars.ReadCSV(ctx, "data.csv",
    golars.WithDelimiter(','),
    golars.WithHasHeader(true),
    golars.WithNullValues([]string{"", "NA"}),
)
```

Options are functions with typed constructors. Option types are scoped to the operation (CSVOption, JoinOption) so the compiler enforces correct combinations.

## IO packages

Each supported file format lives in its own package so programs that only need one format don't pull the rest:

```
io/csv       // RFC 4180 CSV
io/parquet   // Apache Parquet via pqarrow
io/ipc       // Arrow IPC (feather)
io/json      // JSON array-of-objects, object-of-arrays, and NDJSON
io/sql       // database/sql bridge (any pure-Go driver)
```

Each package exposes `Read` (from `io.Reader`), `ReadFile`, and where it makes sense `ReadURL` (net/http-backed) and `ReadString`. Writers are symmetric: `Write`, `WriteFile`. The URL loaders accept `WithHTTPClient` so tests can inject a custom transport and production code can wire retry/auth middleware.

JSON type inference promotes numeric columns like polars: mixed int/float → float64; mixed anything/string → string. NaN and Inf round-trip. Nulls in input become null bitmap entries.

`io/sql` is the pragmatic pure-Go path for databases: plug in any `database/sql`-compatible driver (pgx, modernc.org/sqlite, go-sql-driver/mysql, go-mssqldb) and get a typed DataFrame. `ReadSQL` is eager; `NewReader` streams `WithBatchSize(n)` rows for result sets that exceed memory. Null values are preserved via the arrow validity bitmap. Apache ADBC is deliberately not wrapped: its drivers require cgo, which breaks the pure-Go invariant; the nested demo in `examples/sql/` shows the integration pattern with SQLite.

## Scripting (`script/` + `.glr` files)

`script.Runner` runs a tiny pipe-style language against any `Executor`. One statement per line, `#` for comments, the leading `.` on each command is optional:

```glr
# examples/script/demo.glr
load data/trades.csv
filter volume > 100
groupby symbol amount:sum:total
sort total desc
show
```

`cmd/golars` is the reference host: `golars run path.glr` runs a file one-shot and exits, `.source path.glr` runs one inline from the REPL. Third-party programs plug in via `script.ExecutorFunc`.

Multi-source: `load PATH as NAME` stages a frame in a registry without promoting it to focus; `use NAME` promotes it, parking the prior focus under its own name for later reuse; `join PATH|NAME on KEY` consumes a staged frame by name before trying it as a path. See the full language reference in [`docs/scripting.md`](scripting.md). A Tree-sitter grammar + highlight queries ship at [`editors/tree-sitter-golars/`](../editors/tree-sitter-golars/) for editor integrations.


## Nullability and zero values

Go has no null. Arrow has validity bitmaps. The API presents null the same way polars does:

- `s.Get(i)` returns `(value, valid bool)` for primitive dtypes.
- `s.GetStr(i)` returns `("", false)` for null.
- `s.IsNull()` returns a boolean mask Series.
- Aggregations skip nulls by default. `Sum` over `[1, null, 2]` is 3.
- Comparison operators produce null when either side is null. This matches SQL and polars.

## Iteration

golars does not encourage row-wise iteration. The idiomatic shape of a program is:

```go
result := df.
    Filter(golars.Col("price").GtLit(0)).
    WithColumns(
        golars.Col("price").Mul(golars.Col("qty")).Alias("total"),
    ).
    GroupBy("region").
    Agg(golars.Col("total").Sum())
```

Row iteration is available as `df.Rows()` returning a `RowIter` for cases where it is truly needed (writing to a non-columnar sink, debugging), but it is slow by design and documented as such.

## Versioning

We follow semver. Before v1.0.0 the API is unstable by convention (minor version bumps may break). After v1.0.0 we commit to semver strictly. Deprecations are marked with `// Deprecated:` comments and persist for at least one minor version before removal.

## What we do not export

- Concrete struct fields on `DataFrame`, `Series`, `LazyFrame`. All access is through methods.
- The plan and physical plan node types. They live under `lazy/plan` and `lazy/physical` but the API surface is what you build through the fluent `LazyFrame` API. Direct plan construction is not supported.
- Internal hash table and pool types.
