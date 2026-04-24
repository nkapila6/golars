# Cookbook

End-to-end recipes for common tasks. Every snippet compiles and
assumes `import "github.com/Gaurav-Gosain/golars"` plus whatever
sub-package a particular line needs.

## Typed columns for compile-time literal checks

The `expr` package ships a typed facade (`expr.C[T]`, `expr.Int`,
`expr.Float`, `expr.Str`, `expr.Bool`) that lets Go infer literal
types from method arguments, eliminating the `expr.Lit(int64(...))`
boilerplate:

```go
import "github.com/Gaurav-Gosain/golars/expr"

qty := expr.Int("qty")
price := expr.Float("price")

out, _ := lazy.FromDataFrame(df).
    Filter(expr.All(qty.Gt(2), price.Lt(50))).
    WithColumns(
        price.MulCol(qty.CastFloat64()).As("total").Expr,
        qty.Between(2, 5).Alias("in_range"),
    ).
    Collect(ctx)
```

The runtime plan is identical to the untyped `expr.Col("qty").
GtLit(int64(2))` form. Passing a string literal to an int-typed
column fails at build time rather than panicking at evaluation.
See `examples/*/generic/` for side-by-side comparisons.

## List and struct namespaces

Expression-level helpers mirror polars' `.list.*` and `.struct.*`:

```go
import "github.com/Gaurav-Gosain/golars/expr"

lazy.FromDataFrame(df).Select(
    expr.Col("tags").List().Len().Alias("tag_count"),
    expr.Col("payload").Struct().Field("x").Alias("x"),
    expr.Col("csv").Str().SplitExact(",").List().Get(0).Alias("first"),
).Collect(ctx)
```

Supported list reducers: `Len`, `Sum`, `Mean`, `Min`, `Max`, `First`,
`Last`, `Get(idx)`, `Contains(needle)`, `Join(sep)` (string lists).
Supported struct ops: `Field(name)`.

## Unnest / explode / upsample

Unnest a struct column:

```go
out, _ := df.Unnest(ctx, "payload")
// struct {x:i64, y:str} becomes two top-level cols `x` and `y`.
```

Explode a list column (null and empty lists become a single null row):

```go
out, _ := df.Explode(ctx, "tags")
// [[a, b, c], [], NULL, [d]] produces 3 + 1 + 1 + 1 = 6 rows.
```

Upsample a sorted timestamp column to a dense grid:

```go
out, _ := df.Upsample(ctx, "ts", "1d")
// rows spaced > 1d apart get filled with null-valued neighbours.
```

Accepted intervals: `ns`, `us`, `ms`, `s`, `m`, `h`, `d`, `w`.
Calendar units (`mo`, `y`) are rejected.

## Pretty-print a logical plan

```go
fmt.Println(lazy.ExplainTree(plan.Plan()))
// SORT [total desc]
// └── AGG keys=[dept] aggs=[...]
//     └── FILTER (col("salary") > 75)
//         └── SCAN df
```

`lazy.ExplainTreeASCII` swaps the box-drawing glyphs for ASCII
fallbacks. `lf.ExplainTree()` is the full three-section report
(logical, optimiser, optimised) rendered as a tree.

## Read a CSV, filter, write Parquet

```go
df, _ := golars.ReadCSV("trades.csv")
defer df.Release()

out, _ := golars.Lazy(df).
    Filter(golars.Col("volume").GtLit(int64(100))).
    Collect(ctx)
defer out.Release()

golars.WriteParquet(out, "heavy_trades.parquet")
```

## Group + aggregate multiple columns in one pass

```go
agg, _ := golars.Lazy(df).
    GroupBy("symbol").
    Agg(
        golars.Sum("qty"),
        golars.Mean("price").Alias("avg_price"),
        golars.Max("price").Alias("hi"),
    ).
    Sort("qty_sum", true).
    Collect(ctx)
```

golars detects that every agg targets the same column in a single
bucket and fuses them through `groupby_fused.go`, so four aggregations
cost one scan.

## Join a CSV against a Parquet lookup table

```go
trades, _ := golars.ReadCSV("trades.csv")
defer trades.Release()
lookup, _ := golars.ReadParquet("symbols.parquet")
defer lookup.Release()

out, _ := golars.Lazy(trades).
    Join(golars.Lazy(lookup), []string{"symbol"}, golars.InnerJoin).
    Collect(ctx)
```

## Scan (lazy I/O) plus predicate pushdown

```go
import iocsv "github.com/Gaurav-Gosain/golars/io/csv"

// iocsv.Scan returns a LazyFrame that opens the file only when
// Collect runs. Combined with Filter + Select, the optimiser pushes
// the projection down through the scan.
lf := iocsv.Scan("/tmp/huge.csv").
    Filter(golars.Col("region").EqLit("us")).
    Select(golars.Col("symbol"), golars.Col("price"))

for batch, err := range lf.IterBatches(ctx) {
    if err != nil { log.Fatal(err) }
    defer batch.Release()
    // stream-process each batch here
}
```

## Null handling: drop, fill, or flag

```go
clean, _ := golars.Lazy(df).DropNulls("price", "qty").Collect(ctx)
filled, _ := golars.Lazy(df).FillNull(int64(0)).Collect(ctx)

mask, _ := df.AnyNullMask(ctx)
defer mask.Release()
// `mask` is a boolean Series you can plug back into Filter to flag
// bad rows without dropping them.
```

## Select by dtype or name predicate

```go
import "github.com/Gaurav-Gosain/golars/selector"

numericOnly, _ := df.SelectBy(selector.Numeric())
noTimes := df.DropBy(selector.EndsWith("_ts"))

// Combinators: intersect, union, minus.
usdCols, _ := df.SelectBy(selector.Intersect(
    selector.Float(),
    selector.StartsWith("price_usd"),
))
```

## Cross-language with Arrow

```go
rec := df.ToArrow()          // arrow.RecordBatch
tbl := df.ToArrowTable()     // arrow.Table (multi-chunk)
roundtrip, _ := dataframe.FromArrowTable(tbl)
```

Both sides are Arrow IPC format-compatible. Write with `io/ipc.Write`
and read in PyArrow, pola.rs, DuckDB, or any other Arrow-aware tool
without format conversion.

## String munging

```go
out, _ := df.Apply(func(s *series.Series) (*series.Series, error) {
    if s.Name() != "email" { return s.Clone(), nil }
    return s.Str().Before("@")
})
```

`.Str().Before` / `.After` / `.SplitNth` cover the common parsing
cases; `.SplitWide` returns multiple Series so you can stitch them
into a DataFrame with extra columns.

## Cache an intermediate pipeline

```go
base := golars.Lazy(df).
    Filter(golars.Col("active").EqLit(true)).
    Cache()

// Two downstream pipelines share the same filtered base.
top, _ := base.Sort("score", true).Head(10).Collect(ctx)
flag, _ := base.Filter(golars.Col("score").LtLit(0.5)).Collect(ctx)
```

Cache memoises the first Collect result; subsequent collects reuse
it. The cached frame is released automatically when the cache's
LazyFrame handle is garbage-collected.

## When / then / otherwise

```go
out, _ := golars.Lazy(df).
    Select(golars.When(golars.Col("age").Gt(golars.Lit(18))).
        Then(golars.Lit("adult")).
        Otherwise(golars.Lit("minor")).
        Alias("category")).
    Collect(ctx)
```

Mixed numeric dtypes are promoted (int then + float otherwise -> float64 out).
Null cond values are treated as false (polars semantics).

## Rolling operations

```go
// Rolling sum/mean/min/max/std/var with a fixed window.
out, _ := golars.Lazy(df).
    Select(
        golars.Col("price").RollingMean(30, 1).Alias("ma30"),
        golars.Col("price").RollingStd(30, 5).Alias("vol30"),
    ).Collect(ctx)
```

Second argument is `min_periods` (0 = require full window). Int64 inputs
with no nulls take a SIMD-friendly O(n) slide (two-phase warmup +
4-way unrolled step).

## Regex on strings

```go
// Boolean mask for regex hits.
mask, _ := series.FromString("s", []string{"a1", "xx", "b22"}, nil).
    Str().ContainsRegex(`\d+`)

// Extract first capture group.
ids, _ := emails.Str().Extract(`@([a-z.]+)$`, 1)

// Count matches per row.
counts, _ := tokens.Str().CountMatchesRegex(`\w+`)
```

## Pivot (long -> wide)

```go
// Mirror of polars' df.pivot(index="id", on="cat", values="v").
wide, _ := df.Pivot(ctx, []string{"id"}, "cat", "v", dataframe.PivotSum)
```

Aggregators: `PivotFirst`, `PivotSum`, `PivotMean`, `PivotMin`, `PivotMax`, `PivotCount`.

## Window functions with `.Over(...)`

```go
// Per-group total broadcast back to every row.
out, _ := golars.Lazy(df).
    Select(golars.Col("revenue").Sum().Over("region").Alias("region_total")).
    Collect(ctx)

// Per-group rank.
ranked, _ := golars.Lazy(df).
    Select(golars.Col("score").Rank("dense").Over("cohort").Alias("rank_in_cohort")).
    Collect(ctx)
```

## Forward / backward fill + NaN

```go
// Replace every NaN with 0 in float columns (integer cols pass through).
filled, _ := golars.Lazy(df).FillNan(0).Collect(ctx)

// Carry the last non-null value forward through consecutive nulls.
// Pass limit=3 to stop after three consecutive fills; limit=0 means unlimited.
ff, _ := golars.Lazy(df).ForwardFill(0).Collect(ctx)
bf, _ := golars.Lazy(df).BackwardFill(0).Collect(ctx)

// Per-column variant via Expr:
out, _ := golars.Lazy(df).
    WithColumns(golars.Col("price").ForwardFill(0).Alias("price")).
    Collect(ctx)
```

## Reshape: transpose, unpivot, partition

```go
// Transpose: each input column becomes a row; output value type is
// the promoted float64 (polars' object-dtype fallback is not yet
// supported, so non-numeric inputs error).
wide, _ := df.Transpose(ctx, "column", "row")

// Unpivot (melt): turn value columns into two long-form columns:
//   variable (the original column name) + value.
long, _ := df.Unpivot(ctx, []string{"id"}, nil /* default: all non-id */)

// Partition: one DataFrame per distinct key tuple; ordered by first
// appearance. Caller releases each partition.
parts, _ := df.PartitionBy(ctx, "region", "symbol")
for _, p := range parts {
    defer p.Release()
}
```

## Top-K / Bottom-K / Pipe

```go
topSellers, _ := df.TopK(ctx, 10, "revenue")
worstLatency, _ := df.BottomK(ctx, 5, "p99_ms")

// Pipe keeps chained code flat:
out, _ := df.Pipe(func(d *DataFrame) (*DataFrame, error) {
    return d.Filter(ctx, mask)
})
```

## Stats: skew, kurtosis, corr, cov, approx_n_unique

```go
sk, _ := col.Skew()                    // polars default (biased)
sku, _ := col.SkewUnbiased()           // scipy bias=False
kk, _ := col.Kurtosis()                // excess kurtosis

c, _ := a.PearsonCorr(b)               // Pearson r
cov, _ := a.Covariance(b, 1)           // ddof=1

corrMat, _ := df.Corr(ctx)             // k-by-k frame
covMat, _ := df.Cov(ctx, 1)

approx, _ := col.ApproxNUnique()       // HLL estimate
```

## Extra math helpers

```go
// Trig + hyperbolic family: atan2, cbrt, sinh/cosh/tanh, log1p, expm1,
// radians/degrees, arccos/arcsin/arctan, cot, arcsinh/arccosh/arctanh.
radians, _ := col.Radians()
y, _ := colY.Arctan2(colX)
```

## Coalesce, concat_str, ones/zeros/int_range

```go
picked, _ := golars.Lazy(df).
    Select(golars.Coalesce(golars.Col("primary"), golars.Col("fallback"))).
    Collect(ctx)

joined, _ := golars.Lazy(df).
    Select(golars.ConcatStr("-", golars.Col("sym"), golars.Col("year"))).
    Collect(ctx)

// Build Series out of thin air:
ids, _ := golars.Lazy(df).
    WithColumns(golars.IntRange(0, 100, 1).Alias("idx")).
    Collect(ctx)
```

## Arrow IPC streaming (cross-language)

```go
sw, _ := golars.NewIPCStreamWriter(conn, firstBatch)
for batch := range batches {
    sw.Write(ctx, batch)
}
sw.Close()

// Consumer side (also polars/pyarrow/DuckDB-compatible):
sr, _ := golars.NewIPCStreamReader(conn)
defer sr.Close()
for batch, err := range sr.Iter(ctx) {
    if err != nil { log.Fatal(err) }
    process(batch)
    batch.Release()
}
```

## Row-wise (horizontal) reductions

```go
// Append a column that sums three others on a per-row basis.
withTotal, _ := golars.Lazy(df).
    SumHorizontal("total", "q1", "q2", "q3").
    Collect(ctx)
defer withTotal.Release()

// Or compute the reduction directly as a standalone Series.
total, _ := golars.SumHorizontal(ctx, df, "q1", "q2", "q3")
defer total.Release()
```

Variants: `SumHorizontal`, `MeanHorizontal`, `MinHorizontal`,
`MaxHorizontal`, `AllHorizontal`, `AnyHorizontal`. Omit the column
list to span every numeric (or boolean) column. Null handling
defaults to `IgnoreNulls`; pass `dataframe.PropagateNulls` on the
frame-level method for polars' strict semantics.

## One-row frame-level aggregates

```go
sums, _ := df.SumAll(ctx)     // one row, one column per numeric input
means, _ := df.MeanAll(ctx)
counts, _ := df.CountAll(ctx) // counts non-nulls for every column
nulls, _ := df.NullCountAll(ctx)
```

These mirror polars' `df.sum()`, `df.mean()`, `df.count()`. They are
convenient for dashboards, describe-style summaries, and streamed
ETL checkpoints.

## Lazy scans (pushdown-friendly I/O)

```go
lf := golars.ScanCSV("huge.csv").
    Filter(golars.Col("region").EqLit("us")).
    Select(golars.Col("symbol"), golars.Col("price"))

out, _ := lf.Collect(ctx)
```

Every format has a scan entry point: `ScanCSV`, `ScanParquet`,
`ScanIPC`, `ScanJSON`, `ScanNDJSON`. Compared to `Read*`, a scan
defers opening the file until `Collect` so the optimiser can
push projections + filters into the reader.

## Rank and percent-change

```go
r, _ := golars.Lazy(df).
    Select(
        golars.Col("score").Rank("dense").Alias("rank"),
        golars.Col("score").PctChange(1).Alias("delta"),
    ).
    Collect(ctx)
```

## Apply a custom Go function

```go
out, _ := df.Apply(func(s *series.Series) (*series.Series, error) {
    switch s.DType().String() {
    case "i64":
        return s.ApplyInt64(func(v int64) int64 { return v * 2 })
    case "str":
        return s.Str().Upper()
    }
    return s.Clone(), nil
})
```

## REPL / scripting quickies

Inside `golars` or in a `.glr` file:

```
load data/trades.csv
filter volume > 100
with_row_index row
cast price f64
fill_null 0
rename volume as qty
sum qty
write out.parquet
```

Scalar-only prints: `.sum COL`, `.mean COL`, `.min COL`, etc. write
one-line results instead of a table, convenient for quick spot
checks.

## Per-language equivalents

| polars (Python) | golars (Go) |
|---|---|
| `pl.read_csv(p)` | `golars.ReadCSV(p)` |
| `df.filter(pl.col("x") > 5)` | `df.Filter(ctx, mask)` or `lazy` with `golars.Col("x").GtLit(5)` |
| `df.group_by("k").agg(pl.sum("v"))` | `df.GroupBy("k").Agg(ctx, []expr.Expr{expr.Col("v").Sum()})` |
| `df.unique()` | `df.Unique(ctx)` |
| `df.sample(n=10)` | `df.Sample(ctx, 10, false, seed)` |
| `df.with_row_index()` | `df.WithRowIndex("index", 0)` |
| `pl.col("s").str.to_uppercase()` | `s.Str().Upper()` |
| `df.select(cs.numeric())` | `df.SelectBy(selector.Numeric())` |
| `pl.sum_horizontal("a", "b")` | `golars.SumHorizontal(ctx, df, "a", "b")` |
| `df.fill_nan(0)` | `lf.FillNan(0)` |
| `df.fill_null(strategy="forward")` | `lf.ForwardFill(0)` |
| `df.fill_null(strategy="backward")` | `lf.BackwardFill(0)` |
| `df.top_k(10, by="x")` | `df.TopK(ctx, 10, "x")` |
| `df.transpose()` | `df.Transpose(ctx, "column", "row")` |
| `df.unpivot(index=["id"])` | `df.Unpivot(ctx, []string{"id"}, nil)` |
| `df.partition_by("k")` | `df.PartitionBy(ctx, "k")` |
| `df.corr()` | `df.Corr(ctx)` |
| `pl.coalesce(...)` | `golars.Coalesce(...)` |
| `pl.concat_str(..., sep)` | `golars.ConcatStr(sep, ...)` |
| `pl.int_range(0, n)` | `golars.IntRange(0, int64(n), 1)` |
| `pl.when(p).then(a).otherwise(b)` | `golars.When(p).Then(a).Otherwise(b)` |
| `pl.col("x").rolling_sum(w)` | `golars.Col("x").RollingSum(w, 0)` |
| `pl.col("x").sum().over("k")` | `golars.Col("x").Sum().Over("k")` |
| `s.str.extract(pattern, i)` | `s.Str().Extract(pattern, i)` |
| `s.str.contains_regex(p)` | `s.Str().ContainsRegex(p)` |
| `df.pivot(index=, on=, values=)` | `df.Pivot(ctx, index, on, values, agg)` |
| `df.sum()` | `df.SumAll(ctx)` |
| `pl.scan_csv(p)` | `golars.ScanCSV(p)` |
| `pl.scan_parquet(p)` | `golars.ScanParquet(p)` |

See `docs/api-surface.md` for the full cross-reference.
