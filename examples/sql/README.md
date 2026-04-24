# golars SQL example

Runnable demonstration of `github.com/Gaurav-Gosain/golars/io/sql`
loading a query result into a golars `*DataFrame` via a pure-Go SQLite
driver (`modernc.org/sqlite`).

This is a **nested Go module**: it has its own `go.mod` so the main
golars module doesn't pull `modernc.org/sqlite` and its ~220 MB of
transitive vendored code.

## Run

```sh
cd examples/sql
go run .
```

Expected output:

```
shape: (4, 5)
╭─────┬────────┬────────┬────────┬──────────────────╮
│ id  │ symbol │ price  │ volume │ ts               │
│ i64 │ str    │ f64    │ i64    │ i64              │
├─────┼────────┼────────┼────────┼──────────────────┤
│ 1   │ AAPL   │ 188.23 │ 100    │ 1744970400000000 │
│ 2   │ GOOG   │ 2750.5 │ null   │ 1744970401000000 │
│ 4   │ AAPL   │ 188.51 │ 75     │ 1744970403000000 │
│ 5   │ NVDA   │ 950    │ 200    │ 1744970404000000 │
╰─────┴────────┴────────┴────────┴──────────────────╯
loaded 4 rows × 5 columns

batch 1: 2 rows
batch 2: 2 rows
batch 3: 1 rows
```

## Tests

```sh
cd examples/sql
go test .
```

Runs seven integration tests against an in-process pure-Go SQLite
engine: round-trip with all dtypes + nulls, streaming 25 000 rows in
batches, custom batch sizes, empty result schemas, parameterised
queries, context cancellation, and timestamp micros round-trip.

## Drop-in alternatives

`modernc.org/sqlite` is one choice. `io/sql.ReadSQL` works with any
`database/sql`-compatible driver: PostgreSQL via `jackc/pgx/v5/stdlib`,
MySQL via `go-sql-driver/mysql`, SQL Server via `microsoft/go-mssqldb`,
etc. Swap `_ "modernc.org/sqlite"` for your driver of choice and the
rest of the code works unchanged.
