// Package sql is a tiny SQL frontend for golars. A Session holds a
// registry of tables (DataFrames) and compiles a subset of SQL into
// a lazy plan that runs through the existing optimiser.
//
// Supported grammar (case-insensitive):
//
//	SELECT [DISTINCT] col_list
//	FROM table_ref
//	[WHERE predicate]
//	[GROUP BY col_list]
//	[ORDER BY col_list [ASC|DESC]]
//	[LIMIT n]
//
// col_list:     col[, col...]  |  *  |  agg(col)[, ...]
// predicate:    col OP value [AND|OR ...]  where OP in =, !=, <, <=, >, >=
// table_ref:    table_name
// agg:          SUM | MIN | MAX | AVG | MEAN | COUNT | FIRST | LAST
//
// The frontend is hand-written - no parser generator is pulled in.
// Enough to drive `golars sql "SELECT ..."` from the CLI and to
// support `df.SQL("...")` programmatically. Extend as needs arise.
package sql
