// Package sql reads golars DataFrames from any database/sql source.
//
// # Quick start
//
//	import (
//		_ "modernc.org/sqlite"  // pure-Go SQLite
//		iosql "github.com/Gaurav-Gosain/golars/io/sql"
//	)
//
//	db, _ := sql.Open("sqlite", "file:data.db")
//	df, _ := iosql.ReadSQL(ctx, db, "SELECT * FROM trades WHERE volume > ?", 1000)
//	defer df.Release()
//	fmt.Println(df)
//
// Any database/sql-compatible driver works, including pure-Go options:
//   - modernc.org/sqlite for SQLite
//   - github.com/jackc/pgx/v5 for PostgreSQL (pgx/stdlib)
//   - github.com/microsoft/go-mssqldb for SQL Server
//   - github.com/go-sql-driver/mysql for MySQL
//
// Type mapping (driver-reported dbtype → golars dtype):
//   - INTEGER/INT/BIGINT/SMALLINT/TINYINT/SERIAL → i64
//   - REAL/DOUBLE/FLOAT/DECIMAL/NUMERIC         → f64
//   - BOOL/BOOLEAN                              → bool
//   - TIMESTAMP/DATETIME/DATE/TIME              → i64 (unix microseconds)
//   - anything else                             → str
//
// Null values are preserved via the arrow validity bitmap.
//
// # ADBC
//
// If you want Apache Arrow ADBC's zero-copy driver-side batching
// (postgres/snowflake/bigquery with native Arrow transport), use the
// ADBC Go bindings directly: they require cgo and are outside golars'
// pure-Go build:
//
//	import (
//		"github.com/apache/arrow-adbc/go/adbc"
//		"github.com/apache/arrow-adbc/go/adbc/drivermgr"
//		"github.com/Gaurav-Gosain/golars/dataframe"
//		"github.com/Gaurav-Gosain/golars/series"
//	)
//
// Build the ADBC driver, Database, Connection, Statement chain,
// execute the query to get an arrow.RecordReader, and feed each
// arrow.Record chunk to `series.New` + `dataframe.New`. All cgo
// concerns (driver .so on LD_LIBRARY_PATH, CGO_ENABLED=1) are the
// caller's to arrange; that's why we don't wrap it here.
package sql
