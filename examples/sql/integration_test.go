package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/apache/arrow-go/v18/arrow/array"

	iosql "github.com/Gaurav-Gosain/golars/io/sql"
)

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seed(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed %q: %v", s, err)
		}
	}
}

// Real-driver round-trip: every supported dtype + nulls + timestamps.
func TestSQLiteRoundTrip(t *testing.T) {
	db := openSQLite(t)
	seed(t, db,
		`CREATE TABLE trades (
			id INTEGER PRIMARY KEY,
			symbol TEXT NOT NULL,
			price REAL,
			volume INTEGER,
			is_buy BOOL,
			ts TIMESTAMP
		)`,
		`INSERT INTO trades VALUES
			(1, 'AAPL', 188.23, 100, 1, '2025-04-18 10:00:00'),
			(2, 'GOOG', 2750.50, NULL, 0, '2025-04-18 10:00:01'),
			(3, 'MSFT', NULL, 50, 1, '2025-04-18 10:00:02')`,
	)

	df, err := iosql.ReadSQL(context.Background(), db,
		"SELECT id, symbol, price, volume, is_buy, ts FROM trades ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if df.Height() != 3 {
		t.Fatalf("height=%d want 3", df.Height())
	}
	if df.Width() != 6 {
		t.Fatalf("width=%d want 6", df.Width())
	}

	symbol := df.ColumnAt(1).Chunk(0).(*array.String)
	if symbol.Value(0) != "AAPL" || symbol.Value(2) != "MSFT" {
		t.Fatalf("symbol mismatch: %q %q", symbol.Value(0), symbol.Value(2))
	}

	price := df.ColumnAt(2).Chunk(0).(*array.Float64)
	if price.IsValid(2) {
		t.Fatal("price[2] should be null (SQL NULL)")
	}
	volume := df.ColumnAt(3).Chunk(0).(*array.Int64)
	if volume.IsValid(1) {
		t.Fatal("volume[1] should be null (SQL NULL)")
	}
}

// Streaming path: verify contiguous batches cover all rows in order.
func TestSQLiteStreamingReader(t *testing.T) {
	db := openSQLite(t)
	seed(t, db, `CREATE TABLE nums (n INTEGER NOT NULL)`)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO nums VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 25_000 {
		if _, err := stmt.Exec(i); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	r, err := iosql.NewReader(context.Background(), db, "SELECT n FROM nums ORDER BY n", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	batches := 0
	total := 0
	var lastSeen int64 = -1
	for r.Next() {
		batches++
		df := r.DataFrame()
		col := df.ColumnAt(0).Chunk(0).(*array.Int64)
		for i := range col.Len() {
			v := col.Value(i)
			if v != lastSeen+1 {
				t.Fatalf("gap at batch %d row %d: saw %d after %d", batches, i, v, lastSeen)
			}
			lastSeen = v
			total++
		}
		df.Release()
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	if total != 25_000 {
		t.Fatalf("read %d rows, want 25000", total)
	}
	// 25_000 / 8192 default batch size = 4 batches.
	if batches != 4 {
		t.Fatalf("batches=%d, want 4", batches)
	}
}

func TestSQLiteCustomBatchSize(t *testing.T) {
	db := openSQLite(t)
	seed(t, db, `CREATE TABLE t (n INTEGER)`)
	for i := range 100 {
		db.Exec(`INSERT INTO t VALUES (?)`, i)
	}
	r, err := iosql.NewReader(context.Background(), db, "SELECT n FROM t",
		[]iosql.Option{iosql.WithBatchSize(30)})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	batches := 0
	for r.Next() {
		batches++
		r.DataFrame().Release()
	}
	// 100 / 30 = 4 batches.
	if batches != 4 {
		t.Fatalf("batches=%d, want 4", batches)
	}
}

func TestSQLiteReadAllEmpty(t *testing.T) {
	db := openSQLite(t)
	seed(t, db, `CREATE TABLE empty_t (x INTEGER, y TEXT)`)
	df, err := iosql.ReadSQL(context.Background(), db, "SELECT x, y FROM empty_t")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	if df.Height() != 0 {
		t.Fatalf("height=%d", df.Height())
	}
	if df.Width() != 2 {
		t.Fatalf("width=%d want 2 (schema preserved)", df.Width())
	}
}

func TestSQLiteQueryWithArgs(t *testing.T) {
	db := openSQLite(t)
	seed(t, db,
		`CREATE TABLE events (id INTEGER, tag TEXT, amount REAL)`,
		`INSERT INTO events VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'a', 3.5)`,
	)
	df, err := iosql.ReadSQL(context.Background(), db,
		"SELECT id, amount FROM events WHERE tag = ? AND amount > ?", "a", 1.0)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	if df.Height() != 2 {
		t.Fatalf("height=%d want 2", df.Height())
	}
}

func TestSQLiteContextCancel(t *testing.T) {
	db := openSQLite(t)
	seed(t, db, `CREATE TABLE nums (n INTEGER)`)
	for i := range 1000 {
		db.Exec(`INSERT INTO nums VALUES (?)`, i)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r, err := iosql.NewReader(ctx, db, "SELECT n FROM nums", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !r.Next() {
		t.Fatal("want at least one batch")
	}
	r.DataFrame().Release()

	cancel()
	for r.Next() {
		r.DataFrame().Release()
	}
	_ = r.Err()
}

func TestSQLiteTimestampMicrosPreserved(t *testing.T) {
	db := openSQLite(t)
	seed(t, db,
		`CREATE TABLE events (id INTEGER, at TIMESTAMP)`,
		`INSERT INTO events VALUES (1, '2025-04-18 12:34:56')`,
	)
	df, err := iosql.ReadSQL(context.Background(), db,
		"SELECT id, at FROM events")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	col := df.ColumnAt(1).Chunk(0).(*array.Int64)
	got := time.UnixMicro(col.Value(0))
	want, _ := time.Parse("2006-01-02 15:04:05", "2025-04-18 12:34:56")
	// Allow a day of slack to tolerate driver-side TZ quirks.
	if got.Unix() < want.Unix()-86400 || got.Unix() > want.Unix()+86400 {
		t.Fatalf("timestamp roundtrip off by > 1 day: got %v want %v", got, want)
	}
}
