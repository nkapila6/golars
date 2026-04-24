// Runnable demo: load a SQL query result into a golars DataFrame with
// the pure-Go modernc.org/sqlite driver and print it.
//
// This is a nested module (see examples/sql/go.mod) so the main golars
// vendor tree stays slim. Run with:
//
//	cd examples/sql && go run .
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "modernc.org/sqlite"

	iosql "github.com/Gaurav-Gosain/golars/io/sql"
)

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	seed := `
		CREATE TABLE trades (
			id INTEGER PRIMARY KEY,
			symbol TEXT NOT NULL,
			price REAL,
			volume INTEGER,
			ts TIMESTAMP
		);
		INSERT INTO trades VALUES
			(1, 'AAPL', 188.23, 100, '2025-04-18 10:00:00'),
			(2, 'GOOG', 2750.50, NULL, '2025-04-18 10:00:01'),
			(3, 'MSFT', NULL, 50,  '2025-04-18 10:00:02'),
			(4, 'AAPL', 188.51, 75, '2025-04-18 10:00:03'),
			(5, 'NVDA', 950.00, 200, '2025-04-18 10:00:04');
	`
	if _, err := db.Exec(seed); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Eager: pull the whole query into a single DataFrame.
	df, err := iosql.ReadSQL(ctx, db, "SELECT * FROM trades WHERE price IS NOT NULL ORDER BY id")
	if err != nil {
		log.Fatal(err)
	}
	defer df.Release()

	fmt.Println(df)
	fmt.Printf("loaded %d rows × %d columns\n\n", df.Height(), df.Width())

	// Streaming: use NewReader to process large result sets in batches.
	r, err := iosql.NewReader(ctx, db, "SELECT id, price FROM trades ORDER BY id",
		[]iosql.Option{iosql.WithBatchSize(2)})
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	batch := 0
	for r.Next() {
		batch++
		b := r.DataFrame()
		fmt.Printf("batch %d: %d rows\n", batch, b.Height())
		b.Release()
	}
	if err := r.Err(); err != nil {
		log.Fatal(err)
	}
}
