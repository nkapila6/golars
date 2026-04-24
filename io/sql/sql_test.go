package sql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	iosql "github.com/Gaurav-Gosain/golars/io/sql"
)

// A tiny in-process test driver. Avoids pulling in a real SQL driver
// dependency for the core tests: we're testing the bridge, not any
// specific engine.

type testRow struct {
	vals  []any
	valid []bool
}

type testDriver struct {
	columns []string
	dbTypes []string
	rows    []testRow
}

func (d *testDriver) Open(name string) (driver.Conn, error) { return &testConn{drv: d}, nil }

type testConn struct{ drv *testDriver }

func (c *testConn) Prepare(q string) (driver.Stmt, error) { return &testStmt{drv: c.drv}, nil }
func (c *testConn) Close() error                          { return nil }
func (c *testConn) Begin() (driver.Tx, error)             { return nil, nil }

type testStmt struct{ drv *testDriver }

func (s *testStmt) Close() error                                    { return nil }
func (s *testStmt) NumInput() int                                   { return 0 }
func (s *testStmt) Exec(args []driver.Value) (driver.Result, error) { return nil, nil }
func (s *testStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &testRows{drv: s.drv, i: 0}, nil
}

type testRows struct {
	drv *testDriver
	i   int
}

func (r *testRows) Columns() []string { return r.drv.columns }
func (r *testRows) Close() error      { return nil }
func (r *testRows) Next(dest []driver.Value) error {
	if r.i >= len(r.drv.rows) {
		return io.EOF
	}
	row := r.drv.rows[r.i]
	for j, v := range row.vals {
		if !row.valid[j] {
			dest[j] = nil
		} else {
			dest[j] = v
		}
	}
	r.i++
	return nil
}

// ColumnTypeDatabaseTypeName lets us return driver-style dbtype names.
func (r *testRows) ColumnTypeDatabaseTypeName(i int) string { return r.drv.dbTypes[i] }

func registerDriver(t *testing.T, name string, d *testDriver) {
	t.Helper()
	sql.Register(name, d)
}

var drvCounter int

func newTestDB(t *testing.T, d *testDriver) *sql.DB {
	t.Helper()
	drvCounter++
	name := fmt.Sprintf("golars-test-%d", drvCounter)
	registerDriver(t, name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestReadSQLInts(t *testing.T) {
	d := &testDriver{
		columns: []string{"id", "n"},
		dbTypes: []string{"INTEGER", "BIGINT"},
		rows: []testRow{
			{vals: []any{int64(1), int64(100)}, valid: []bool{true, true}},
			{vals: []any{int64(2), int64(200)}, valid: []bool{true, true}},
			{vals: []any{int64(3), nil}, valid: []bool{true, false}},
		},
	}
	db := newTestDB(t, d)
	defer db.Close()

	df, err := iosql.ReadSQL(context.Background(), db, "SELECT id, n FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if df.Height() != 3 || df.Width() != 2 {
		t.Fatalf("shape %d×%d want 3×2", df.Height(), df.Width())
	}
	idCol := df.ColumnAt(0).Chunk(0).(*array.Int64)
	nCol := df.ColumnAt(1).Chunk(0).(*array.Int64)
	if idCol.Value(0) != 1 || idCol.Value(2) != 3 {
		t.Fatalf("id: %v %v %v", idCol.Value(0), idCol.Value(1), idCol.Value(2))
	}
	if !nCol.IsValid(0) || !nCol.IsValid(1) || nCol.IsValid(2) {
		t.Fatal("null propagation wrong")
	}
}

func TestReadSQLFloats(t *testing.T) {
	d := &testDriver{
		columns: []string{"x", "y"},
		dbTypes: []string{"REAL", "DOUBLE"},
		rows: []testRow{
			{vals: []any{float64(1.5), float64(2.5)}, valid: []bool{true, true}},
			{vals: []any{float64(0), float64(-1)}, valid: []bool{false, true}},
		},
	}
	db := newTestDB(t, d)
	defer db.Close()

	df, err := iosql.ReadSQL(context.Background(), db, "SELECT x,y FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	if df.Height() != 2 || df.Width() != 2 {
		t.Fatalf("shape %d×%d", df.Height(), df.Width())
	}
}

func TestReadSQLStrings(t *testing.T) {
	d := &testDriver{
		columns: []string{"name"},
		dbTypes: []string{"TEXT"},
		rows: []testRow{
			{vals: []any{"alice"}, valid: []bool{true}},
			{vals: []any{""}, valid: []bool{false}},
			{vals: []any{"bob"}, valid: []bool{true}},
		},
	}
	db := newTestDB(t, d)
	defer db.Close()

	df, err := iosql.ReadSQL(context.Background(), db, "SELECT name FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	arr := df.ColumnAt(0).Chunk(0).(*array.String)
	if arr.Value(0) != "alice" || arr.Value(2) != "bob" {
		t.Fatal("strings wrong")
	}
	if arr.IsValid(1) {
		t.Fatal("null not preserved")
	}
}

func TestReadSQLBool(t *testing.T) {
	d := &testDriver{
		columns: []string{"ok"},
		dbTypes: []string{"BOOL"},
		rows: []testRow{
			{vals: []any{true}, valid: []bool{true}},
			{vals: []any{false}, valid: []bool{true}},
		},
	}
	db := newTestDB(t, d)
	defer db.Close()

	df, err := iosql.ReadSQL(context.Background(), db, "SELECT ok FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	arr := df.ColumnAt(0).Chunk(0).(*array.Boolean)
	if !arr.Value(0) || arr.Value(1) {
		t.Fatal("bool wrong")
	}
}

func TestReadSQLEmpty(t *testing.T) {
	d := &testDriver{
		columns: []string{"n"},
		dbTypes: []string{"INTEGER"},
		rows:    nil,
	}
	db := newTestDB(t, d)
	defer db.Close()
	df, err := iosql.ReadSQL(context.Background(), db, "SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	if df.Height() != 0 {
		t.Fatalf("empty rows but height=%d", df.Height())
	}
}

func TestReadSQLUnknownTypeFallsBackToString(t *testing.T) {
	d := &testDriver{
		columns: []string{"x"},
		dbTypes: []string{"JSONB"},
		rows: []testRow{
			{vals: []any{"{\"a\":1}"}, valid: []bool{true}},
		},
	}
	db := newTestDB(t, d)
	defer db.Close()
	df, err := iosql.ReadSQL(context.Background(), db, "q")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	arr := df.ColumnAt(0).Chunk(0).(*array.String)
	if arr.Value(0) != `{"a":1}` {
		t.Fatalf("fallback: got %q", arr.Value(0))
	}
}
