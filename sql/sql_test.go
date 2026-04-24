package sql_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
	"github.com/Gaurav-Gosain/golars/sql"
)

func TestSQLBasicSelect(t *testing.T) {
	name, _ := series.FromString("name", []string{"a", "b", "c"}, nil)
	age, _ := series.FromInt64("age", []int64{10, 20, 30}, nil)
	df, _ := dataframe.New(name, age)
	defer df.Release()

	s := sql.NewSession()
	defer s.Close()
	if err := s.Register("people", df); err != nil {
		t.Fatal(err)
	}
	out, err := s.Query(context.Background(), "SELECT name FROM people WHERE age > 15")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height: got %d want 2", out.Height())
	}
	col, _ := out.Column("name")
	arr := col.Chunk(0).(*array.String)
	if arr.Value(0) != "b" || arr.Value(1) != "c" {
		t.Fatalf("rows: got %q, %q", arr.Value(0), arr.Value(1))
	}
}

func TestSQLGroupBy(t *testing.T) {
	d, _ := series.FromString("d", []string{"x", "x", "y", "y"}, nil)
	v, _ := series.FromInt64("v", []int64{1, 2, 10, 20}, nil)
	df, _ := dataframe.New(d, v)
	defer df.Release()

	s := sql.NewSession()
	defer s.Close()
	s.Register("t", df)
	out, err := s.Query(context.Background(),
		"SELECT d, SUM(v) AS total FROM t GROUP BY d ORDER BY total")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height: %d", out.Height())
	}
	total, _ := out.Column("total")
	arr := total.Chunk(0).(*array.Int64)
	if arr.Value(0) != 3 || arr.Value(1) != 30 {
		t.Fatalf("totals: %d, %d", arr.Value(0), arr.Value(1))
	}
}

func TestSQLDistinctLimit(t *testing.T) {
	v, _ := series.FromInt64("v", []int64{1, 1, 2, 2, 3, 3}, nil)
	df, _ := dataframe.New(v)
	defer df.Release()
	s := sql.NewSession()
	defer s.Close()
	s.Register("t", df)
	out, err := s.Query(context.Background(), "SELECT DISTINCT v FROM t LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height: got %d want 2", out.Height())
	}
}
