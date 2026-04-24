package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// buildStructSeries assembles a struct-typed Series {x: int64, y: str}
// with three rows (1,"a"), (2,"b"), and a null row. Test uses this
// to exercise Unnest's null-propagation.
func buildStructSeries(t *testing.T) *series.Series {
	t.Helper()
	mem := memory.DefaultAllocator
	fields := []arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64},
		{Name: "y", Type: arrow.BinaryTypes.String},
	}
	b := array.NewStructBuilder(mem, arrow.StructOf(fields...))
	defer b.Release()
	xb := b.FieldBuilder(0).(*array.Int64Builder)
	yb := b.FieldBuilder(1).(*array.StringBuilder)
	b.Append(true)
	xb.Append(1)
	yb.Append("a")
	b.Append(true)
	xb.Append(2)
	yb.Append("b")
	b.AppendNull()
	xb.AppendNull()
	yb.AppendNull()
	arr := b.NewArray()
	s, err := series.New("payload", arr)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestUnnestStruct(t *testing.T) {
	payload := buildStructSeries(t)
	id, _ := series.FromInt64("id", []int64{10, 20, 30}, nil)
	df, err := dataframe.New(id, payload)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	out, err := df.Unnest(context.Background(), "payload")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	// payload is replaced by its fields, preserving column order.
	wantCols := []string{"id", "x", "y"}
	if got := out.Schema().Names(); !equalStrings(got, wantCols) {
		t.Fatalf("columns = %v, want %v", got, wantCols)
	}
	// Check null propagation: row 2 is null in the struct, so both
	// x and y should be null on that row even if the child had no
	// explicit null bitmap.
	xs, _ := out.Column("x")
	ys, _ := out.Column("y")
	if xs.NullCount() != 1 {
		t.Errorf("x nullCount = %d, want 1", xs.NullCount())
	}
	if ys.NullCount() != 1 {
		t.Errorf("y nullCount = %d, want 1", ys.NullCount())
	}
}

func TestUnnestCollision(t *testing.T) {
	payload := buildStructSeries(t)
	x, _ := series.FromInt64("x", []int64{0, 0, 0}, nil) // name clashes with struct field
	df, err := dataframe.New(x, payload)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	_, err = df.Unnest(context.Background(), "payload")
	if err == nil {
		t.Fatal("expected collision error, got nil")
	}
}

func TestUnnestNonStruct(t *testing.T) {
	s, _ := series.FromInt64("id", []int64{1, 2}, nil)
	df, _ := dataframe.New(s)
	defer df.Release()
	_, err := df.Unnest(context.Background(), "id")
	if err == nil {
		t.Fatal("expected not-struct error")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
