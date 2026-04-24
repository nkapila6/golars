package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func buildStructSeriesForTest(t *testing.T) *series.Series {
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
	s, err := series.New("payload", b.NewArray())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStructField(t *testing.T) {
	s := buildStructSeriesForTest(t)
	defer s.Release()
	x, err := s.Struct().Field("x")
	if err != nil {
		t.Fatal(err)
	}
	defer x.Release()
	if x.Len() != 3 {
		t.Fatalf("len=%d want 3", x.Len())
	}
	if x.NullCount() != 1 {
		t.Errorf("x nullCount=%d want 1", x.NullCount())
	}
}

func TestStructFieldNames(t *testing.T) {
	s := buildStructSeriesForTest(t)
	defer s.Release()
	names, err := s.Struct().FieldNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "x" || names[1] != "y" {
		t.Errorf("field names %v, want [x y]", names)
	}
}

func TestStructNonStruct(t *testing.T) {
	s, _ := series.FromInt64("x", []int64{1, 2}, nil)
	defer s.Release()
	if _, err := s.Struct().Field("x"); err == nil {
		t.Error("expected error on non-struct series")
	}
}
