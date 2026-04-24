package dataframe_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

func makeDF(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil)
	b, _ := series.FromString("b", []string{"w", "x", "y", "z"}, nil)
	c, _ := series.FromFloat64("c", []float64{1.5, 2.5, 3.5, 4.5}, nil)
	df, err := dataframe.New(a, b, c)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return df
}

func TestNewAndShape(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	if h, w := df.Shape(); h != 4 || w != 3 {
		t.Errorf("Shape = (%d, %d), want (4, 3)", h, w)
	}

	wantSchema, _ := schema.New(
		schema.Field{Name: "a", DType: dtype.Int64()},
		schema.Field{Name: "b", DType: dtype.String()},
		schema.Field{Name: "c", DType: dtype.Float64()},
	)
	if !df.Schema().Equal(wantSchema) {
		t.Errorf("Schema = %s, want %s", df.Schema(), wantSchema)
	}
}

func TestNewEmpty(t *testing.T) {
	t.Parallel()
	df, err := dataframe.New()
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	defer df.Release()
	if h, w := df.Shape(); h != 0 || w != 0 {
		t.Errorf("Shape = (%d, %d), want (0, 0)", h, w)
	}
}

func TestNewRejectsHeightMismatch(t *testing.T) {
	t.Parallel()
	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil)
	b, _ := series.FromInt64("b", []int64{1, 2}, nil)

	_, err := dataframe.New(a, b)
	if !errors.Is(err, dataframe.ErrHeightMismatch) {
		t.Errorf("expected ErrHeightMismatch, got %v", err)
	}
	// On error the constructor did not consume references.
	a.Release()
	b.Release()
}

func TestNewRejectsDuplicateColumn(t *testing.T) {
	t.Parallel()
	a, _ := series.FromInt64("x", []int64{1}, nil)
	b, _ := series.FromInt64("x", []int64{2}, nil)

	_, err := dataframe.New(a, b)
	if !errors.Is(err, dataframe.ErrDuplicateColumn) {
		t.Errorf("expected ErrDuplicateColumn, got %v", err)
	}
	a.Release()
	b.Release()
}

func TestColumn(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	col, err := df.Column("b")
	if err != nil {
		t.Fatalf("Column: %v", err)
	}
	if col.Name() != "b" {
		t.Errorf("Column name = %q, want b", col.Name())
	}

	if _, err := df.Column("missing"); !errors.Is(err, dataframe.ErrColumnNotFound) {
		t.Errorf("expected ErrColumnNotFound, got %v", err)
	}
}

func TestSelect(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	got, err := df.Select("c", "a")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	defer got.Release()

	wantSchema, _ := schema.New(
		schema.Field{Name: "c", DType: dtype.Float64()},
		schema.Field{Name: "a", DType: dtype.Int64()},
	)
	if !got.Schema().Equal(wantSchema) {
		t.Errorf("Schema = %s, want %s", got.Schema(), wantSchema)
	}
	if got.Height() != 4 {
		t.Errorf("Height = %d, want 4", got.Height())
	}

	if _, err := df.Select("missing"); !errors.Is(err, schema.ErrColumnNotFound) {
		t.Errorf("expected ErrColumnNotFound, got %v", err)
	}
}

func TestDrop(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	got := df.Drop("b", "missing")
	defer got.Release()

	if got.Width() != 2 {
		t.Errorf("Width = %d, want 2", got.Width())
	}
	if got.Contains("b") {
		t.Error("result should not contain b")
	}
}

func TestRename(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	got, err := df.Rename("b", "beta")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	defer got.Release()
	if !got.Contains("beta") || got.Contains("b") {
		t.Error("rename did not propagate")
	}

	if _, err := df.Rename("missing", "x"); !errors.Is(err, dataframe.ErrColumnNotFound) {
		t.Errorf("expected ErrColumnNotFound, got %v", err)
	}
	if _, err := df.Rename("a", "c"); !errors.Is(err, dataframe.ErrDuplicateColumn) {
		t.Errorf("expected ErrDuplicateColumn, got %v", err)
	}
}

func TestWithColumnAppend(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	d, _ := series.FromBool("d", []bool{true, false, true, false}, nil)
	got, err := df.WithColumn(d)
	if err != nil {
		d.Release()
		t.Fatalf("WithColumn: %v", err)
	}
	defer got.Release()

	if got.Width() != 4 {
		t.Errorf("Width = %d, want 4", got.Width())
	}
	col, _ := got.Column("d")
	if !col.DType().Equal(dtype.Bool()) {
		t.Errorf("appended DType = %s, want bool", col.DType())
	}
}

func TestWithColumnReplace(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	replacement, _ := series.FromInt64("a", []int64{100, 200, 300, 400}, nil)
	got, err := df.WithColumn(replacement)
	if err != nil {
		replacement.Release()
		t.Fatalf("WithColumn: %v", err)
	}
	defer got.Release()

	if got.Width() != 3 {
		t.Errorf("after replace Width = %d, want 3", got.Width())
	}
	col, _ := got.Column("a")
	if col.Len() != 4 {
		t.Errorf("replaced column Len = %d, want 4", col.Len())
	}
}

func TestWithColumnHeightMismatch(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	bad, _ := series.FromInt64("bad", []int64{1, 2}, nil)
	_, err := df.WithColumn(bad)
	if !errors.Is(err, dataframe.ErrHeightMismatch) {
		t.Errorf("expected ErrHeightMismatch, got %v", err)
	}
	bad.Release()
}

func TestSlice(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	got, err := df.Slice(1, 2)
	if err != nil {
		t.Fatalf("Slice: %v", err)
	}
	defer got.Release()

	if got.Height() != 2 {
		t.Errorf("Height = %d, want 2", got.Height())
	}
	if got.Width() != 3 {
		t.Errorf("Width = %d, want 3", got.Width())
	}
}

func TestSliceOutOfBounds(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	for _, tc := range []struct{ offset, length int }{
		{-1, 1}, {0, -1}, {0, 10}, {2, 3},
	} {
		if _, err := df.Slice(tc.offset, tc.length); !errors.Is(err, dataframe.ErrSliceOutOfBounds) {
			t.Errorf("Slice(%d, %d) err = %v, want ErrSliceOutOfBounds",
				tc.offset, tc.length, err)
		}
	}
}

func TestHeadTail(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	head := df.Head(2)
	defer head.Release()
	if head.Height() != 2 {
		t.Errorf("Head(2) Height = %d, want 2", head.Height())
	}

	tail := df.Tail(1)
	defer tail.Release()
	if tail.Height() != 1 {
		t.Errorf("Tail(1) Height = %d, want 1", tail.Height())
	}

	// Head beyond height clamps.
	big := df.Head(100)
	defer big.Release()
	if big.Height() != 4 {
		t.Errorf("Head(100) Height = %d, want 4", big.Height())
	}

	// Negative clamps to 0.
	none := df.Head(-5)
	defer none.Release()
	if none.Height() != 0 {
		t.Errorf("Head(-5) Height = %d, want 0", none.Height())
	}
}

func TestEmptySchemaDF(t *testing.T) {
	t.Parallel()
	sch, _ := schema.New(
		schema.Field{Name: "a", DType: dtype.Int64()},
		schema.Field{Name: "b", DType: dtype.String()},
	)
	df := dataframe.Empty(sch)
	defer df.Release()

	if df.Height() != 0 {
		t.Errorf("Height = %d, want 0", df.Height())
	}
	if df.Width() != 2 {
		t.Errorf("Width = %d, want 2", df.Width())
	}
}

func TestClone(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	clone := df.Clone()
	defer clone.Release()

	if clone.Height() != df.Height() || clone.Width() != df.Width() {
		t.Error("clone shape mismatch")
	}

	a1, _ := df.Column("a")
	a2, _ := clone.Column("a")
	if a1.Chunked() != a2.Chunked() {
		t.Error("clone should share chunked data")
	}
}

func TestString(t *testing.T) {
	t.Parallel()
	df := makeDF(t)
	defer df.Release()

	// Summary() keeps the old one-liner shape; String() is now the
	// polars-style pretty table.
	want := "dataframe [4 x 3] schema{a: i64, b: str, c: f64}"
	if got := df.Summary(); got != want {
		t.Errorf("Summary = %q, want %q", got, want)
	}
	s := df.String()
	if !strings.Contains(s, "shape: (4, 3)") {
		t.Errorf("String does not contain shape header: %q", s)
	}
	for _, want := range []string{"a", "b", "c", "i64", "str", "f64"} {
		if !strings.Contains(s, want) {
			t.Errorf("String missing %q in:\n%s", want, s)
		}
	}
}
