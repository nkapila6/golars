package schema_test

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/schema"
)

func sampleSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.New(
		schema.Field{Name: "a", DType: dtype.Int64()},
		schema.Field{Name: "b", DType: dtype.String()},
		schema.Field{Name: "c", DType: dtype.Float64()},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestNewAndBasicAccessors(t *testing.T) {
	t.Parallel()
	s := sampleSchema(t)

	if got, want := s.Len(), 3; got != want {
		t.Errorf("Len = %d, want %d", got, want)
	}
	if s.Empty() {
		t.Error("Empty should be false")
	}

	if got, want := s.Names(), []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("Names = %v, want %v", got, want)
	}

	if f := s.Field(1); f.Name != "b" || !f.DType.Equal(dtype.String()) {
		t.Errorf("Field(1) = %+v, want {b, str}", f)
	}

	if i, ok := s.Index("c"); !ok || i != 2 {
		t.Errorf("Index(c) = (%d, %v), want (2, true)", i, ok)
	}
	if _, ok := s.Index("missing"); ok {
		t.Error("Index(missing) should return false")
	}

	if got := s.Contains("a"); !got {
		t.Error("Contains(a) should be true")
	}
}

func TestEmptySchema(t *testing.T) {
	t.Parallel()
	s, err := schema.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !s.Empty() {
		t.Error("Empty should be true")
	}
}

func TestDuplicateNameRejected(t *testing.T) {
	t.Parallel()
	_, err := schema.New(
		schema.Field{Name: "x", DType: dtype.Int64()},
		schema.Field{Name: "x", DType: dtype.Float64()},
	)
	if !errors.Is(err, schema.ErrDuplicateColumn) {
		t.Fatalf("expected ErrDuplicateColumn, got %v", err)
	}
}

func TestSelect(t *testing.T) {
	t.Parallel()
	s := sampleSchema(t)

	got, err := s.Select("c", "a")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	want, _ := schema.New(
		schema.Field{Name: "c", DType: dtype.Float64()},
		schema.Field{Name: "a", DType: dtype.Int64()},
	)
	if !got.Equal(want) {
		t.Errorf("Select = %s, want %s", got, want)
	}

	if _, err := s.Select("missing"); !errors.Is(err, schema.ErrColumnNotFound) {
		t.Errorf("expected ErrColumnNotFound, got %v", err)
	}
}

func TestDrop(t *testing.T) {
	t.Parallel()
	s := sampleSchema(t)
	got := s.Drop("b", "missing")
	want, _ := schema.New(
		schema.Field{Name: "a", DType: dtype.Int64()},
		schema.Field{Name: "c", DType: dtype.Float64()},
	)
	if !got.Equal(want) {
		t.Errorf("Drop = %s, want %s", got, want)
	}
}

func TestRename(t *testing.T) {
	t.Parallel()
	s := sampleSchema(t)

	got, err := s.Rename("b", "beta")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	want, _ := schema.New(
		schema.Field{Name: "a", DType: dtype.Int64()},
		schema.Field{Name: "beta", DType: dtype.String()},
		schema.Field{Name: "c", DType: dtype.Float64()},
	)
	if !got.Equal(want) {
		t.Errorf("Rename = %s, want %s", got, want)
	}

	if _, err := s.Rename("missing", "x"); !errors.Is(err, schema.ErrColumnNotFound) {
		t.Errorf("expected ErrColumnNotFound, got %v", err)
	}

	if _, err := s.Rename("a", "c"); !errors.Is(err, schema.ErrDuplicateColumn) {
		t.Errorf("expected ErrDuplicateColumn, got %v", err)
	}

	if rs, err := s.Rename("a", "a"); err != nil || !rs.Equal(s) {
		t.Errorf("Rename to same name: want identity, got (%v, %v)", rs, err)
	}
}

func TestWithField(t *testing.T) {
	t.Parallel()
	s := sampleSchema(t)

	// append
	appended := s.WithField(schema.Field{Name: "d", DType: dtype.Bool()})
	if appended.Len() != 4 {
		t.Errorf("after append, Len = %d, want 4", appended.Len())
	}
	if f, _ := appended.FieldByName("d"); !f.DType.Equal(dtype.Bool()) {
		t.Errorf("appended field dtype = %s, want bool", f.DType)
	}

	// replace
	replaced := s.WithField(schema.Field{Name: "b", DType: dtype.Int32()})
	if replaced.Len() != 3 {
		t.Errorf("after replace, Len = %d, want 3", replaced.Len())
	}
	if f, _ := replaced.FieldByName("b"); !f.DType.Equal(dtype.Int32()) {
		t.Errorf("replaced field dtype = %s, want i32", f.DType)
	}

	// source schema must be unchanged
	if !s.Equal(sampleSchema(t)) {
		t.Error("source schema was mutated")
	}
}

func TestEqual(t *testing.T) {
	t.Parallel()
	a := sampleSchema(t)
	b := sampleSchema(t)
	if !a.Equal(b) {
		t.Error("identical schemas should be equal")
	}

	c, _ := schema.New(
		schema.Field{Name: "a", DType: dtype.Int64()},
		schema.Field{Name: "b", DType: dtype.String()},
	)
	if a.Equal(c) {
		t.Error("different-length schemas should not be equal")
	}

	var nilS *schema.Schema
	if !nilS.Equal(nil) {
		t.Error("nil schemas should be equal")
	}
	if nilS.Equal(a) {
		t.Error("nil should not equal non-nil")
	}
}

func TestArrowRoundtrip(t *testing.T) {
	t.Parallel()
	orig := sampleSchema(t)
	round := schema.FromArrow(orig.ToArrow())
	if !orig.Equal(round) {
		t.Errorf("round trip lost equality:\norig = %s\nround = %s", orig, round)
	}
}

func TestToArrowFields(t *testing.T) {
	t.Parallel()
	s := sampleSchema(t)
	as := s.ToArrow()
	if as.NumFields() != 3 {
		t.Errorf("arrow schema fields = %d, want 3", as.NumFields())
	}
	if got := as.Field(0).Type.ID(); got != arrow.INT64 {
		t.Errorf("field 0 type = %v, want INT64", got)
	}
	if !as.Field(1).Nullable {
		t.Error("fields should be marked nullable by default")
	}
}

func TestString(t *testing.T) {
	t.Parallel()
	s := sampleSchema(t)
	if got, want := s.String(), "schema{a: i64, b: str, c: f64}"; got != want {
		t.Errorf("String = %q, want %q", got, want)
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
