// Package schema defines the Schema type, an ordered, immutable collection of
// named column dtypes. Schemas are the metadata half of a DataFrame.
package schema

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// Field is a named, typed column description.
type Field struct {
	Name  string
	DType dtype.DType
}

// Sentinel errors returned by Schema operations.
var (
	ErrColumnNotFound  = errors.New("schema: column not found")
	ErrDuplicateColumn = errors.New("schema: duplicate column name")
)

// Schema is an ordered, immutable collection of Fields. Construct through New
// or FromArrow; the zero value is not useful.
type Schema struct {
	fields []Field
	index  map[string]int
}

// New returns a Schema for the given fields. Duplicate names are rejected.
func New(fields ...Field) (*Schema, error) {
	return build(fields, true)
}

// MustNew is New with a panic on error. Intended for test fixtures and
// statically known schemas.
func MustNew(fields ...Field) *Schema {
	s, err := New(fields...)
	if err != nil {
		panic(err)
	}
	return s
}

// FromArrow adapts an arrow.Schema into a Schema. Arrow metadata on fields is
// dropped.
func FromArrow(s *arrow.Schema) *Schema {
	if s == nil {
		return &Schema{index: map[string]int{}}
	}
	af := s.Fields()
	fields := make([]Field, len(af))
	for i, f := range af {
		fields[i] = Field{Name: f.Name, DType: dtype.FromArrow(f.Type)}
	}
	out, err := build(fields, false)
	if err != nil {
		// arrow schemas may contain duplicate field names; we accept them by
		// taking the first occurrence for lookup, but preserve the order.
		return buildLenient(fields)
	}
	return out
}

func build(fields []Field, strict bool) (*Schema, error) {
	copyFields := make([]Field, len(fields))
	copy(copyFields, fields)
	idx := make(map[string]int, len(copyFields))
	for i, f := range copyFields {
		if _, dup := idx[f.Name]; dup && strict {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, f.Name)
		}
		if _, ok := idx[f.Name]; !ok {
			idx[f.Name] = i
		}
	}
	return &Schema{fields: copyFields, index: idx}, nil
}

func buildLenient(fields []Field) *Schema {
	s, _ := build(fields, false)
	return s
}

// ToArrow converts to an arrow.Schema. All fields are marked nullable, matching
// polars' defaults.
func (s *Schema) ToArrow() *arrow.Schema {
	af := make([]arrow.Field, len(s.fields))
	for i, f := range s.fields {
		af[i] = arrow.Field{Name: f.Name, Type: f.DType.Arrow(), Nullable: true}
	}
	return arrow.NewSchema(af, nil)
}

// Len returns the number of fields.
func (s *Schema) Len() int { return len(s.fields) }

// Empty reports whether the schema has no fields.
func (s *Schema) Empty() bool { return len(s.fields) == 0 }

// Field returns the field at position i. Panics if i is out of range.
func (s *Schema) Field(i int) Field { return s.fields[i] }

// Fields returns a copy of the fields slice.
func (s *Schema) Fields() []Field {
	out := make([]Field, len(s.fields))
	copy(out, s.fields)
	return out
}

// FieldByName looks up a field by name.
func (s *Schema) FieldByName(name string) (Field, bool) {
	i, ok := s.index[name]
	if !ok {
		return Field{}, false
	}
	return s.fields[i], true
}

// Index returns the position of name, or -1 if absent.
func (s *Schema) Index(name string) (int, bool) {
	i, ok := s.index[name]
	return i, ok
}

// Contains reports whether the schema has a field of the given name.
func (s *Schema) Contains(name string) bool {
	_, ok := s.index[name]
	return ok
}

// Names returns column names in order.
func (s *Schema) Names() []string {
	out := make([]string, len(s.fields))
	for i, f := range s.fields {
		out[i] = f.Name
	}
	return out
}

// DTypes returns column dtypes in order.
func (s *Schema) DTypes() []dtype.DType {
	out := make([]dtype.DType, len(s.fields))
	for i, f := range s.fields {
		out[i] = f.DType
	}
	return out
}

// Select returns a schema with only the named columns in the order requested.
// Unknown names produce ErrColumnNotFound.
func (s *Schema) Select(names ...string) (*Schema, error) {
	out := make([]Field, 0, len(names))
	for _, n := range names {
		f, ok := s.FieldByName(n)
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, n)
		}
		out = append(out, f)
	}
	return New(out...)
}

// Drop returns a schema without the named columns. Missing names are ignored.
func (s *Schema) Drop(names ...string) *Schema {
	if len(names) == 0 {
		return s
	}
	drop := make(map[string]struct{}, len(names))
	for _, n := range names {
		drop[n] = struct{}{}
	}
	out := make([]Field, 0, len(s.fields))
	for _, f := range s.fields {
		if _, removed := drop[f.Name]; removed {
			continue
		}
		out = append(out, f)
	}
	return buildLenient(out)
}

// Rename returns a new schema with oldName replaced by newName.
func (s *Schema) Rename(oldName, newName string) (*Schema, error) {
	if oldName == newName {
		if !s.Contains(oldName) {
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, oldName)
		}
		return s, nil
	}
	i, ok := s.index[oldName]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, oldName)
	}
	if _, dup := s.index[newName]; dup {
		return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, newName)
	}
	out := make([]Field, len(s.fields))
	copy(out, s.fields)
	out[i].Name = newName
	return New(out...)
}

// WithField appends f if new, or replaces the existing field with that name.
func (s *Schema) WithField(f Field) *Schema {
	out := make([]Field, len(s.fields))
	copy(out, s.fields)
	if i, ok := s.index[f.Name]; ok {
		out[i] = f
	} else {
		out = append(out, f)
	}
	return buildLenient(out)
}

// Equal reports whether two schemas have the same fields in the same order.
func (s *Schema) Equal(other *Schema) bool {
	if s == nil || other == nil {
		return s == nil && other == nil
	}
	if len(s.fields) != len(other.fields) {
		return false
	}
	for i, f := range s.fields {
		of := other.fields[i]
		if f.Name != of.Name || !f.DType.Equal(of.DType) {
			return false
		}
	}
	return true
}

// String returns a single-line repr: schema{a: i64, b: str}.
func (s *Schema) String() string {
	if s == nil {
		return "<nil schema>"
	}
	var b strings.Builder
	b.WriteString("schema{")
	for i, f := range s.fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(f.Name)
		b.WriteString(": ")
		b.WriteString(f.DType.String())
	}
	b.WriteByte('}')
	return b.String()
}
