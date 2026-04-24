// Behavioural parity with polars' tests/unit/series/test_series.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Polars' `pl.Series` constructor is heavily overloaded (Python list,
// numpy, pyarrow, dtype coercion, inference from literals). golars'
// constructor set is typed: one From* per dtype. These tests confirm
// the semantic contract each constructor honours: lengths, nulls,
// dtype, chunk layout, and basic accessors.
//
// Scenarios NOT ported:
//   - numpy / pyarrow interop      (no zero-copy adapter yet)
//   - dtype inference from Python literals (strictly typed API)
//   - Python list-of-dict ingestion (use io/json instead)
//   - List/Struct/Categorical      (no such dtypes)

package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: pl.Series([1, 2, 3]): a plain int series has length 3,
// dtype Int64, and no nulls.
func TestParitySeriesInt64FromSlice(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, err := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()

	if s.Len() != 3 {
		t.Fatalf("len=%d want 3", s.Len())
	}
	if s.DType().String() != "i64" {
		t.Fatalf("dtype=%s want i64", s.DType())
	}
	if s.NullCount() != 0 {
		t.Fatalf("null count=%d want 0", s.NullCount())
	}
	if s.Name() != "a" {
		t.Fatalf("name=%q want a", s.Name())
	}
}

// Polars: pl.Series([1.0, None, 3.0]): validity bitmap reflects the
// None. golars requires an explicit []bool; both representations must
// produce null_count == 1.
func TestParitySeriesFloat64WithNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, err := series.FromFloat64("a",
		[]float64{1.0, 0, 3.0},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()

	if s.NullCount() != 1 {
		t.Fatalf("null count=%d want 1", s.NullCount())
	}
	arr := s.Chunk(0).(*array.Float64)
	if !arr.IsValid(0) || arr.Value(0) != 1.0 {
		t.Fatal("[0] should be 1.0")
	}
	if arr.IsValid(1) {
		t.Fatal("[1] should be null")
	}
	if !arr.IsValid(2) || arr.Value(2) != 3.0 {
		t.Fatal("[2] should be 3.0")
	}
}

// Polars: an empty series has length 0 and null_count 0; its dtype
// is still whatever was declared.
func TestParitySeriesEmptyLength(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	cases := []struct {
		name string
		make func() *series.Series
		want string
	}{
		{"int64", func() *series.Series {
			s, _ := series.FromInt64("e", nil, nil, series.WithAllocator(alloc))
			return s
		}, "i64"},
		{"float64", func() *series.Series {
			s, _ := series.FromFloat64("e", nil, nil, series.WithAllocator(alloc))
			return s
		}, "f64"},
		{"string", func() *series.Series {
			s, _ := series.FromString("e", nil, nil, series.WithAllocator(alloc))
			return s
		}, "str"},
		{"bool", func() *series.Series {
			s, _ := series.FromBool("e", nil, nil, series.WithAllocator(alloc))
			return s
		}, "bool"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := c.make()
			defer s.Release()
			if s.Len() != 0 || s.NullCount() != 0 {
				t.Fatalf("len=%d nulls=%d, want 0/0", s.Len(), s.NullCount())
			}
			if s.DType().String() != c.want {
				t.Fatalf("dtype=%s want %s", s.DType(), c.want)
			}
		})
	}
}

// Polars: special float values round-trip: NaN, +Inf, -Inf.
func TestParitySeriesFloat64SpecialValues(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("a",
		[]float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, -0},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	arr := s.Chunk(0).(*array.Float64)
	if !math.IsNaN(arr.Value(0)) {
		t.Fatal("NaN didn't round-trip")
	}
	if !math.IsInf(arr.Value(1), 1) {
		t.Fatal("+Inf didn't round-trip")
	}
	if !math.IsInf(arr.Value(2), -1) {
		t.Fatal("-Inf didn't round-trip")
	}
}

// Polars: string constructor round-trips empty, ASCII, and UTF-8.
func TestParitySeriesStringEncodings(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	vals := []string{"", "ascii", "日本語", "emoji-🎉", "mixed-αβγ"}
	s, _ := series.FromString("a", vals, nil, series.WithAllocator(alloc))
	defer s.Release()

	arr := s.Chunk(0).(*array.String)
	for i, v := range vals {
		if arr.Value(i) != v {
			t.Fatalf("[%d]=%q want %q", i, arr.Value(i), v)
		}
	}
}

// Polars: pl.Series("x", data).name == "x" and a Rename produces a
// new series with the updated name (and the same values).
func TestParitySeriesRename(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("orig", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()
	renamed := s.Rename("renamed")
	defer renamed.Release()

	if renamed.Name() != "renamed" {
		t.Fatalf("Rename not applied: name=%q", renamed.Name())
	}
	if s.Name() != "orig" {
		t.Fatalf("Rename mutated source: name=%q", s.Name())
	}
	if renamed.Len() != s.Len() {
		t.Fatalf("len drift: %d vs %d", renamed.Len(), s.Len())
	}
}

// Polars: series cloned shares buffers but keeps the original alive
// until both are released. We verify both report identical length
// and dtype; the checked allocator catches ref-count leaks.
func TestParitySeriesClone(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	defer s.Release()
	c := s.Clone()
	defer c.Release()

	if c.Len() != s.Len() || c.DType().String() != s.DType().String() {
		t.Fatal("Clone diverged from source")
	}
	if c.Name() != s.Name() {
		t.Fatalf("name drift: %q vs %q", c.Name(), s.Name())
	}
}

// Polars: slice(offset, length) returns a zero-copy view of a
// sub-range. We verify length, first and last elements, and that
// out-of-range slices error.
func TestParitySeriesSlice(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{10, 20, 30, 40, 50}, nil, series.WithAllocator(alloc))
	defer s.Release()

	sl, err := s.Slice(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer sl.Release()
	if sl.Len() != 3 {
		t.Fatalf("slice len=%d want 3", sl.Len())
	}
	arr := sl.Chunk(0).(*array.Int64)
	if arr.Value(0) != 20 || arr.Value(2) != 40 {
		t.Fatalf("slice values [%d, _, %d]", arr.Value(0), arr.Value(2))
	}

	if _, err := s.Slice(0, 100); err == nil {
		t.Fatal("out-of-range slice must error")
	}
}

// Polars: constructing with a shorter validity slice is a user error.
// golars enforces valid length == values length at construction.
func TestParitySeriesInt64MismatchedValidityErrors(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// valid is length 2 but values is length 3: construction must fail.
	if _, err := series.FromInt64("a",
		[]int64{1, 2, 3},
		[]bool{true, false},
		series.WithAllocator(alloc)); err == nil {
		t.Fatal("mismatched validity length must error")
	}
}

// Polars: each constructor populates a single chunk for the simple
// "give me a slice" path. Multi-chunk behaviour is reserved for
// chunked builders; `FromInt64` should always return 1-chunk.
func TestParitySeriesConstructorsReturnSingleChunk(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	i, _ := series.FromInt64("i", []int64{1, 2}, nil, series.WithAllocator(alloc))
	defer i.Release()
	f, _ := series.FromFloat64("f", []float64{1}, nil, series.WithAllocator(alloc))
	defer f.Release()
	s, _ := series.FromString("s", []string{"a"}, nil, series.WithAllocator(alloc))
	defer s.Release()
	b, _ := series.FromBool("b", []bool{true}, nil, series.WithAllocator(alloc))
	defer b.Release()

	for _, ser := range []*series.Series{i, f, s, b} {
		if ser.NumChunks() != 1 {
			t.Fatalf("%s: num chunks = %d, want 1", ser.Name(), ser.NumChunks())
		}
	}
}

// Polars: pl.Series("x", [True, False, True]).dtype == Boolean: the
// bitmap packs into a single byte. We verify round-trip of every bit.
func TestParitySeriesBoolBitmapRoundTrip(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// 17 bools to cross a byte boundary (bits 0-7, 8-15, 16).
	src := []bool{
		true, false, true, true, false, false, true, false,
		false, true, false, true, true, false, true, true,
		true,
	}
	s, _ := series.FromBool("b", src, nil, series.WithAllocator(alloc))
	defer s.Release()

	arr := s.Chunk(0).(*array.Boolean)
	for i, w := range src {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%v want %v", i, arr.Value(i), w)
		}
	}
}
