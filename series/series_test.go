package series_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestFromInt64(t *testing.T) {
	t.Parallel()

	s, err := series.FromInt64("x", []int64{1, 2, 3, 4}, nil)
	if err != nil {
		t.Fatalf("FromInt64: %v", err)
	}
	defer s.Release()

	if got, want := s.Name(), "x"; got != want {
		t.Errorf("Name = %q, want %q", got, want)
	}
	if got, want := s.Len(), 4; got != want {
		t.Errorf("Len = %d, want %d", got, want)
	}
	if got, want := s.NullCount(), 0; got != want {
		t.Errorf("NullCount = %d, want %d", got, want)
	}
	if !s.DType().Equal(dtype.Int64()) {
		t.Errorf("DType = %s, want i64", s.DType())
	}
	if got, want := s.NumChunks(), 1; got != want {
		t.Errorf("NumChunks = %d, want %d", got, want)
	}
}

func TestFromInt64WithNulls(t *testing.T) {
	t.Parallel()
	s, err := series.FromInt64("x", []int64{1, 0, 3, 0}, []bool{true, false, true, false})
	if err != nil {
		t.Fatalf("FromInt64: %v", err)
	}
	defer s.Release()
	if got, want := s.NullCount(), 2; got != want {
		t.Errorf("NullCount = %d, want %d", got, want)
	}
}

func TestFromFloat64(t *testing.T) {
	t.Parallel()
	s, err := series.FromFloat64("y", []float64{1.5, 2.5, 3.5}, nil)
	if err != nil {
		t.Fatalf("FromFloat64: %v", err)
	}
	defer s.Release()
	if !s.DType().Equal(dtype.Float64()) {
		t.Errorf("DType = %s, want f64", s.DType())
	}
	if s.Len() != 3 {
		t.Errorf("Len = %d, want 3", s.Len())
	}
}

func TestFromString(t *testing.T) {
	t.Parallel()
	s, err := series.FromString("s", []string{"a", "b", "", "d"}, []bool{true, true, false, true})
	if err != nil {
		t.Fatalf("FromString: %v", err)
	}
	defer s.Release()
	if !s.DType().Equal(dtype.String()) {
		t.Errorf("DType = %s, want str", s.DType())
	}
	if got, want := s.NullCount(), 1; got != want {
		t.Errorf("NullCount = %d, want %d", got, want)
	}
}

func TestFromBool(t *testing.T) {
	t.Parallel()
	s, err := series.FromBool("b", []bool{true, false, true}, nil)
	if err != nil {
		t.Fatalf("FromBool: %v", err)
	}
	defer s.Release()
	if !s.DType().Equal(dtype.Bool()) {
		t.Errorf("DType = %s, want bool", s.DType())
	}
}

func TestLengthMismatch(t *testing.T) {
	t.Parallel()
	_, err := series.FromInt64("x", []int64{1, 2}, []bool{true})
	if !errors.Is(err, series.ErrLengthMismatch) {
		t.Errorf("expected ErrLengthMismatch, got %v", err)
	}
}

func TestEmpty(t *testing.T) {
	t.Parallel()
	s := series.Empty("empty", dtype.Int64())
	defer s.Release()
	if s.Len() != 0 {
		t.Errorf("Len = %d, want 0", s.Len())
	}
	if !s.DType().Equal(dtype.Int64()) {
		t.Errorf("DType = %s, want i64", s.DType())
	}
}

func TestSlice(t *testing.T) {
	t.Parallel()
	s, _ := series.FromInt64("x", []int64{10, 20, 30, 40, 50}, nil)
	defer s.Release()

	sl, err := s.Slice(1, 3)
	if err != nil {
		t.Fatalf("Slice: %v", err)
	}
	defer sl.Release()

	if got, want := sl.Len(), 3; got != want {
		t.Errorf("sliced Len = %d, want %d", got, want)
	}
	if sl.Name() != "x" {
		t.Errorf("sliced Name = %q, want x", sl.Name())
	}
}

func TestSliceBounds(t *testing.T) {
	t.Parallel()
	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil)
	defer s.Release()

	for _, tc := range []struct {
		offset, length int
	}{
		{-1, 2},
		{0, -1},
		{0, 4},
		{2, 2},
	} {
		if _, err := s.Slice(tc.offset, tc.length); !errors.Is(err, series.ErrSliceOutOfBounds) {
			t.Errorf("Slice(%d, %d) err = %v, want ErrSliceOutOfBounds", tc.offset, tc.length, err)
		}
	}
}

func TestRenameSharesData(t *testing.T) {
	t.Parallel()
	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil)
	defer s.Release()

	renamed := s.Rename("y")
	defer renamed.Release()

	if renamed.Name() != "y" {
		t.Errorf("Name = %q, want y", renamed.Name())
	}
	if s.Name() != "x" {
		t.Errorf("original Name changed to %q", s.Name())
	}
	if renamed.Chunked() != s.Chunked() {
		t.Error("renamed Series should share underlying chunked data")
	}
}

func TestClone(t *testing.T) {
	t.Parallel()
	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil)
	defer s.Release()

	clone := s.Clone()
	defer clone.Release()
	if clone.Chunked() != s.Chunked() {
		t.Error("clone should share chunked data")
	}
	if clone.Name() != s.Name() {
		t.Errorf("clone Name = %q, want %q", clone.Name(), s.Name())
	}
}

func TestNewRejectsEmptyChunks(t *testing.T) {
	t.Parallel()
	if _, err := series.New("x"); !errors.Is(err, series.ErrEmptyChunks) {
		t.Errorf("expected ErrEmptyChunks, got %v", err)
	}
}

func TestNewRejectsDTypeMismatch(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator

	i64b := array.NewInt64Builder(mem)
	i64b.Append(1)
	iarr := i64b.NewArray()
	i64b.Release()

	f64b := array.NewFloat64Builder(mem)
	f64b.Append(1.0)
	farr := f64b.NewArray()
	f64b.Release()

	s, err := series.New("bad", iarr, farr)
	if err == nil {
		s.Release()
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, series.ErrChunkDTypeMismatch) {
		t.Errorf("expected ErrChunkDTypeMismatch, got %v", err)
	}
	// On error the Series constructor did not consume references.
	iarr.Release()
	farr.Release()
}

func TestMultiChunk(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator

	b1 := array.NewInt64Builder(mem)
	b1.AppendValues([]int64{1, 2, 3}, nil)
	a1 := b1.NewArray()
	b1.Release()

	b2 := array.NewInt64Builder(mem)
	b2.AppendValues([]int64{4, 5}, nil)
	a2 := b2.NewArray()
	b2.Release()

	s, err := series.New("x", a1, a2)
	if err != nil {
		a1.Release()
		a2.Release()
		t.Fatalf("New: %v", err)
	}
	defer s.Release()

	if got, want := s.Len(), 5; got != want {
		t.Errorf("Len = %d, want %d", got, want)
	}
	if got, want := s.NumChunks(), 2; got != want {
		t.Errorf("NumChunks = %d, want %d", got, want)
	}
}

func TestString(t *testing.T) {
	t.Parallel()
	s, _ := series.FromInt64("col", []int64{1, 2, 3}, []bool{true, false, true})
	defer s.Release()
	// Summary is the one-line compact repr; String is the pretty table.
	want := "col: i64 [len=3, nulls=1]"
	if got := s.Summary(); got != want {
		t.Errorf("Summary = %q, want %q", got, want)
	}
	pretty := s.String()
	for _, w := range []string{"shape: (3,)", "col", "i64", "null"} {
		if !strings.Contains(pretty, w) {
			t.Errorf("String missing %q in:\n%s", w, pretty)
		}
	}
}

func TestReleaseIdempotent(t *testing.T) {
	t.Parallel()
	s, _ := series.FromInt64("x", []int64{1}, nil)
	s.Release()
	s.Release() // must not panic on double-release
}
