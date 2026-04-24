// Behavioural parity with polars' tests/unit/dataframe/test_equals.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Polars exposes `df.equals(other)` returning bool. golars does not
// yet surface a single DataFrame.Equals method, so these tests assert
// the same semantic contract by comparing the observable shape /
// dtype / logical row sequence: which is what a conforming Equals
// implementation must check.
//
// TODO(polars-parity): implement DataFrame.Equals(other) and
// Series.Equals(other); replace the per-column helpers below with
// direct calls once landed.

package dataframe_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// framesEqual approximates polars' df.equals(other): structural
// comparison of widths, column names, dtypes, and per-row values.
// Nulls must line up; values at null positions are ignored.
func framesEqual(a, b *dataframe.DataFrame) bool {
	if a.Width() != b.Width() || a.Height() != b.Height() {
		return false
	}
	for i := range a.Width() {
		ca := a.ColumnAt(i)
		cb := b.ColumnAt(i)
		if ca.Name() != cb.Name() {
			return false
		}
		if ca.DType().String() != cb.DType().String() {
			return false
		}
		if ca.NullCount() != cb.NullCount() {
			return false
		}
		// Compare values chunk-by-flattened-position for each dtype.
		if !columnValuesEqual(ca, cb) {
			return false
		}
	}
	return true
}

// columnValuesEqual compares two same-dtype, same-length columns by
// their logical row sequence. Correct across differing chunk
// boundaries: polars' equals checks the logical sequence, not the
// physical layout.
func columnValuesEqual(a, b *series.Series) bool {
	if a.Len() != b.Len() {
		return false
	}
	switch a.DType().String() {
	case "i64":
		return flattenInt64(a) == flattenInt64(b)
	case "f64":
		return flattenFloat64(a) == flattenFloat64(b)
	case "str":
		return flattenString(a) == flattenString(b)
	case "bool":
		return flattenBool(a) == flattenBool(b)
	}
	return false
}

// Simple linearised signatures keyed off dtype. They're deliberately
// stringy so the comparison is a single == check. Not ideal for
// large frames (builds a full copy) but fine for test-sized input.

func flattenInt64(s *series.Series) string {
	var b []byte
	for _, c := range s.Chunks() {
		a := c.(*array.Int64)
		for i := range a.Len() {
			if a.IsNull(i) {
				b = append(b, 'N')
			} else {
				b = append(b, 'V')
				v := a.Value(i)
				for range 8 {
					b = append(b, byte(v))
					v >>= 8
				}
			}
		}
	}
	return string(b)
}
func flattenFloat64(s *series.Series) string {
	var b []byte
	for _, c := range s.Chunks() {
		a := c.(*array.Float64)
		for i := range a.Len() {
			if a.IsNull(i) {
				b = append(b, 'N')
			} else {
				b = append(b, 'V')
				// Bit-pack via a uint64 reinterpret for exact compare
				// (including NaN bit-pattern equality).
				bits := float64ToBits(a.Value(i))
				for range 8 {
					b = append(b, byte(bits))
					bits >>= 8
				}
			}
		}
	}
	return string(b)
}
func flattenString(s *series.Series) string {
	var b []byte
	for _, c := range s.Chunks() {
		a := c.(*array.String)
		for i := range a.Len() {
			if a.IsNull(i) {
				b = append(b, 'N')
			} else {
				b = append(b, 'V')
				v := a.Value(i)
				// Length-prefix to disambiguate "a" + "bc" vs "ab" + "c".
				b = append(b, byte(len(v)), byte(len(v)>>8))
				b = append(b, v...)
			}
		}
	}
	return string(b)
}
func flattenBool(s *series.Series) string {
	var b []byte
	for _, c := range s.Chunks() {
		a := c.(*array.Boolean)
		for i := range a.Len() {
			if a.IsNull(i) {
				b = append(b, 'N')
			} else if a.Value(i) {
				b = append(b, 'T')
			} else {
				b = append(b, 'F')
			}
		}
	}
	return string(b)
}
func float64ToBits(v float64) uint64 { return math.Float64bits(v) }

// --- Ported polars scenarios ---------------------------------------

// Polars: test_equals: identical frames compare equal.
func TestParityEqualsIdentical(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := testFrameTriple(t, alloc, []int64{1, 2}, []int64{6, 7}, []string{"x", "y"})
	defer a.Release()
	b := testFrameTriple(t, alloc, []int64{1, 2}, []int64{6, 7}, []string{"x", "y"})
	defer b.Release()

	if !framesEqual(a, b) {
		t.Fatal("identical frames should compare equal")
	}
}

// Polars: differing values break equality.
func TestParityEqualsDifferentValues(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := testFrameTriple(t, alloc, []int64{1, 2}, []int64{6, 7}, []string{"x", "y"})
	defer a.Release()
	b := testFrameTriple(t, alloc, []int64{1, 3}, []int64{6, 7}, []string{"x", "y"})
	defer b.Release()

	if framesEqual(a, b) {
		t.Fatal("frames with differing values should NOT be equal")
	}
}

// Polars: same values, different column names → NOT equal. Renaming
// changes the frame's identity.
func TestParityEqualsDifferentColumnNames(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := testFrameTriple(t, alloc, []int64{1}, []int64{6}, []string{"x"})
	defer a.Release()
	b := testFrameTriple(t, alloc, []int64{1}, []int64{6}, []string{"x"})
	defer b.Release()
	// Rename one column on b.
	bRen, _ := b.Rename("foo", "other")
	defer bRen.Release()
	if framesEqual(a, bRen) {
		t.Fatal("frames with different column names should NOT be equal")
	}
}

// Polars: different heights → NOT equal.
func TestParityEqualsDifferentHeight(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := testFrameTriple(t, alloc, []int64{1}, []int64{6}, []string{"x"})
	defer a.Release()
	b := testFrameTriple(t, alloc, []int64{1, 2}, []int64{6, 7}, []string{"x", "y"})
	defer b.Release()
	if framesEqual(a, b) {
		t.Fatal("different heights must not be equal")
	}
}

// Polars: different widths → NOT equal.
func TestParityEqualsDifferentWidth(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := testFrameTriple(t, alloc, []int64{1}, []int64{6}, []string{"x"})
	defer a.Release()
	bDropped := a.Drop("ham")
	defer bDropped.Release()
	if framesEqual(a, bDropped) {
		t.Fatal("different widths must not be equal")
	}
}

// Polars: an empty frame equals another empty frame of the same
// schema, but not an empty frame of different schema.
func TestParityEqualsEmptyFramesSameSchema(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	x1, _ := series.FromInt64("x", nil, nil, series.WithAllocator(alloc))
	y1, _ := series.FromInt64("y", nil, nil, series.WithAllocator(alloc))
	a, _ := dataframe.New(x1, y1)
	defer a.Release()

	x2, _ := series.FromInt64("x", nil, nil, series.WithAllocator(alloc))
	y2, _ := series.FromInt64("y", nil, nil, series.WithAllocator(alloc))
	b, _ := dataframe.New(x2, y2)
	defer b.Release()

	if !framesEqual(a, b) {
		t.Fatal("same-schema empty frames should be equal")
	}
}

// Polars: row order matters. Two frames with the same rows in
// different order are NOT equal (polars does not auto-sort).
func TestParityEqualsRowOrderMatters(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := testFrameTriple(t, alloc, []int64{1, 2, 3}, []int64{10, 20, 30}, []string{"a", "b", "c"})
	defer a.Release()
	b := testFrameTriple(t, alloc, []int64{3, 2, 1}, []int64{30, 20, 10}, []string{"c", "b", "a"})
	defer b.Release()
	if framesEqual(a, b) {
		t.Fatal("row-order-differing frames should not compare equal")
	}
}

// Polars: nulls at the same positions compare equal; nulls shifted
// do not.
func TestParityEqualsNullPositionsMatter(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a1, _ := series.FromInt64("a",
		[]int64{1, 0, 3}, []bool{true, false, true}, series.WithAllocator(alloc))
	af1, _ := dataframe.New(a1)
	defer af1.Release()

	a2, _ := series.FromInt64("a",
		[]int64{1, 0, 3}, []bool{true, false, true}, series.WithAllocator(alloc))
	af2, _ := dataframe.New(a2)
	defer af2.Release()
	if !framesEqual(af1, af2) {
		t.Fatal("identical null patterns should match")
	}

	// Now shift the null.
	b, _ := series.FromInt64("a",
		[]int64{1, 0, 3}, []bool{false, true, true}, series.WithAllocator(alloc))
	bf, _ := dataframe.New(b)
	defer bf.Release()
	if framesEqual(af1, bf) {
		t.Fatal("null at different positions should NOT be equal")
	}
}

// Polars: dtype differences break equality (e.g. i64 column versus
// f64 column with the same numeric values).
func TestParityEqualsDtypeMismatch(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	i, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	iFrame, _ := dataframe.New(i)
	defer iFrame.Release()
	f, _ := series.FromFloat64("a", []float64{1, 2, 3}, nil, series.WithAllocator(alloc))
	fFrame, _ := dataframe.New(f)
	defer fFrame.Release()

	if framesEqual(iFrame, fFrame) {
		t.Fatal("same numeric content but different dtype should not be equal")
	}
}
