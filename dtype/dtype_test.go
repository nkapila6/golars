package dtype_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dtype"
)

func TestPrimitiveConstructors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		dt      dtype.DType
		wantID  arrow.Type
		wantStr string
	}{
		{"null", dtype.Null(), arrow.NULL, "null"},
		{"bool", dtype.Bool(), arrow.BOOL, "bool"},
		{"i8", dtype.Int8(), arrow.INT8, "i8"},
		{"i16", dtype.Int16(), arrow.INT16, "i16"},
		{"i32", dtype.Int32(), arrow.INT32, "i32"},
		{"i64", dtype.Int64(), arrow.INT64, "i64"},
		{"u8", dtype.Uint8(), arrow.UINT8, "u8"},
		{"u16", dtype.Uint16(), arrow.UINT16, "u16"},
		{"u32", dtype.Uint32(), arrow.UINT32, "u32"},
		{"u64", dtype.Uint64(), arrow.UINT64, "u64"},
		{"f32", dtype.Float32(), arrow.FLOAT32, "f32"},
		{"f64", dtype.Float64(), arrow.FLOAT64, "f64"},
		{"str", dtype.String(), arrow.STRING, "str"},
		{"binary", dtype.Binary(), arrow.BINARY, "binary"},
		{"date", dtype.Date(), arrow.DATE32, "date"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !tc.dt.IsValid() {
				t.Fatal("expected IsValid to be true")
			}
			if got := tc.dt.ID(); got != tc.wantID {
				t.Errorf("ID() = %v, want %v", got, tc.wantID)
			}
			if got := tc.dt.String(); got != tc.wantStr {
				t.Errorf("String() = %q, want %q", got, tc.wantStr)
			}
		})
	}
}

func TestZeroValueInvalid(t *testing.T) {
	t.Parallel()
	var d dtype.DType
	if d.IsValid() {
		t.Error("zero DType should not be valid")
	}
	if got := d.String(); got != "<invalid>" {
		t.Errorf("String() = %q, want <invalid>", got)
	}
	var e dtype.DType
	if !d.Equal(e) {
		t.Error("two zero DTypes should be equal")
	}
}

func TestDatetimeFormatting(t *testing.T) {
	t.Parallel()

	naive := dtype.Datetime(dtype.Microsecond, "")
	if got, want := naive.String(), "datetime[us]"; got != want {
		t.Errorf("naive String() = %q, want %q", got, want)
	}

	tz := dtype.Datetime(dtype.Nanosecond, "Asia/Tokyo")
	if got, want := tz.String(), "datetime[ns, Asia/Tokyo]"; got != want {
		t.Errorf("tz String() = %q, want %q", got, want)
	}

	if naive.Equal(tz) {
		t.Error("datetime[us] should not equal datetime[ns, Asia/Tokyo]")
	}
}

func TestDurationFormatting(t *testing.T) {
	t.Parallel()
	d := dtype.Duration(dtype.Millisecond)
	if got, want := d.String(), "duration[ms]"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestListNested(t *testing.T) {
	t.Parallel()
	l := dtype.List(dtype.Int32())
	if got, want := l.String(), "list[i32]"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if !l.IsNested() {
		t.Error("list dtype should be nested")
	}

	nested := dtype.List(dtype.List(dtype.Float64()))
	if got, want := nested.String(), "list[list[f64]]"; got != want {
		t.Errorf("nested String() = %q, want %q", got, want)
	}
}

func TestFixedList(t *testing.T) {
	t.Parallel()
	fl := dtype.FixedList(dtype.Int64(), 4)
	if got, want := fl.String(), "list[i64; 4]"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if !fl.IsNested() {
		t.Error("fixed list should be nested")
	}
}

func TestStruct(t *testing.T) {
	t.Parallel()
	s := dtype.Struct(
		dtype.StructField{Name: "a", DType: dtype.Int64()},
		dtype.StructField{Name: "b", DType: dtype.String()},
	)
	if got, want := s.String(), "struct{a: i64, b: str}"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if !s.IsNested() {
		t.Error("struct should be nested")
	}
}

func TestClassification(t *testing.T) {
	t.Parallel()

	cases := []struct {
		dt                                                                            dtype.DType
		isNull, isBool, isInt, isFloat, isNumeric, isString, isBinary, isTemp, isNest bool
	}{
		{dtype.Null(), true, false, false, false, false, false, false, false, false},
		{dtype.Bool(), false, true, false, false, false, false, false, false, false},
		{dtype.Int64(), false, false, true, false, true, false, false, false, false},
		{dtype.Float64(), false, false, false, true, true, false, false, false, false},
		{dtype.String(), false, false, false, false, false, true, false, false, false},
		{dtype.Binary(), false, false, false, false, false, false, true, false, false},
		{dtype.Date(), false, false, false, false, false, false, false, true, false},
		{dtype.Datetime(dtype.Microsecond, ""), false, false, false, false, false, false, false, true, false},
		{dtype.Duration(dtype.Second), false, false, false, false, false, false, false, true, false},
		{dtype.List(dtype.Int64()), false, false, false, false, false, false, false, false, true},
	}

	for _, c := range cases {
		t.Run(c.dt.String(), func(t *testing.T) {
			t.Parallel()
			check(t, "IsNull", c.dt.IsNull(), c.isNull)
			check(t, "IsBool", c.dt.IsBool(), c.isBool)
			check(t, "IsInteger", c.dt.IsInteger(), c.isInt)
			check(t, "IsFloating", c.dt.IsFloating(), c.isFloat)
			check(t, "IsNumeric", c.dt.IsNumeric(), c.isNumeric)
			check(t, "IsString", c.dt.IsString(), c.isString)
			check(t, "IsBinary", c.dt.IsBinary(), c.isBinary)
			check(t, "IsTemporal", c.dt.IsTemporal(), c.isTemp)
			check(t, "IsNested", c.dt.IsNested(), c.isNest)
		})
	}
}

func check(t *testing.T, name string, got, want bool) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func TestFromArrowRoundtrip(t *testing.T) {
	t.Parallel()
	inputs := []dtype.DType{
		dtype.Int32(),
		dtype.Float64(),
		dtype.String(),
		dtype.Date(),
		dtype.Datetime(dtype.Nanosecond, "UTC"),
		dtype.List(dtype.Int64()),
	}
	for _, d := range inputs {
		roundtripped := dtype.FromArrow(d.Arrow())
		if !d.Equal(roundtripped) {
			t.Errorf("roundtrip %s lost equality", d)
		}
	}
}

func TestFromArrowNilPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil arrow.DataType")
		}
	}()
	dtype.FromArrow(nil)
}

func TestListWithInvalidInnerPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on invalid inner dtype")
		}
	}()
	var invalid dtype.DType
	dtype.List(invalid)
}
