package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Regression: Filter / Take used to error with `unsupported dtype for
// kernel: Filter does not accept datetime[s]` when a mask was applied
// to a DataFrame that carried a temporal column. These tests pin the
// fix: each temporal dtype gathers through its int32/int64 buffer and
// preserves the source dtype on the output.

func timestampSeries(t *testing.T, mem memory.Allocator, unit arrow.TimeUnit, values []arrow.Timestamp) *series.Series {
	t.Helper()
	tt := &arrow.TimestampType{Unit: unit}
	b := array.NewTimestampBuilder(mem, tt)
	defer b.Release()
	b.AppendValues(values, nil)
	arr := b.NewArray()
	s, err := series.New("ts", arr)
	if err != nil {
		t.Fatalf("series.New: %v", err)
	}
	return s
}

func date32Series(t *testing.T, mem memory.Allocator, values []arrow.Date32) *series.Series {
	t.Helper()
	b := array.NewDate32Builder(mem)
	defer b.Release()
	b.AppendValues(values, nil)
	arr := b.NewArray()
	s, err := series.New("d", arr)
	if err != nil {
		t.Fatalf("series.New: %v", err)
	}
	return s
}

func date64Series(t *testing.T, mem memory.Allocator, values []arrow.Date64) *series.Series {
	t.Helper()
	b := array.NewDate64Builder(mem)
	defer b.Release()
	b.AppendValues(values, nil)
	arr := b.NewArray()
	s, err := series.New("d", arr)
	if err != nil {
		t.Fatalf("series.New: %v", err)
	}
	return s
}

func durationSeries(t *testing.T, mem memory.Allocator, unit arrow.TimeUnit, values []arrow.Duration) *series.Series {
	t.Helper()
	dt := &arrow.DurationType{Unit: unit}
	b := array.NewDurationBuilder(mem, dt)
	defer b.Release()
	b.AppendValues(values, nil)
	arr := b.NewArray()
	s, err := series.New("dur", arr)
	if err != nil {
		t.Fatalf("series.New: %v", err)
	}
	return s
}

func TestFilterTimestamp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s := timestampSeries(t, mem, arrow.Second, []arrow.Timestamp{100, 200, 300, 400, 500})
	mask, _ := series.FromBool("m",
		[]bool{true, false, true, false, true}, nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	got, err := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Filter timestamp: %v", err)
	}
	defer got.Release()

	if got.DType().String() != s.DType().String() {
		t.Fatalf("dtype preserved: want %s, got %s", s.DType(), got.DType())
	}
	if got.Len() != 3 {
		t.Fatalf("len: want 3, got %d", got.Len())
	}
	arr := got.Chunk(0).(*array.Timestamp)
	want := []arrow.Timestamp{100, 300, 500}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("row %d: want %d, got %d", i, w, arr.Value(i))
		}
	}
}

func TestFilterDate32(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s := date32Series(t, mem, []arrow.Date32{10, 20, 30, 40})
	mask, _ := series.FromBool("m",
		[]bool{false, true, true, false}, nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	got, err := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Filter date32: %v", err)
	}
	defer got.Release()

	if got.DType().String() != s.DType().String() {
		t.Fatalf("dtype preserved: want %s, got %s", s.DType(), got.DType())
	}
	arr := got.Chunk(0).(*array.Date32)
	want := []arrow.Date32{20, 30}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("row %d: want %d, got %d", i, w, arr.Value(i))
		}
	}
}

func TestFilterDate64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s := date64Series(t, mem, []arrow.Date64{1000, 2000, 3000})
	mask, _ := series.FromBool("m", []bool{true, false, true}, nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	got, err := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Filter date64: %v", err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Date64)
	want := []arrow.Date64{1000, 3000}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("row %d: want %d, got %d", i, w, arr.Value(i))
		}
	}
}

func TestFilterDuration(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s := durationSeries(t, mem, arrow.Millisecond, []arrow.Duration{50, 75, 100})
	mask, _ := series.FromBool("m", []bool{false, true, true}, nil, series.WithAllocator(mem))
	defer s.Release()
	defer mask.Release()

	got, err := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Filter duration: %v", err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Duration)
	want := []arrow.Duration{75, 100}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("row %d: want %d, got %d", i, w, arr.Value(i))
		}
	}
}

func TestFilterTimestampWithNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	tt := &arrow.TimestampType{Unit: arrow.Second}
	b := array.NewTimestampBuilder(mem, tt)
	defer b.Release()
	b.AppendValues([]arrow.Timestamp{10, 20, 30, 40}, []bool{true, false, true, true})
	arr := b.NewArray()
	s, err := series.New("ts", arr)
	if err != nil {
		t.Fatalf("series.New: %v", err)
	}
	defer s.Release()

	mask, _ := series.FromBool("m",
		[]bool{true, true, false, true}, nil, series.WithAllocator(mem))
	defer mask.Release()

	got, err := compute.Filter(ctx, s, mask, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Filter with nulls: %v", err)
	}
	defer got.Release()

	tsArr := got.Chunk(0).(*array.Timestamp)
	if got.Len() != 3 {
		t.Fatalf("len: want 3, got %d", got.Len())
	}
	// Expect [10, null, 40]: positions 0, 1, 3 of source (the null at 1 is preserved).
	if tsArr.IsNull(0) || tsArr.Value(0) != 10 {
		t.Errorf("row 0: want 10 non-null, got valid=%v value=%d", tsArr.IsValid(0), tsArr.Value(0))
	}
	if !tsArr.IsNull(1) {
		t.Errorf("row 1: want null, got valid=%v", tsArr.IsValid(1))
	}
	if tsArr.IsNull(2) || tsArr.Value(2) != 40 {
		t.Errorf("row 2: want 40 non-null, got valid=%v value=%d", tsArr.IsValid(2), tsArr.Value(2))
	}
}

func TestTakeTimestamp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	s := timestampSeries(t, mem, arrow.Millisecond, []arrow.Timestamp{10, 20, 30, 40, 50})
	defer s.Release()

	got, err := compute.Take(ctx, s, []int{4, 2, 0}, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Take timestamp: %v", err)
	}
	defer got.Release()

	arr := got.Chunk(0).(*array.Timestamp)
	want := []arrow.Timestamp{50, 30, 10}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("row %d: want %d, got %d", i, w, arr.Value(i))
		}
	}
}
