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

// buildTimestampSeries writes a sorted ms-resolution timestamp col.
// The caller provides values in ms since epoch.
func buildTimestampSeries(t *testing.T, name string, ms []int64) *series.Series {
	t.Helper()
	mem := memory.DefaultAllocator
	tst := &arrow.TimestampType{Unit: arrow.Millisecond}
	b := array.NewTimestampBuilder(mem, tst)
	defer b.Release()
	for _, v := range ms {
		b.Append(arrow.Timestamp(v))
	}
	s, err := series.New(name, b.NewArray())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestUpsampleDaily(t *testing.T) {
	// Three source rows spaced 2 days apart (ms precision). Upsample
	// at 1d should produce 5 rows total, with rows 1 and 3 null on
	// the value column.
	const day = int64(86400 * 1000)
	ts := buildTimestampSeries(t, "ts", []int64{0, 2 * day, 4 * day})
	vals, _ := series.FromInt64("v", []int64{10, 20, 30}, nil)
	df, err := dataframe.New(ts, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	out, err := df.Upsample(context.Background(), "ts", "1d")
	if err != nil {
		t.Fatalf("Upsample: %v", err)
	}
	defer out.Release()

	if out.Height() != 5 {
		t.Fatalf("height = %d, want 5", out.Height())
	}
	// Value column should have 2 nulls (days 1 and 3 missing).
	vs, _ := out.Column("v")
	if vs.NullCount() != 2 {
		t.Errorf("v nullCount = %d, want 2", vs.NullCount())
	}
}

func TestUpsampleBadInterval(t *testing.T) {
	ts := buildTimestampSeries(t, "ts", []int64{0, 1000})
	v, _ := series.FromInt64("v", []int64{1, 2}, nil)
	df, _ := dataframe.New(ts, v)
	defer df.Release()
	for _, bad := range []string{"", "1x", "mo", "1y", "abc"} {
		if _, err := df.Upsample(context.Background(), "ts", bad); err == nil {
			t.Errorf("expected error for interval %q, got nil", bad)
		}
	}
}

func TestUpsampleNonTemporal(t *testing.T) {
	s, _ := series.FromInt64("id", []int64{1, 2}, nil)
	df, _ := dataframe.New(s)
	defer df.Release()
	_, err := df.Upsample(context.Background(), "id", "1d")
	if err == nil {
		t.Fatal("expected non-temporal error")
	}
}
