package csv_test

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestCSVRoundTripInferred(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{1.5, 2.5, 3.5}, nil, series.WithAllocator(mem))
	c, _ := series.FromString("c", []string{"x", "y", "z"}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	var buf bytes.Buffer
	if err := iocsv.Write(ctx, &buf, df, iocsv.WithAllocator(mem)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if !strings.Contains(buf.String(), "a,b,c") {
		t.Errorf("missing header in output: %q", buf.String())
	}

	got, err := iocsv.Read(ctx, &buf, iocsv.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer got.Release()

	if got.Width() != 3 {
		t.Errorf("Width = %d, want 3", got.Width())
	}
	if got.Height() != 3 {
		t.Errorf("Height = %d, want 3", got.Height())
	}
}

func TestCSVReadWithSchema(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	input := "x,y\n1,10\n2,20\n3,30\n"
	sch, _ := schema.New(
		schema.Field{Name: "x", DType: dtype.Int64()},
		schema.Field{Name: "y", DType: dtype.Int64()},
	)
	got, err := iocsv.Read(ctx, strings.NewReader(input),
		iocsv.WithAllocator(mem), iocsv.WithSchema(sch))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer got.Release()

	if got.Height() != 3 {
		t.Errorf("Height = %d, want 3", got.Height())
	}
	xCol, _ := got.Column("x")
	if !xCol.DType().Equal(dtype.Int64()) {
		t.Errorf("DType = %s, want i64", xCol.DType())
	}
	vals := xCol.Chunk(0).(*array.Int64).Int64Values()
	if len(vals) != 3 || vals[0] != 1 || vals[2] != 3 {
		t.Errorf("x values = %v, want [1 2 3]", vals)
	}
}

func TestCSVWithNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	input := "a,b\n1,x\n,y\n3,\n"
	got, err := iocsv.Read(ctx, strings.NewReader(input),
		iocsv.WithAllocator(mem),
		iocsv.WithNullValues(""))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer got.Release()

	aCol, _ := got.Column("a")
	if aCol.NullCount() != 1 {
		t.Errorf("a NullCount = %d, want 1", aCol.NullCount())
	}
	bCol, _ := got.Column("b")
	if bCol.NullCount() != 1 {
		t.Errorf("b NullCount = %d, want 1", bCol.NullCount())
	}
}

func TestCSVFileRoundTrip(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	defer df.Release()

	path := filepath.Join(t.TempDir(), "out.csv")
	if err := iocsv.WriteFile(ctx, path, df, iocsv.WithAllocator(mem)); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := iocsv.ReadFile(ctx, path, iocsv.WithAllocator(mem))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	defer got.Release()

	if got.Height() != 3 {
		t.Errorf("Height = %d, want 3", got.Height())
	}
}

func TestCSVDelimiter(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	input := "x|y\n1|2\n3|4\n"
	got, err := iocsv.Read(ctx, strings.NewReader(input),
		iocsv.WithAllocator(mem), iocsv.WithDelimiter('|'))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer got.Release()

	if got.Width() != 2 {
		t.Errorf("Width = %d, want 2", got.Width())
	}
}

func BenchmarkCSVWriteRead(b *testing.B) {
	ctx := context.Background()
	const n = 1 << 13
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i)
	}
	s, _ := series.FromInt64("x", vals, nil)
	df, _ := dataframe.New(s)
	defer df.Release()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	for b.Loop() {
		var buf bytes.Buffer
		if err := iocsv.Write(ctx, &buf, df); err != nil {
			b.Fatal(err)
		}
		got, err := iocsv.Read(ctx, &buf)
		if err != nil {
			b.Fatal(err)
		}
		got.Release()
	}
}
