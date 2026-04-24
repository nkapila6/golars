package parquet_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/compress"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

func sampleDF(t *testing.T, mem interface {
	Allocate(size int) []byte
	Reallocate(size int, b []byte) []byte
	Free(b []byte)
}) *dataframe.DataFrame {
	t.Helper()
	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{1.5, 2.5, 3.5, 4.5, 5.5}, nil, series.WithAllocator(mem))
	c, _ := series.FromString("c", []string{"v", "w", "x", "y", "z"}, nil, series.WithAllocator(mem))
	df, err := dataframe.New(a, b, c)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return df
}

func TestParquetRoundTrip(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := sampleDF(t, mem)
	defer df.Release()

	var buf bytes.Buffer
	if err := parquet.Write(ctx, &buf, df, parquet.WithAllocator(mem)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := parquet.ReadBytes(ctx, buf.Bytes(), parquet.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer got.Release()

	if got.Height() != df.Height() {
		t.Errorf("Height = %d, want %d", got.Height(), df.Height())
	}
	if got.Width() != df.Width() {
		t.Errorf("Width = %d, want %d", got.Width(), df.Width())
	}

	aCol, _ := got.Column("a")
	chunks := aCol.Chunks()
	var all []int64
	for _, c := range chunks {
		all = append(all, c.(*array.Int64).Int64Values()...)
	}
	if len(all) != 5 || all[0] != 1 || all[4] != 5 {
		t.Errorf("a values = %v, want [1..5]", all)
	}
}

func TestParquetFileRoundTrip(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := sampleDF(t, mem)
	defer df.Release()

	path := filepath.Join(t.TempDir(), "out.parquet")
	if err := parquet.WriteFile(ctx, path, df, parquet.WithAllocator(mem)); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := parquet.ReadFile(ctx, path, parquet.WithAllocator(mem))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	defer got.Release()

	if got.Height() != df.Height() {
		t.Errorf("Height = %d, want %d", got.Height(), df.Height())
	}
}

func TestParquetNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	defer df.Release()

	var buf bytes.Buffer
	if err := parquet.Write(ctx, &buf, df, parquet.WithAllocator(mem)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := parquet.ReadBytes(ctx, buf.Bytes(), parquet.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer got.Release()

	aCol, _ := got.Column("a")
	if aCol.NullCount() != 2 {
		t.Errorf("NullCount = %d, want 2", aCol.NullCount())
	}
}

func TestParquetCompressionOptions(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := sampleDF(t, mem)
	defer df.Release()

	for _, codec := range []compress.Compression{
		compress.Codecs.Uncompressed,
		compress.Codecs.Snappy,
		compress.Codecs.Gzip,
		compress.Codecs.Zstd,
	} {
		t.Run(codec.String(), func(t *testing.T) {
			var buf bytes.Buffer
			if err := parquet.Write(ctx, &buf, df,
				parquet.WithAllocator(mem),
				parquet.WithCompression(codec)); err != nil {
				t.Fatalf("Write: %v", err)
			}
			got, err := parquet.ReadBytes(ctx, buf.Bytes(), parquet.WithAllocator(mem))
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			defer got.Release()
			if got.Height() != df.Height() {
				t.Errorf("Height = %d, want %d", got.Height(), df.Height())
			}
		})
	}
}

func BenchmarkParquetWriteRead(b *testing.B) {
	ctx := context.Background()
	const n = 1 << 14
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
		if err := parquet.Write(ctx, &buf, df); err != nil {
			b.Fatal(err)
		}
		got, err := parquet.ReadBytes(ctx, buf.Bytes())
		if err != nil {
			b.Fatal(err)
		}
		got.Release()
	}
}
