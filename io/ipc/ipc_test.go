package ipc_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/ipc"
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
	d, _ := series.FromBool("d", []bool{true, false, true, false, true}, nil, series.WithAllocator(mem))
	df, err := dataframe.New(a, b, c, d)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return df
}

func TestIPCRoundTrip(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := sampleDF(t, mem)
	defer df.Release()

	var buf bytes.Buffer
	if err := ipc.Write(ctx, &buf, df, ipc.WithAllocator(mem)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := ipc.Read(ctx, &buf, ipc.WithAllocator(mem))
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
	if !got.Schema().Equal(df.Schema()) {
		t.Errorf("Schema:\n  got  %s\n  want %s", got.Schema(), df.Schema())
	}

	aCol, _ := got.Column("a")
	aVals := aCol.Chunk(0).(*array.Int64).Int64Values()
	for i, v := range aVals {
		if v != int64(i+1) {
			t.Errorf("a[%d] = %d, want %d", i, v, i+1)
		}
	}

	cCol, _ := got.Column("c")
	cArr := cCol.Chunk(0).(*array.String)
	wantStrs := []string{"v", "w", "x", "y", "z"}
	for i, w := range wantStrs {
		if g := cArr.Value(i); g != w {
			t.Errorf("c[%d] = %q, want %q", i, g, w)
		}
	}
}

func TestIPCFileRoundTrip(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := sampleDF(t, mem)
	defer df.Release()

	path := filepath.Join(t.TempDir(), "out.arrow")
	if err := ipc.WriteFile(ctx, path, df, ipc.WithAllocator(mem)); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := ipc.ReadFile(ctx, path, ipc.WithAllocator(mem))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	defer got.Release()

	if got.Height() != df.Height() {
		t.Errorf("Height = %d, want %d", got.Height(), df.Height())
	}
}

func TestIPCWithNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(mem))
	df, err := dataframe.New(a)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer df.Release()

	var buf bytes.Buffer
	if err := ipc.Write(ctx, &buf, df, ipc.WithAllocator(mem)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := ipc.Read(ctx, &buf, ipc.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer got.Release()

	aCol, _ := got.Column("a")
	if aCol.NullCount() != 2 {
		t.Errorf("NullCount = %d, want 2", aCol.NullCount())
	}
}

func BenchmarkIPCWriteRead(b *testing.B) {
	ctx := context.Background()
	const n = 1 << 16
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
		if err := ipc.Write(ctx, &buf, df); err != nil {
			b.Fatal(err)
		}
		got, err := ipc.Read(ctx, &buf)
		if err != nil {
			b.Fatal(err)
		}
		got.Release()
	}
}
