package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestBuildInt64DirectBasic(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, err := series.BuildInt64Direct("x", 5, mem, func(out []int64) {
		for i := range out {
			out[i] = int64(i * 7)
		}
	})
	if err != nil {
		t.Fatalf("BuildInt64Direct: %v", err)
	}
	defer s.Release()

	if !s.DType().Equal(dtype.Int64()) {
		t.Errorf("dtype = %s, want i64", s.DType())
	}
	if s.Len() != 5 {
		t.Errorf("Len = %d, want 5", s.Len())
	}
	got := s.Chunk(0).(*array.Int64).Int64Values()
	want := []int64{0, 7, 14, 21, 28}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestBuildInt64DirectZeroLength(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, err := series.BuildInt64Direct("empty", 0, mem, func(out []int64) {
		if len(out) != 0 {
			t.Errorf("expected empty slice, got len %d", len(out))
		}
	})
	if err != nil {
		t.Fatalf("BuildInt64Direct zero: %v", err)
	}
	defer s.Release()

	if s.Len() != 0 {
		t.Errorf("Len = %d, want 0", s.Len())
	}
}

func TestBuildInt64DirectLargeAndLeakFree(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	const n = 100_000
	s, err := series.BuildInt64Direct("big", n, mem, func(out []int64) {
		for i := range out {
			out[i] = int64(i)
		}
	})
	if err != nil {
		t.Fatalf("BuildInt64Direct: %v", err)
	}
	if s.Len() != n {
		t.Errorf("Len = %d, want %d", s.Len(), n)
	}
	vals := s.Chunk(0).(*array.Int64).Int64Values()
	if vals[0] != 0 || vals[n-1] != int64(n-1) {
		t.Errorf("boundaries wrong: vals[0]=%d vals[n-1]=%d", vals[0], vals[n-1])
	}
	s.Release()
	// Cleanup will assert zero leaked bytes.
}

func TestBuildInt64DirectNullable(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	valid := []bool{true, false, true, false, true}
	s, err := series.BuildInt64DirectNullable("x", 5, mem, func(out []int64) {
		out[0] = 10
		out[1] = 99 // will be masked as null
		out[2] = 30
		out[3] = 99
		out[4] = 50
	}, valid)
	if err != nil {
		t.Fatalf("BuildInt64DirectNullable: %v", err)
	}
	defer s.Release()

	if s.Len() != 5 {
		t.Errorf("Len = %d, want 5", s.Len())
	}
	if s.NullCount() != 2 {
		t.Errorf("NullCount = %d, want 2", s.NullCount())
	}
	arr := s.Chunk(0).(*array.Int64)
	for i, want := range valid {
		if arr.IsValid(i) != want {
			t.Errorf("IsValid(%d) = %v, want %v", i, arr.IsValid(i), want)
		}
	}
	vals := arr.Int64Values()
	if vals[0] != 10 || vals[2] != 30 || vals[4] != 50 {
		t.Errorf("values wrong: %v", vals)
	}
}

func TestBuildFloat64DirectBasic(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, err := series.BuildFloat64Direct("f", 3, mem, func(out []float64) {
		out[0] = 1.5
		out[1] = 2.5
		out[2] = 3.5
	})
	if err != nil {
		t.Fatalf("BuildFloat64Direct: %v", err)
	}
	defer s.Release()

	if !s.DType().Equal(dtype.Float64()) {
		t.Errorf("dtype = %s, want f64", s.DType())
	}
	vals := s.Chunk(0).(*array.Float64).Float64Values()
	want := []float64{1.5, 2.5, 3.5}
	for i := range want {
		if vals[i] != want[i] {
			t.Errorf("[%d] = %v, want %v", i, vals[i], want[i])
		}
	}
}

func TestBuildBoolDirectBasic(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, err := series.BuildBoolDirect("b", 10, mem, func(bits []byte) {
		// Set bits 0, 3, 5, 7, 9 to true.
		bits[0] = 1<<0 | 1<<3 | 1<<5 | 1<<7
		bits[1] = 1 << (9 - 8)
	})
	if err != nil {
		t.Fatalf("BuildBoolDirect: %v", err)
	}
	defer s.Release()

	arr := s.Chunk(0).(*array.Boolean)
	want := []bool{true, false, false, true, false, true, false, true, false, true}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("[%d] = %v, want %v", i, arr.Value(i), w)
		}
	}
}

func TestBuildInt64DirectCrossPrimitive(t *testing.T) {
	t.Parallel()
	// Round-trip: build via direct, read via normal API, values match.
	mem := testutil.NewCheckedAllocator(t)

	n := 4096
	direct, _ := series.BuildInt64Direct("d", n, mem, func(out []int64) {
		for i := range out {
			out[i] = int64(n - i)
		}
	})
	defer direct.Release()

	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(n - i)
	}
	normal, _ := series.FromInt64("n", vals, nil, series.WithAllocator(mem))
	defer normal.Release()

	dv := direct.Chunk(0).(*array.Int64).Int64Values()
	nv := normal.Chunk(0).(*array.Int64).Int64Values()
	for i := range nv {
		if dv[i] != nv[i] {
			t.Fatalf("mismatch at %d: direct=%d, normal=%d", i, dv[i], nv[i])
		}
	}
}

func TestBuildBoolDirectZeroLength(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, err := series.BuildBoolDirect("b", 0, mem, func(bits []byte) {
		if len(bits) != 0 {
			t.Errorf("expected empty bits, got %d", len(bits))
		}
	})
	if err != nil {
		t.Fatalf("BuildBoolDirect zero: %v", err)
	}
	defer s.Release()
	if s.Len() != 0 {
		t.Errorf("Len = %d, want 0", s.Len())
	}
}

func TestBuildInt64DirectAlignmentBoundary(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	// Exercise sizes that straddle arrow's alignment pad (64 bytes = 8 int64).
	for _, n := range []int{1, 7, 8, 9, 63, 64, 65} {
		s, err := series.BuildInt64Direct("x", n, mem, func(out []int64) {
			for i := range out {
				out[i] = int64(i * 3)
			}
		})
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		vals := s.Chunk(0).(*array.Int64).Int64Values()
		if len(vals) != n {
			t.Errorf("n=%d: got len=%d", n, len(vals))
		}
		for i := range vals {
			if vals[i] != int64(i*3) {
				t.Errorf("n=%d [%d] = %d, want %d", n, i, vals[i], i*3)
				break
			}
		}
		s.Release()
	}
}
