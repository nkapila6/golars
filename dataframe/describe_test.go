package dataframe_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestDescribeNumeric(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{2.0, 2.0, 2.0, 2.0, 2.0}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	stats, err := df.Describe(ctx)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	defer stats.Release()

	if stats.Height() != 9 {
		t.Errorf("Describe height = %d, want 9", stats.Height())
	}
	// Columns: statistic + 2 data columns.
	if stats.Width() != 3 {
		t.Errorf("Describe width = %d, want 3", stats.Width())
	}
	// Column "a" mean should be 3.
	aCol, _ := stats.Column("a")
	if v, ok := valueAt(t, aCol, 2); !ok || v != 3.0 {
		t.Errorf("a mean = %v (ok=%v), want 3.0", v, ok)
	}
	// Column "b" std = 0 (all identical).
	bCol, _ := stats.Column("b")
	if v, ok := valueAt(t, bCol, 3); !ok || v != 0.0 {
		t.Errorf("b std = %v (ok=%v), want 0.0", v, ok)
	}
}

func valueAt(t *testing.T, s *series.Series, i int) (float64, bool) {
	t.Helper()
	chunk := s.Chunk(0)
	if chunk.IsNull(i) {
		return 0, false
	}
	return float64FromArrow(t, chunk, i), true
}

func float64FromArrow(t *testing.T, chunk any, i int) float64 {
	t.Helper()
	type f64Arr interface {
		Value(int) float64
	}
	type i64Arr interface {
		Value(int) int64
	}
	switch a := chunk.(type) {
	case f64Arr:
		return a.Value(i)
	case i64Arr:
		return float64(a.Value(i))
	}
	t.Fatalf("unsupported chunk type: %T", chunk)
	return 0
}

func TestDescribeWithNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// [1, null, 3, null, 5] -> non-null=3, null=2, mean=3
	a, _ := series.FromInt64("a", []int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true}, series.WithAllocator(mem))
	df, _ := dataframe.New(a)
	defer df.Release()

	stats, err := df.Describe(ctx)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	defer stats.Release()
	aCol, _ := stats.Column("a")

	// count = 3
	if v, ok := valueAt(t, aCol, 0); !ok || v != 3 {
		t.Errorf("count = %v, want 3", v)
	}
	// null_count = 2
	if v, ok := valueAt(t, aCol, 1); !ok || v != 2 {
		t.Errorf("null_count = %v, want 2", v)
	}
	// mean = 3
	if v, ok := valueAt(t, aCol, 2); !ok || v != 3 {
		t.Errorf("mean = %v, want 3", v)
	}
}

func TestDescribeNonNumericColumn(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	names, _ := series.FromString("name", []string{"a", "b", "c"}, nil, series.WithAllocator(mem))
	nums, _ := series.FromInt64("n", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(names, nums)
	defer df.Release()

	stats, err := df.Describe(ctx)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	defer stats.Release()

	// String column "name" should show count/null_count and null for others.
	nameCol, _ := stats.Column("name")
	if nameCol.NullCount() != 7 {
		t.Errorf("name NullCount = %d, want 7 (count+null_count populated, 7 others null)",
			nameCol.NullCount())
	}
}
