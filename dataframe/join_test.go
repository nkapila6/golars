package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestInnerJoinInt64Key(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// left: (id, value)
	id1, _ := series.FromInt64("id", []int64{1, 2, 3, 4}, nil, series.WithAllocator(mem))
	v1, _ := series.FromString("value", []string{"a", "b", "c", "d"}, nil, series.WithAllocator(mem))
	left, _ := dataframe.New(id1, v1)
	defer left.Release()

	// right: (id, qty)
	id2, _ := series.FromInt64("id", []int64{2, 2, 4, 5}, nil, series.WithAllocator(mem))
	q, _ := series.FromInt64("qty", []int64{10, 20, 30, 40}, nil, series.WithAllocator(mem))
	right, _ := dataframe.New(id2, q)
	defer right.Release()

	got, err := left.Join(ctx, right, []string{"id"}, dataframe.InnerJoin,
		dataframe.WithJoinAllocator(mem))
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	defer got.Release()

	// Expected: id=2 matches twice, id=4 once.
	if got.Height() != 3 {
		t.Fatalf("Height = %d, want 3", got.Height())
	}
	if got.Width() != 3 {
		t.Errorf("Width = %d, want 3", got.Width())
	}

	idCol, _ := got.Column("id")
	ids := idCol.Chunk(0).(*array.Int64).Int64Values()
	qtyCol, _ := got.Column("qty")
	qtys := qtyCol.Chunk(0).(*array.Int64).Int64Values()

	// Emissions: (id=2, qty=10), (id=2, qty=20), (id=4, qty=30).
	wantIDs := []int64{2, 2, 4}
	wantQty := []int64{10, 20, 30}
	for i := range wantIDs {
		if ids[i] != wantIDs[i] {
			t.Errorf("id[%d] = %d, want %d", i, ids[i], wantIDs[i])
		}
		if qtys[i] != wantQty[i] {
			t.Errorf("qty[%d] = %d, want %d", i, qtys[i], wantQty[i])
		}
	}
}

func TestLeftJoinStringKey(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	k1, _ := series.FromString("k", []string{"a", "b", "c"}, nil, series.WithAllocator(mem))
	v1, _ := series.FromInt64("v", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	left, _ := dataframe.New(k1, v1)
	defer left.Release()

	k2, _ := series.FromString("k", []string{"a", "c", "d"}, nil, series.WithAllocator(mem))
	w, _ := series.FromInt64("w", []int64{10, 30, 40}, nil, series.WithAllocator(mem))
	right, _ := dataframe.New(k2, w)
	defer right.Release()

	got, err := left.Join(ctx, right, []string{"k"}, dataframe.LeftJoin,
		dataframe.WithJoinAllocator(mem))
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	defer got.Release()

	if got.Height() != 3 {
		t.Errorf("Height = %d, want 3", got.Height())
	}

	wCol, _ := got.Column("w")
	if wCol.NullCount() != 1 {
		t.Errorf("w NullCount = %d, want 1", wCol.NullCount())
	}

	wVals := wCol.Chunk(0).(*array.Int64).Int64Values()
	// Rows: ("a", 1, 10), ("b", 2, null), ("c", 3, 30).
	if wVals[0] != 10 || wVals[2] != 30 {
		t.Errorf("w = %v, want [10, _, 30]", wVals)
	}
}

func TestJoinColumnCollision(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// Both sides have "value" columns; the right-side must be suffixed.
	k1, _ := series.FromInt64("k", []int64{1, 2}, nil, series.WithAllocator(mem))
	v1, _ := series.FromString("value", []string{"x", "y"}, nil, series.WithAllocator(mem))
	left, _ := dataframe.New(k1, v1)
	defer left.Release()

	k2, _ := series.FromInt64("k", []int64{1, 2}, nil, series.WithAllocator(mem))
	v2, _ := series.FromString("value", []string{"X", "Y"}, nil, series.WithAllocator(mem))
	right, _ := dataframe.New(k2, v2)
	defer right.Release()

	got, err := left.Join(ctx, right, []string{"k"}, dataframe.InnerJoin,
		dataframe.WithJoinAllocator(mem))
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	defer got.Release()

	if !got.Contains("value") || !got.Contains("value_right") {
		t.Errorf("missing collision-suffixed columns: %v", got.Schema().Names())
	}
}

func TestJoinDTypeMismatch(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	k1, _ := series.FromInt64("k", []int64{1}, nil, series.WithAllocator(mem))
	left, _ := dataframe.New(k1)
	defer left.Release()

	k2, _ := series.FromFloat64("k", []float64{1}, nil, series.WithAllocator(mem))
	right, _ := dataframe.New(k2)
	defer right.Release()

	_, err := left.Join(ctx, right, []string{"k"}, dataframe.InnerJoin,
		dataframe.WithJoinAllocator(mem))
	if err == nil {
		t.Fatal("expected dtype mismatch error")
	}
}

func TestCrossJoin(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(mem))
	left, _ := dataframe.New(a)
	defer left.Release()

	b, _ := series.FromString("b", []string{"x", "y", "z"}, nil, series.WithAllocator(mem))
	right, _ := dataframe.New(b)
	defer right.Release()

	got, err := left.Join(ctx, right, nil, dataframe.CrossJoin,
		dataframe.WithJoinAllocator(mem))
	if err != nil {
		t.Fatalf("CrossJoin: %v", err)
	}
	defer got.Release()

	if got.Height() != 6 {
		t.Errorf("Height = %d, want 6", got.Height())
	}
	if got.Width() != 2 {
		t.Errorf("Width = %d, want 2", got.Width())
	}
}

func TestInnerJoinNullKeysNeverMatch(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// Both sides have null keys that should not match each other.
	k1, _ := series.FromInt64("k",
		[]int64{1, 0, 3},
		[]bool{true, false, true},
		series.WithAllocator(mem))
	v1, _ := series.FromInt64("v", []int64{10, 20, 30}, nil, series.WithAllocator(mem))
	left, _ := dataframe.New(k1, v1)
	defer left.Release()

	k2, _ := series.FromInt64("k",
		[]int64{0, 3},
		[]bool{false, true},
		series.WithAllocator(mem))
	w, _ := series.FromInt64("w", []int64{99, 300}, nil, series.WithAllocator(mem))
	right, _ := dataframe.New(k2, w)
	defer right.Release()

	got, _ := left.Join(ctx, right, []string{"k"}, dataframe.InnerJoin,
		dataframe.WithJoinAllocator(mem))
	defer got.Release()

	// Only k=3 matches; nulls on both sides must not join.
	if got.Height() != 1 {
		t.Errorf("Height = %d, want 1", got.Height())
	}
	vCol, _ := got.Column("v")
	wCol, _ := got.Column("w")
	if vCol.Chunk(0).(*array.Int64).Int64Values()[0] != 30 {
		t.Errorf("v = %d, want 30", vCol.Chunk(0).(*array.Int64).Int64Values()[0])
	}
	if wCol.Chunk(0).(*array.Int64).Int64Values()[0] != 300 {
		t.Errorf("w = %d, want 300", wCol.Chunk(0).(*array.Int64).Int64Values()[0])
	}
}

func BenchmarkInnerJoinN(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 14, 1 << 16, 1 << 18} {
		b.Run(itoa(n), func(b *testing.B) {
			leftKey := make([]int64, n)
			leftVal := make([]int64, n)
			rightKey := make([]int64, n)
			rightVal := make([]int64, n)
			for i := range leftKey {
				leftKey[i] = int64(i)
				leftVal[i] = int64(i * 2)
				rightKey[i] = int64(i)
				rightVal[i] = int64(i * 3)
			}
			lk, _ := series.FromInt64("id", leftKey, nil)
			lv, _ := series.FromInt64("lv", leftVal, nil)
			left, _ := dataframe.New(lk, lv)
			rk, _ := series.FromInt64("id", rightKey, nil)
			rv, _ := series.FromInt64("rv", rightVal, nil)
			right, _ := dataframe.New(rk, rv)
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 16)
			for b.Loop() {
				out, err := left.Join(ctx, right, []string{"id"}, dataframe.InnerJoin)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
			left.Release()
			right.Release()
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func BenchmarkInnerJoin(b *testing.B) {
	ctx := context.Background()
	const n = 1 << 14

	leftKey := make([]int64, n)
	leftVal := make([]int64, n)
	for i := range leftKey {
		leftKey[i] = int64(i)
		leftVal[i] = int64(i * 2)
	}
	rightKey := make([]int64, n)
	rightVal := make([]int64, n)
	for i := range rightKey {
		rightKey[i] = int64(i)
		rightVal[i] = int64(i * 3)
	}

	lk, _ := series.FromInt64("id", leftKey, nil)
	lv, _ := series.FromInt64("lv", leftVal, nil)
	left, _ := dataframe.New(lk, lv)
	defer left.Release()

	rk, _ := series.FromInt64("id", rightKey, nil)
	rv, _ := series.FromInt64("rv", rightVal, nil)
	right, _ := dataframe.New(rk, rv)
	defer right.Release()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(n) * 16)
	for b.Loop() {
		out, err := left.Join(ctx, right, []string{"id"}, dataframe.InnerJoin)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
