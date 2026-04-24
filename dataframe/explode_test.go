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

// buildListSeries returns a List<Int64> Series with rows:
//
//	[1, 2, 3]
//	[]
//	NULL
//	[10, 20]
func buildListSeries(t *testing.T) *series.Series {
	t.Helper()
	mem := memory.DefaultAllocator
	b := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	defer b.Release()
	vb := b.ValueBuilder().(*array.Int64Builder)
	b.Append(true)
	vb.Append(1)
	vb.Append(2)
	vb.Append(3)
	b.Append(true) // empty
	b.AppendNull()
	b.Append(true)
	vb.Append(10)
	vb.Append(20)
	arr := b.NewListArray()
	s, err := series.New("vals", arr)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestExplodeList(t *testing.T) {
	vals := buildListSeries(t)
	id, _ := series.FromInt64("id", []int64{1, 2, 3, 4}, nil)
	df, err := dataframe.New(id, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	out, err := df.Explode(context.Background(), "vals")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	// Expected shape: 3 from first list + 1 for empty + 1 for null + 2 = 7 rows.
	if got := out.Height(); got != 7 {
		t.Fatalf("height = %d, want 7", got)
	}
	// Column order preserved.
	wantCols := []string{"id", "vals"}
	if got := out.Schema().Names(); !equalStrings(got, wantCols) {
		t.Fatalf("columns = %v, want %v", got, wantCols)
	}
	// id column: each source row repeats the list's length (min 1):
	// id = [1,1,1, 2, 3, 4,4]
	idOut, _ := out.Column("id")
	rows, err := out.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 7 {
		t.Fatalf("rows: got %d want 7", len(rows))
	}
	wantIDs := []int64{1, 1, 1, 2, 3, 4, 4}
	for i, r := range rows {
		got := r[0].(int64)
		if got != wantIDs[i] {
			t.Errorf("row %d id = %d, want %d", i, got, wantIDs[i])
		}
	}
	// vals column: rows 3 (empty) and 4 (null source) should be nulls.
	vs, _ := out.Column("vals")
	if vs.NullCount() != 2 {
		t.Errorf("vals nullCount = %d, want 2", vs.NullCount())
	}
	_ = idOut
}

func TestExplodeNonList(t *testing.T) {
	s, _ := series.FromInt64("id", []int64{1, 2, 3}, nil)
	df, _ := dataframe.New(s)
	defer df.Release()
	_, err := df.Explode(context.Background(), "id")
	if err == nil {
		t.Fatal("expected not-list error")
	}
}
