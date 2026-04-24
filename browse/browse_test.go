package browse

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestModelCellAccessDoesNotMaterialise verifies the model pulls
// cells lazily: no matter how large the frame, creating the model
// should not allocate a full copy of the data.
func TestModelLazyCellAccess(t *testing.T) {
	n := 100_000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i)
	}
	s, _ := series.FromInt64("x", vals, nil)
	df, _ := dataframe.New(s)
	defer df.Release()

	m := newModel(df, "test.csv")
	if got := m.rowCount(); got != n {
		t.Fatalf("rowCount = %d, want %d", got, n)
	}
	// Sanity: random reads into the middle of the frame return the
	// correct stringified cell without materialising anything.
	if got := m.cellAt(42_000, 0); got != "42000" {
		t.Fatalf("cellAt = %q, want %q", got, "42000")
	}
}

// TestFilterAndSort drives the model through an apply-filter pass
// and checks the view is filtered and sort order is correct.
func TestFilterAndSort(t *testing.T) {
	names := []string{"alpha", "beta", "charlie", "delta"}
	ages := []int64{30, 20, 40, 10}
	name, _ := series.FromString("name", names, nil)
	age, _ := series.FromInt64("age", ages, nil)
	df, _ := dataframe.New(name, age)
	defer df.Release()

	m := newModel(df, "test")

	// Filter to rows containing "ha" - only charlie and alpha.
	m.filter = "ha"
	m.applyView()
	if got := m.rowCount(); got != 2 {
		t.Fatalf("rowCount = %d, want 2", got)
	}

	// Sort ascending by age on the filtered view: alpha (30) before charlie (40).
	m.sort = sortKey{idx: 1, desc: false}
	m.applyView()
	if m.cellAt(0, 0) != "alpha" || m.cellAt(1, 0) != "charlie" {
		t.Fatalf("asc sort wrong: %q %q", m.cellAt(0, 0), m.cellAt(1, 0))
	}

	// Flip descending.
	m.sort.desc = true
	m.applyView()
	if m.cellAt(0, 0) != "charlie" {
		t.Fatalf("desc head = %q, want charlie", m.cellAt(0, 0))
	}

	// Clear.
	m.filter = ""
	m.sort = sortKey{idx: -1}
	m.applyView()
	if m.rowCount() != 4 {
		t.Fatalf("cleared rowCount = %d, want 4", m.rowCount())
	}
}

// TestHideFreezeColumns exercises the visibility/ordering helpers.
func TestHideFreezeColumns(t *testing.T) {
	a, _ := series.FromInt64("a", []int64{1, 2}, nil)
	b, _ := series.FromInt64("b", []int64{1, 2}, nil)
	c, _ := series.FromInt64("c", []int64{1, 2}, nil)
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	m := newModel(df, "x")
	m.cols[1].hidden = true
	if got := m.visibleCols(); len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("hidden: got %v, want [0 2]", got)
	}
	m.cols[2].frozen = true
	if got := m.visibleCols(); got[0] != 2 || got[1] != 0 {
		t.Fatalf("frozen: got %v, want [2 0]", got)
	}
}
