package dataframe_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/selector"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestDataFrameSelectByAndDropBy(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("id", []int64{1, 2}, nil, series.WithAllocator(alloc))
	b, _ := series.FromString("name", []string{"x", "y"}, nil, series.WithAllocator(alloc))
	c, _ := series.FromFloat64("salary_usd", []float64{100, 200}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	num, err := df.SelectBy(selector.Numeric())
	if err != nil {
		t.Fatal(err)
	}
	defer num.Release()
	if num.Width() != 2 {
		t.Errorf("SelectBy(Numeric) width = %d, want 2", num.Width())
	}

	noSalary := df.DropBy(selector.StartsWith("salary_"))
	defer noSalary.Release()
	if noSalary.Width() != 2 {
		t.Errorf("DropBy(StartsWith) width = %d, want 2", noSalary.Width())
	}
	for _, n := range noSalary.ColumnNames() {
		if n == "salary_usd" {
			t.Errorf("salary_usd still present after DropBy")
		}
	}
}
