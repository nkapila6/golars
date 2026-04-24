// Arrow interop: DataFrame ↔ arrow.RecordBatch / arrow.Table.
// Run: go run ./examples/arrow_interop
package main

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	name, _ := series.FromString("name", []string{"ada", "brian"}, nil)
	age, _ := series.FromInt64("age", []int64{27, 34}, nil)
	df, _ := dataframe.New(name, age)
	defer df.Release()

	// Export as a single record batch.
	rec := df.ToArrow()
	defer rec.Release()
	fmt.Printf("record: %d rows, schema: %s\n", rec.NumRows(), rec.Schema())

	// Export as a multi-chunk Table.
	tbl := df.ToArrowTable()
	defer tbl.Release()
	fmt.Printf("table:  %d rows x %d cols\n", tbl.NumRows(), tbl.NumCols())

	// Round-trip the table back to a DataFrame.
	back, err := dataframe.FromArrowTable(tbl)
	if err != nil {
		panic(err)
	}
	defer back.Release()
	fmt.Println(back)
}
