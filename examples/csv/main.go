// Write a DataFrame to CSV and read it back.
// Run: go run ./examples/csv
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	name, _ := series.FromString("name", []string{"ada", "brian", "carl"}, nil)
	age, _ := series.FromInt64("age", []int64{27, 34, 19}, nil)
	df, _ := dataframe.New(name, age)
	defer df.Release()

	tmp, _ := os.MkdirTemp("", "golars-csv")
	defer os.RemoveAll(tmp)
	path := filepath.Join(tmp, "people.csv")

	if err := csv.WriteFile(ctx, path, df); err != nil {
		panic(err)
	}

	back, err := csv.ReadFile(ctx, path)
	if err != nil {
		panic(err)
	}
	defer back.Release()
	fmt.Println("round-tripped via CSV:")
	fmt.Println(back)
}
