// Write a DataFrame to Parquet and read it back.
// Run: go run ./examples/parquet
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	city, _ := series.FromString("city", []string{"Tokyo", "Delhi", "Shanghai"}, nil)
	pop, _ := series.FromInt64("population", []int64{13960000, 28514000, 25582000}, nil)
	df, _ := dataframe.New(city, pop)
	defer df.Release()

	tmp, _ := os.MkdirTemp("", "golars-parquet")
	defer os.RemoveAll(tmp)
	path := filepath.Join(tmp, "cities.parquet")

	if err := parquet.WriteFile(ctx, path, df); err != nil {
		panic(err)
	}

	back, err := parquet.ReadFile(ctx, path)
	if err != nil {
		panic(err)
	}
	defer back.Release()
	fmt.Println("round-tripped via Parquet:")
	fmt.Println(back)
}
