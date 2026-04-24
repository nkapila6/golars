// Pivot: long → wide reshape.
// Run: go run ./examples/pivot
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	// Sales by (product, region) - long form.
	product, _ := series.FromString("product", []string{
		"A", "A", "A", "B", "B", "B",
	}, nil)
	region, _ := series.FromString("region", []string{
		"east", "west", "north", "east", "west", "north",
	}, nil)
	amount, _ := series.FromInt64("amount", []int64{
		100, 80, 120, 60, 90, 70,
	}, nil)
	long, _ := dataframe.New(product, region, amount)
	defer long.Release()

	fmt.Println("--- long ---")
	fmt.Println(long)

	// Pivot to wide: product as row index, region as column header.
	wide, err := long.Pivot(ctx, []string{"product"}, "region", "amount", dataframe.PivotSum)
	if err != nil {
		log.Fatal(err)
	}
	defer wide.Release()
	fmt.Println("--- wide (PivotSum) ---")
	fmt.Println(wide)
}
