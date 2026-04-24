// Parse JSON (array of objects) into a DataFrame and write it back.
// Run: go run ./examples/json
package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/io/json"
)

func main() {
	ctx := context.Background()

	doc := `[
		{"name": "ada", "age": 27, "score": 9.1},
		{"name": "brian", "age": 34, "score": 7.3},
		{"name": "carl", "age": 19, "score": 6.4}
	]`
	df, err := json.ReadString(ctx, doc)
	if err != nil {
		panic(err)
	}
	defer df.Release()
	fmt.Println("parsed:")
	fmt.Println(df)

	var buf bytes.Buffer
	if err := json.Write(ctx, &buf, df); err != nil {
		panic(err)
	}
	fmt.Println("serialised:")
	fmt.Print(buf.String())
}
