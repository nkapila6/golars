// Parse newline-delimited JSON (one object per line) into a DataFrame.
// Run: go run ./examples/ndjson
package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/io/json"
)

func main() {
	ctx := context.Background()

	doc := `{"event": "login",  "user": "ada",   "ts": 1699200000}
{"event": "checkout","user": "brian", "ts": 1699200120}
{"event": "logout",  "user": "ada",   "ts": 1699200180}
`
	df, err := json.ReadNDJSONString(ctx, doc)
	if err != nil {
		panic(err)
	}
	defer df.Release()
	fmt.Println("parsed NDJSON:")
	fmt.Println(df)

	var buf bytes.Buffer
	if err := json.WriteNDJSON(ctx, &buf, df); err != nil {
		panic(err)
	}
	fmt.Println("round-tripped NDJSON:")
	fmt.Print(buf.String())
}
