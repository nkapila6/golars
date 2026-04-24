// Write + read an Arrow IPC stream (cross-language binary format).
// Run: go run ./examples/ipc_streaming
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	// Producer: write three record batches into a buffer.
	name, _ := series.FromString("name", []string{"a", "b"}, nil)
	age, _ := series.FromInt64("age", []int64{30, 40}, nil)
	batch1, _ := dataframe.New(name, age)

	var buf bytes.Buffer
	sw, err := ipc.NewStreamWriter(&buf, batch1)
	if err != nil {
		log.Fatal(err)
	}
	for range 3 {
		if err := sw.Write(ctx, batch1); err != nil {
			log.Fatal(err)
		}
	}
	sw.Close()
	batch1.Release()

	// Consumer: pull each batch back out.
	sr, err := ipc.NewStreamReader(&buf)
	if err != nil {
		log.Fatal(err)
	}
	defer sr.Close()

	i := 0
	for df, err := range sr.Iter(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("batch %d: %d x %d\n", i, df.Height(), df.Width())
		df.Release()
		i++
	}
}
