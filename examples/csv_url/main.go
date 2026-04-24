// Fetch a CSV from an http(s) URL.
// Run: go run ./examples/csv_url
//
// Uses a local httptest server so the example is self-contained. For real
// usage, pass any HTTPS URL that returns CSV.
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/Gaurav-Gosain/golars/io/csv"
)

func main() {
	body := "city,population\nTokyo,13960000\nDelhi,28514000\nShanghai,25582000\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	df, err := csv.ReadURL(context.Background(), srv.URL)
	if err != nil {
		panic(err)
	}
	defer df.Release()
	fmt.Println(df)
}
