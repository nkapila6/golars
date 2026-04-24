package csv_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Gaurav-Gosain/golars/io/csv"
)

func TestReadURL(t *testing.T) {
	t.Parallel()
	body := "name,value\na,1\nb,2\nc,3\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	df, err := csv.ReadURL(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("ReadURL: %v", err)
	}
	defer df.Release()
	if df.Height() != 3 || df.Width() != 2 {
		t.Errorf("got %dx%d, want 3x2", df.Height(), df.Width())
	}
}

func TestReadURLNon2xx(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()
	_, err := csv.ReadURL(context.Background(), srv.URL)
	if err == nil {
		t.Error("expected error for 404 response")
	}
}
