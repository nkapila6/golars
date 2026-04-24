package csv_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/lazy"
)

func TestCSVScanDefersIO(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "t.csv")
	if err := os.WriteFile(p, []byte("a,b\n1,2\n3,4\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Scan returns a LazyFrame; construction should not yet have opened
	// the file. We confirm by deleting the file after Scan, before Collect.
	// The subsequent Collect must error (file gone).
	lf := iocsv.Scan(p)
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if _, err := lf.Collect(context.Background(), lazy.WithExecAllocator(nil)); err == nil {
		t.Errorf("Collect after file removal should fail; Scan must be lazy")
	}

	// Write the file again and collect should succeed.
	if err := os.WriteFile(p, []byte("a,b\n1,2\n3,4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	df, err := iocsv.Scan(p).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	if df.Height() != 2 || df.Width() != 2 {
		t.Errorf("shape = (%d,%d), want (2,2)", df.Height(), df.Width())
	}
}
