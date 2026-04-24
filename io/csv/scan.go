package csv

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Scan returns a LazyFrame that reads path only at Collect time.
// Options propagate to ReadFile. Mirrors polars' `pl.scan_csv(path)`
// : projection pushdown kicks in after the read, but the file open
// itself is deferred.
func Scan(path string, opts ...Option) lazy.LazyFrame {
	return lazy.FromSource("csv:"+path, nil, func(ctx context.Context) (*dataframe.DataFrame, error) {
		return ReadFile(ctx, path, opts...)
	})
}
