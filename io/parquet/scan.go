package parquet

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Scan returns a LazyFrame that reads the Parquet file at path only
// when Collect runs. Options propagate to ReadFile.
func Scan(path string, opts ...Option) lazy.LazyFrame {
	return lazy.FromSource("parquet:"+path, nil, func(ctx context.Context) (*dataframe.DataFrame, error) {
		return ReadFile(ctx, path, opts...)
	})
}
