package json

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Scan returns a LazyFrame that reads a JSON (array-of-object) file
// lazily at Collect time.
func Scan(path string, opts ...Option) lazy.LazyFrame {
	return lazy.FromSource("json:"+path, nil, func(ctx context.Context) (*dataframe.DataFrame, error) {
		return ReadFile(ctx, path, opts...)
	})
}

// ScanNDJSON is the NDJSON (line-delimited) counterpart.
func ScanNDJSON(path string, opts ...Option) lazy.LazyFrame {
	return lazy.FromSource("ndjson:"+path, nil, func(ctx context.Context) (*dataframe.DataFrame, error) {
		return ReadNDJSONFile(ctx, path, opts...)
	})
}
