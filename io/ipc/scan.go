package ipc

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Scan returns a LazyFrame that reads the Arrow IPC file at path
// lazily at Collect time.
func Scan(path string, opts ...Option) lazy.LazyFrame {
	return lazy.FromSource("ipc:"+path, nil, func(ctx context.Context) (*dataframe.DataFrame, error) {
		return ReadFile(ctx, path, opts...)
	})
}
