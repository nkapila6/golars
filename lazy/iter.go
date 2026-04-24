package lazy

import (
	"context"
	"iter"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// IterBatches returns a Go 1.23+ iterator that yields the result of
// Collect as a sequence of DataFrame batches. The current
// implementation emits a single batch equal to the full Collect
// output; this is still useful for composing standard range-over
// loops in user code. When the streaming engine is enabled via
// WithStreaming, a future improvement will yield per-morsel batches
// directly.
//
// Each yielded DataFrame must be Released by the caller. Stopping the
// iteration early (breaking out of range) releases the unyielded
// frames and runs no further work.
func (lf LazyFrame) IterBatches(ctx context.Context, opts ...ExecOption) iter.Seq2[*dataframe.DataFrame, error] {
	return func(yield func(*dataframe.DataFrame, error) bool) {
		df, err := lf.Collect(ctx, opts...)
		if err != nil {
			yield(nil, err)
			return
		}
		if !yield(df, nil) {
			df.Release()
		}
	}
}
