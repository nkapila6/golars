package lazy

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// SinkFunc signals the executor to run the plan and hand the
// materialised DataFrame to a writer callback. It's the generic
// backbone for SinkCSV/SinkParquet/SinkIPC/SinkJSON without coupling
// lazy to every io/* package.
//
// The writer is called exactly once. On success the returned
// DataFrame.Release is handled for the caller; on error the frame
// is released before the error propagates.
func (lf LazyFrame) Sink(ctx context.Context, writer func(context.Context, *dataframe.DataFrame) error, opts ...ExecOption) error {
	df, err := lf.Collect(ctx, opts...)
	if err != nil {
		return err
	}
	defer df.Release()
	return writer(ctx, df)
}
