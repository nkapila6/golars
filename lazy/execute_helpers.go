package lazy

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// executeTail evaluates the upstream and keeps the last N rows.
func executeTail(ctx context.Context, cfg execConfig, t TailNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, t.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	n := min(max(t.N, 0), input.Height())
	out := input.Tail(n)
	return out, nil
}

// executeReverse evaluates the upstream and reverses the row order.
func executeReverse(ctx context.Context, cfg execConfig, r ReverseNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, r.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.Reverse(ctx)
}
