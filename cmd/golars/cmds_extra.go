package main

import (
	"context"
	"fmt"
	"strconv"
)

// cmdReverse materialises the current pipeline and reverses row order.
func (s *state) cmdReverse() error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	rev, err := df.Reverse(context.Background())
	if err != nil {
		return err
	}
	defer rev.Release()
	printTable(rev)
	return nil
}

// cmdSample takes N rows randomly (no replacement by default).
// Syntax: .sample N [seed]
func (s *state) cmdSample(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf(".sample N [seed]: sample N rows")
	}
	n, err := strconv.Atoi(args[0])
	if err != nil || n < 0 {
		return fmt.Errorf(".sample: invalid N %q", args[0])
	}
	var seed uint64 = 42
	if len(args) >= 2 {
		v, perr := strconv.ParseUint(args[1], 10, 64)
		if perr != nil {
			return fmt.Errorf(".sample: invalid seed %q", args[1])
		}
		seed = v
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	out, err := df.Sample(context.Background(), n, false, seed)
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}

// cmdShuffle reorders rows randomly.
// Syntax: .shuffle [seed]
func (s *state) cmdShuffle(args []string) error {
	var seed uint64 = 42
	if len(args) >= 1 {
		v, perr := strconv.ParseUint(args[0], 10, 64)
		if perr != nil {
			return fmt.Errorf(".shuffle: invalid seed %q", args[0])
		}
		seed = v
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	out, err := df.Shuffle(context.Background(), seed)
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}

// cmdNullCount prints per-column null counts.
func (s *state) cmdNullCount() error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	nc := df.NullCount()
	defer nc.Release()
	printTable(nc)
	return nil
}

// cmdGlimpse prints the first N rows in a compact format (default 5).
func (s *state) cmdGlimpse(args []string) error {
	n := 5
	if len(args) >= 1 {
		v, perr := strconv.Atoi(args[0])
		if perr == nil && v > 0 {
			n = v
		}
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	fmt.Println(df.Glimpse(n))
	return nil
}

// cmdSize prints the EstimatedSize of the current pipeline result, in
// human-readable bytes.
func (s *state) cmdSize() error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	b := df.EstimatedSize()
	fmt.Printf("%s  %d rows × %d cols  %s\n",
		successStyle.Render("estimated size:"),
		df.Height(), df.Width(),
		humanBytes(b))
	return nil
}

// cmdUnique removes duplicate rows over the entire frame. For now
// we dispatch per-column: groupby every column then emit the keys.
// That's exactly the polars semantics (subset=all, keep=first).
func (s *state) cmdUnique() error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	if df.Width() == 0 {
		return nil
	}
	keys := df.ColumnNames()
	out, err := df.GroupBy(keys...).Agg(context.Background(), nil)
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}

func humanBytes(n int) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%d B", n)
	case n < 1024*1024:
		return fmt.Sprintf("%.1f KiB", float64(n)/1024)
	case n < 1024*1024*1024:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1024*1024))
	}
	return fmt.Sprintf("%.1f GiB", float64(n)/(1024*1024*1024))
}
