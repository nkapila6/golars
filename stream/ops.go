package stream

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// FilterStage applies a boolean predicate expression to each morsel and
// forwards the filtered DataFrame. Empty results are forwarded as
// zero-height morsels so the sink sees them once (counts and progress).
func FilterStage(cfg Config, pred expr.Expr) Stage {
	return func(ctx context.Context, in <-chan Morsel, out chan<- Morsel) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case m, ok := <-in:
				if !ok {
					return nil
				}
				mask, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.Allocator}, pred, m.DF)
				if err != nil {
					m.Release()
					return fmt.Errorf("filter eval: %w", err)
				}
				if !mask.DType().IsBool() {
					mask.Release()
					m.Release()
					return fmt.Errorf("filter predicate must produce bool, got %s", mask.DType())
				}
				filtered, err := m.DF.Filter(ctx, mask, dataframe.WithFilterAllocator(cfg.Allocator))
				mask.Release()
				m.Release()
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					filtered.Release()
					return ctx.Err()
				case out <- Morsel{DF: filtered}:
				}
			}
		}
	}
}

// ProjectStage evaluates each expression against the incoming morsel and
// builds a new DataFrame with one column per expression.
func ProjectStage(cfg Config, exprs []expr.Expr) Stage {
	return func(ctx context.Context, in <-chan Morsel, out chan<- Morsel) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case m, ok := <-in:
				if !ok {
					return nil
				}
				cols := make([]*series.Series, len(exprs))
				var evalErr error
				for i, e := range exprs {
					s, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.Allocator}, e, m.DF)
					if err != nil {
						for _, c := range cols[:i] {
							if c != nil {
								c.Release()
							}
						}
						evalErr = fmt.Errorf("project %s: %w", e, err)
						break
					}
					name := expr.OutputName(e)
					if s.Name() != name {
						r := s.Rename(name)
						s.Release()
						s = r
					}
					cols[i] = s
				}
				m.Release()
				if evalErr != nil {
					return evalErr
				}
				outDF, err := dataframe.New(cols...)
				if err != nil {
					for _, c := range cols {
						c.Release()
					}
					return err
				}
				select {
				case <-ctx.Done():
					outDF.Release()
					return ctx.Err()
				case out <- Morsel{DF: outDF}:
				}
			}
		}
	}
}

// RenameStage renames a column in every morsel.
func RenameStage(_ Config, oldName, newName string) Stage {
	return func(ctx context.Context, in <-chan Morsel, out chan<- Morsel) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case m, ok := <-in:
				if !ok {
					return nil
				}
				renamed, err := m.DF.Rename(oldName, newName)
				m.Release()
				if err != nil {
					return fmt.Errorf("rename: %w", err)
				}
				select {
				case <-ctx.Done():
					renamed.Release()
					return ctx.Err()
				case out <- Morsel{DF: renamed}:
				}
			}
		}
	}
}

// DropStage removes columns from every morsel. Missing names are ignored.
func DropStage(_ Config, cols []string) Stage {
	return func(ctx context.Context, in <-chan Morsel, out chan<- Morsel) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case m, ok := <-in:
				if !ok {
					return nil
				}
				dropped := m.DF.Drop(cols...)
				m.Release()
				select {
				case <-ctx.Done():
					dropped.Release()
					return ctx.Err()
				case out <- Morsel{DF: dropped}:
				}
			}
		}
	}
}

// SliceStage emits only the global row range [offset, offset+length). It
// tracks a running row counter across morsels and trims or discards morsels
// as needed. When the window is exhausted, remaining incoming morsels are
// drained and released without forwarding.
func SliceStage(_ Config, offset, length int) Stage {
	return func(ctx context.Context, in <-chan Morsel, out chan<- Morsel) error {
		rowsSeen := 0
		remaining := length
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case m, ok := <-in:
				if !ok {
					return nil
				}
				h := m.DF.Height()
				morselStart := rowsSeen
				morselEnd := rowsSeen + h
				rowsSeen = morselEnd

				if remaining <= 0 || morselEnd <= offset {
					m.Release()
					continue
				}
				start := 0
				if morselStart < offset {
					start = offset - morselStart
				}
				take := min(h-start, remaining)
				var outDF *dataframe.DataFrame
				if start == 0 && take == h {
					outDF = m.DF
				} else {
					sl, err := m.DF.Slice(start, take)
					m.Release()
					if err != nil {
						return fmt.Errorf("slice: %w", err)
					}
					outDF = sl
				}
				remaining -= take
				select {
				case <-ctx.Done():
					outDF.Release()
					return ctx.Err()
				case out <- Morsel{DF: outDF}:
				}
			}
		}
	}
}

// WithColumnsStage extends each morsel with new/replacement columns.
func WithColumnsStage(cfg Config, exprs []expr.Expr) Stage {
	return func(ctx context.Context, in <-chan Morsel, out chan<- Morsel) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case m, ok := <-in:
				if !ok {
					return nil
				}
				outDF := m.DF.Clone()
				for _, e := range exprs {
					s, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.Allocator}, e, outDF)
					if err != nil {
						outDF.Release()
						m.Release()
						return fmt.Errorf("with_columns %s: %w", e, err)
					}
					name := expr.OutputName(e)
					if s.Name() != name {
						r := s.Rename(name)
						s.Release()
						s = r
					}
					next, err := outDF.WithColumn(s)
					outDF.Release()
					if err != nil {
						s.Release()
						m.Release()
						return err
					}
					outDF = next
				}
				m.Release()
				select {
				case <-ctx.Done():
					outDF.Release()
					return ctx.Err()
				case out <- Morsel{DF: outDF}:
				}
			}
		}
	}
}
