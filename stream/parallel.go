package stream

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// PerMorsel is the per-morsel worker function used by ParallelMapStage. It
// receives a morsel (owned), and returns a replacement morsel (ownership
// transferred to the stage). Errors cancel the pipeline.
type PerMorsel func(ctx context.Context, m Morsel) (Morsel, error)

// ParallelMapStage applies fn to every incoming morsel using up to `workers`
// worker goroutines. Output order preserves input order: morsels are tagged
// with a sequence number on ingress and a small reorder buffer restores
// order on egress.
//
// When workers <= 1, the stage degenerates to the serial path and the
// reorder machinery is skipped.
func ParallelMapStage(cfg Config, workers int, fn PerMorsel) Stage {
	if workers <= 1 {
		return func(ctx context.Context, in <-chan Morsel, out chan<- Morsel) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case m, ok := <-in:
					if !ok {
						return nil
					}
					next, err := fn(ctx, m)
					if err != nil {
						return err
					}
					select {
					case <-ctx.Done():
						next.Release()
						return ctx.Err()
					case out <- next:
					}
				}
			}
		}
	}

	return func(ctx context.Context, in <-chan Morsel, out chan<- Morsel) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		type tagged struct {
			seq int
			m   Morsel
		}

		taggedIn := make(chan tagged, cfg.ChannelBuffer*workers)
		processedOut := make(chan tagged, cfg.ChannelBuffer*workers)

		// Tagger: read in, assign sequence numbers, forward to taggedIn.
		go func() {
			defer close(taggedIn)
			seq := 0
			for {
				select {
				case <-ctx.Done():
					return
				case m, ok := <-in:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						m.Release()
						return
					case taggedIn <- tagged{seq: seq, m: m}:
						seq++
					}
				}
			}
		}()

		// Workers.
		var wg sync.WaitGroup
		errCh := make(chan error, workers)
		for range workers {
			wg.Go(func() {
				for {
					select {
					case <-ctx.Done():
						return
					case t, ok := <-taggedIn:
						if !ok {
							return
						}
						next, err := fn(ctx, t.m)
						if err != nil {
							errCh <- err
							cancel()
							return
						}
						select {
						case <-ctx.Done():
							next.Release()
							return
						case processedOut <- tagged{seq: t.seq, m: next}:
						}
					}
				}
			})
		}
		go func() {
			wg.Wait()
			close(processedOut)
			close(errCh)
		}()

		// Reorderer: buffer out-of-order results and emit in sequence.
		pending := make(map[int]Morsel)
		nextSeq := 0
		drain := func() {
			for _, m := range pending {
				m.Release()
			}
		}

		for {
			select {
			case <-ctx.Done():
				drain()
				for range processedOut {
				}
				select {
				case e := <-errCh:
					if e != nil {
						return e
					}
				default:
				}
				return ctx.Err()
			case t, ok := <-processedOut:
				if !ok {
					// All workers done; try one last drain of errCh.
					drain()
					select {
					case e := <-errCh:
						if e != nil {
							return e
						}
					default:
					}
					return nil
				}
				pending[t.seq] = t.m
				for {
					m, present := pending[nextSeq]
					if !present {
						break
					}
					delete(pending, nextSeq)
					nextSeq++
					select {
					case <-ctx.Done():
						m.Release()
						drain()
						return ctx.Err()
					case out <- m:
					}
				}
			}
		}
	}
}

// ParallelFilterStage is a filter stage that uses `workers` goroutines.
// Output order is preserved.
func ParallelFilterStage(cfg Config, pred expr.Expr, workers int) Stage {
	return ParallelMapStage(cfg, workers, makeFilterPerMorsel(cfg, pred))
}

// ParallelProjectStage is a projection stage that uses `workers` goroutines.
// Output order is preserved.
func ParallelProjectStage(cfg Config, exprs []expr.Expr, workers int) Stage {
	return ParallelMapStage(cfg, workers, makeProjectPerMorsel(cfg, exprs))
}

// ParallelWithColumnsStage uses `workers` goroutines to extend each morsel.
func ParallelWithColumnsStage(cfg Config, exprs []expr.Expr, workers int) Stage {
	return ParallelMapStage(cfg, workers, makeWithColumnsPerMorsel(cfg, exprs))
}

// makeFilterPerMorsel builds the per-morsel body used by FilterStage and
// ParallelFilterStage. Extracted so both paths share identical semantics.
func makeFilterPerMorsel(cfg Config, pred expr.Expr) PerMorsel {
	return func(ctx context.Context, m Morsel) (Morsel, error) {
		mask, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.Allocator}, pred, m.DF)
		if err != nil {
			m.Release()
			return Morsel{}, err
		}
		if !mask.DType().IsBool() {
			mask.Release()
			m.Release()
			return Morsel{}, errMaskNotBool
		}
		filtered, err := m.DF.Filter(ctx, mask, dataframe.WithFilterAllocator(cfg.Allocator))
		mask.Release()
		m.Release()
		if err != nil {
			return Morsel{}, err
		}
		return Morsel{DF: filtered}, nil
	}
}

func makeProjectPerMorsel(cfg Config, exprs []expr.Expr) PerMorsel {
	return func(ctx context.Context, m Morsel) (Morsel, error) {
		cols := make([]*series.Series, len(exprs))
		for i, e := range exprs {
			s, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.Allocator}, e, m.DF)
			if err != nil {
				for _, c := range cols[:i] {
					if c != nil {
						c.Release()
					}
				}
				m.Release()
				return Morsel{}, err
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
		out, err := dataframe.New(cols...)
		if err != nil {
			for _, c := range cols {
				c.Release()
			}
			return Morsel{}, err
		}
		return Morsel{DF: out}, nil
	}
}

func makeWithColumnsPerMorsel(cfg Config, exprs []expr.Expr) PerMorsel {
	return func(ctx context.Context, m Morsel) (Morsel, error) {
		outDF := m.DF.Clone()
		m.Release()
		for _, e := range exprs {
			s, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.Allocator}, e, outDF)
			if err != nil {
				outDF.Release()
				return Morsel{}, err
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
				return Morsel{}, err
			}
			outDF = next
		}
		return Morsel{DF: outDF}, nil
	}
}

// errMaskNotBool is reused across stage implementations.
var errMaskNotBool = errMaskTypeError()

func errMaskTypeError() error { return &maskTypeError{} }

type maskTypeError struct{}

func (e *maskTypeError) Error() string { return "filter predicate must produce bool" }

// keep allocator import used in this file.
var _ = memory.DefaultAllocator
