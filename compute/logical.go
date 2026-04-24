package compute

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// And returns a boolean Series a AND b with Kleene three-valued logic:
//   - true AND null = null
//   - false AND null = false
//   - null AND null = null
func And(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runKleene(ctx, a, b, opts, "And", true)
}

// Or returns a boolean Series a OR b with Kleene three-valued logic:
//   - false OR null = null
//   - true OR null = true
//   - null OR null = null
func Or(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runKleene(ctx, a, b, opts, "Or", false)
}

// Not returns a boolean Series NOT a. Nulls propagate.
func Not(ctx context.Context, s *series.Series, opts ...Option) (*series.Series, error) {
	if !s.DType().IsBool() {
		return nil, isUnsupported("Not", s.DType())
	}
	cfg := resolve(opts)

	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()

	name := cfg.outName(s.Name())
	par := inferParallelism(cfg, s.Len())
	n := arr.Len()

	boolArr := arr.(*array.Boolean)
	out := make([]bool, n)
	valid := buildValidityCopy(arr)

	err = pool.ParallelFor(ctx, n, par, func(ctx context.Context, start, end int) error {
		if valid == nil {
			for i := start; i < end; i++ {
				out[i] = !boolArr.Value(i)
			}
			return nil
		}
		for i := start; i < end; i++ {
			if valid[i] {
				out[i] = !boolArr.Value(i)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fromBoolResult(name, out, valid, cfg.alloc)
}

// runKleene implements AND/OR with three-valued logic. isAnd selects the op.
func runKleene(ctx context.Context, a, b *series.Series, opts []Option, kernel string, isAnd bool) (*series.Series, error) {
	if err := checkBinary(a, b); err != nil {
		return nil, err
	}
	if !a.DType().IsBool() {
		return nil, isUnsupported(kernel, a.DType())
	}
	cfg := resolve(opts)

	aArr, err := extractChunk(a, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer aArr.Release()
	bArr, err := extractChunk(b, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer bArr.Release()

	aBool := aArr.(*array.Boolean)
	bBool := bArr.(*array.Boolean)

	n := aArr.Len()
	out := make([]bool, n)
	var valid []bool
	if aArr.NullN() > 0 || bArr.NullN() > 0 {
		valid = make([]bool, n)
	}

	par := inferParallelism(cfg, n)
	err = pool.ParallelFor(ctx, n, par, func(ctx context.Context, s, e int) error {
		for i := s; i < e; i++ {
			av, aok := aBool.Value(i), aArr.IsValid(i)
			bv, bok := bBool.Value(i), bArr.IsValid(i)
			val, valOK := kleene(isAnd, av, aok, bv, bok)
			out[i] = val
			if valid != nil {
				valid[i] = valOK
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fromBoolResult(cfg.outName(a.Name()), out, valid, cfg.alloc)
}

// kleene returns (value, ok) under Kleene three-valued logic. ok=false means
// the result is null.
func kleene(isAnd bool, av, aok, bv, bok bool) (bool, bool) {
	if isAnd {
		// false AND x = false (even if x is null)
		if aok && !av {
			return false, true
		}
		if bok && !bv {
			return false, true
		}
		// at this point any false has been handled; if either is null the
		// result is null, otherwise both are true so the result is true.
		if !aok || !bok {
			return false, false
		}
		return true, true
	}
	// Or: true OR x = true (even if x is null).
	if aok && av {
		return true, true
	}
	if bok && bv {
		return true, true
	}
	if !aok || !bok {
		return false, false
	}
	return false, true
}

// IsNull returns a boolean Series where out[i] is true iff s[i] is null. The
// result itself has no nulls.
func IsNull(ctx context.Context, s *series.Series, opts ...Option) (*series.Series, error) {
	cfg := resolve(opts)

	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()

	name := cfg.outName(s.Name())
	par := inferParallelism(cfg, s.Len())
	n := arr.Len()

	out := make([]bool, n)
	if arr.NullN() == 0 {
		return fromBoolResult(name, out, nil, cfg.alloc)
	}

	err = pool.ParallelFor(ctx, n, par, func(ctx context.Context, start, end int) error {
		for i := start; i < end; i++ {
			out[i] = arr.IsNull(i)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fromBoolResult(name, out, nil, cfg.alloc)
}

// IsNotNull returns a boolean Series where out[i] is true iff s[i] is valid.
func IsNotNull(ctx context.Context, s *series.Series, opts ...Option) (*series.Series, error) {
	cfg := resolve(opts)

	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()

	name := cfg.outName(s.Name())
	par := inferParallelism(cfg, s.Len())
	n := arr.Len()

	out := make([]bool, n)
	if arr.NullN() == 0 {
		for i := range out {
			out[i] = true
		}
		return fromBoolResult(name, out, nil, cfg.alloc)
	}

	err = pool.ParallelFor(ctx, n, par, func(ctx context.Context, start, end int) error {
		for i := start; i < end; i++ {
			out[i] = arr.IsValid(i)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fromBoolResult(name, out, nil, cfg.alloc)
}

// Make sure arrow import is used once we add more logical helpers.
var _ arrow.Array = (*array.Boolean)(nil)
