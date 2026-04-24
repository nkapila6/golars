package expr

import "github.com/Gaurav-Gosain/golars/dtype"

// Scalar is the set of Go scalar types that can be promoted to a
// literal expression. It mirrors the cases the untyped Lit function
// accepts, but as a type constraint so generic helpers can be
// compile-time checked.
type Scalar interface {
	~int | ~int32 | ~int64 | ~float32 | ~float64 | ~bool | ~string
}

// LitOf is a type-safe variant of Lit. The literal's Go type must be
// one of the supported scalar types; non-scalar values fail at build
// time rather than panicking at evaluation.
//
//	expr.LitOf(int64(2))   // or expr.LitOf[int64](2)
//	expr.LitOf("USD")      // T inferred as string
func LitOf[T Scalar](v T) Expr { return Lit(any(v)) }

// TCol is a typed column reference. It wraps an Expr with a phantom
// type that represents the column's Go-level scalar type, so the
// comparison/arithmetic helpers can take a bare T instead of an
// Expr wrapped around a Lit.
//
//	expr.C[int64]("qty").Gt(2).Alias("bulk")
//
// The type parameter is an intent hint, not a proof. If the runtime
// schema disagrees, evaluation panics with the same message that the
// untyped path produces. Use C when the column dtype is statically
// known; stick with Col when it is dynamic (config-driven,
// user-supplied, etc.).
type TCol[T Scalar] struct{ Expr }

// C constructs a typed column reference.
func C[T Scalar](name string) TCol[T] { return TCol[T]{Col(name)} }

// Int, Float, Str, Bool, Int32, Float32 are ergonomic aliases for
// the common C[T] instantiations. They read more naturally at the
// call site: `expr.Int("qty").Gt(2)` vs `expr.C[int64]("qty").Gt(2)`.
func Int(name string) TCol[int64]       { return C[int64](name) }
func Float(name string) TCol[float64]   { return C[float64](name) }
func Str(name string) TCol[string]      { return C[string](name) }
func Bool(name string) TCol[bool]       { return C[bool](name) }
func Int32(name string) TCol[int32]     { return C[int32](name) }
func Float32(name string) TCol[float32] { return C[float32](name) }

// As renames the expression output, returning a typed column so
// further typed operations can chain. Alias is a drop-in synonym
// that matches the untyped Expr's naming.
func (c TCol[T]) As(name string) TCol[T]    { return TCol[T]{c.Expr.Alias(name)} }
func (c TCol[T]) Alias(name string) TCol[T] { return TCol[T]{c.Expr.Alias(name)} }

// Eq, Ne, Lt, Le, Gt, Ge compare against a literal of the column's
// type. They return an untyped boolean Expr because downstream use
// (And/Or/Filter) is dtype-agnostic.
func (c TCol[T]) Eq(v T) Expr { return c.Expr.EqLit(v) }
func (c TCol[T]) Ne(v T) Expr { return c.Expr.NeLit(v) }
func (c TCol[T]) Lt(v T) Expr { return c.Expr.LtLit(v) }
func (c TCol[T]) Le(v T) Expr { return c.Expr.LeLit(v) }
func (c TCol[T]) Gt(v T) Expr { return c.Expr.GtLit(v) }
func (c TCol[T]) Ge(v T) Expr { return c.Expr.GeLit(v) }

// EqCol, NeCol, LtCol, LeCol, GtCol, GeCol compare two typed columns
// of the same T. Mixed-type comparisons go through the untyped API:
// `c.Expr.Gt(other.Expr)`.
func (c TCol[T]) EqCol(o TCol[T]) Expr { return c.Expr.Eq(o.Expr) }
func (c TCol[T]) NeCol(o TCol[T]) Expr { return c.Expr.Ne(o.Expr) }
func (c TCol[T]) LtCol(o TCol[T]) Expr { return c.Expr.Lt(o.Expr) }
func (c TCol[T]) LeCol(o TCol[T]) Expr { return c.Expr.Le(o.Expr) }
func (c TCol[T]) GtCol(o TCol[T]) Expr { return c.Expr.Gt(o.Expr) }
func (c TCol[T]) GeCol(o TCol[T]) Expr { return c.Expr.Ge(o.Expr) }

// Between returns a boolean expression: lo <= c <= hi.
// Polars' pl.col().is_between(lo, hi) equivalent.
func (c TCol[T]) Between(lo, hi T) Expr {
	return c.Expr.Between(any(lo), any(hi))
}

// IsIn returns a boolean expression: c in {values}. Membership is
// evaluated with equality on the column's scalar type.
func (c TCol[T]) IsIn(values ...T) Expr {
	args := make([]any, len(values))
	for i, v := range values {
		args[i] = any(v)
	}
	return c.Expr.IsIn(args...)
}

// Add, Sub, Mul, Div take a literal of the column's type and
// preserve T on the output. Promotion rules (e.g. int + float -> float)
// are not expressible at the type level; callers that need cross-type
// arithmetic should drop to the embedded Expr and .Cast explicitly.
func (c TCol[T]) Add(v T) TCol[T] { return TCol[T]{c.Expr.AddLit(v)} }
func (c TCol[T]) Sub(v T) TCol[T] { return TCol[T]{c.Expr.SubLit(v)} }
func (c TCol[T]) Mul(v T) TCol[T] { return TCol[T]{c.Expr.MulLit(v)} }
func (c TCol[T]) Div(v T) TCol[T] { return TCol[T]{c.Expr.DivLit(v)} }

// AddCol, SubCol, MulCol, DivCol combine two same-typed columns and
// preserve T on the output.
func (c TCol[T]) AddCol(o TCol[T]) TCol[T] { return TCol[T]{c.Expr.Add(o.Expr)} }
func (c TCol[T]) SubCol(o TCol[T]) TCol[T] { return TCol[T]{c.Expr.Sub(o.Expr)} }
func (c TCol[T]) MulCol(o TCol[T]) TCol[T] { return TCol[T]{c.Expr.Mul(o.Expr)} }
func (c TCol[T]) DivCol(o TCol[T]) TCol[T] { return TCol[T]{c.Expr.Div(o.Expr)} }

// Neg negates the column (valid for numeric dtypes; behaviour on
// non-numeric is inherited from the untyped Neg).
func (c TCol[T]) Neg() TCol[T] { return TCol[T]{c.Expr.Neg()} }

// Abs returns the absolute value. Numeric dtypes only.
func (c TCol[T]) Abs() TCol[T] { return TCol[T]{c.Expr.Abs()} }

// IsNull and IsNotNull return boolean expressions. Null checks apply
// to every dtype, so the output is untyped for downstream logic.
func (c TCol[T]) IsNull() Expr    { return c.Expr.IsNull() }
func (c TCol[T]) IsNotNull() Expr { return c.Expr.IsNotNull() }

// FillNull replaces nulls with a literal of the column's type. For
// cross-type fill (e.g. fill an int64 column with a float default)
// use the embedded Expr.FillNull(any).
func (c TCol[T]) FillNull(v T) TCol[T] { return TCol[T]{c.Expr.FillNull(v)} }

// Sum, Min, Max, First, Last preserve T because their output dtype
// matches the input dtype for every scalar the constraint admits.
func (c TCol[T]) Sum() TCol[T]   { return TCol[T]{c.Expr.Sum()} }
func (c TCol[T]) Min() TCol[T]   { return TCol[T]{c.Expr.Min()} }
func (c TCol[T]) Max() TCol[T]   { return TCol[T]{c.Expr.Max()} }
func (c TCol[T]) First() TCol[T] { return TCol[T]{c.Expr.First()} }
func (c TCol[T]) Last() TCol[T]  { return TCol[T]{c.Expr.Last()} }

// Mean always promotes to float64. Count and NullCount always
// produce int64. Median/Std/Var similarly promote to float64 for
// every numeric input.
func (c TCol[T]) Mean() TCol[float64]     { return TCol[float64]{c.Expr.Mean()} }
func (c TCol[T]) Count() TCol[int64]      { return TCol[int64]{c.Expr.Count()} }
func (c TCol[T]) NullCount() TCol[int64]  { return TCol[int64]{c.Expr.NullCount()} }
func (c TCol[T]) Median() TCol[float64]   { return TCol[float64]{c.Expr.Median()} }
func (c TCol[T]) Std() TCol[float64]      { return TCol[float64]{c.Expr.Std()} }
func (c TCol[T]) Var() TCol[float64]      { return TCol[float64]{c.Expr.Var()} }
func (c TCol[T]) Quantile(q float64) TCol[float64] {
	return TCol[float64]{c.Expr.Quantile(q)}
}
func (c TCol[T]) Product() TCol[T]         { return TCol[T]{c.Expr.Product()} }
func (c TCol[T]) Skew() TCol[float64]      { return TCol[float64]{c.Expr.Skew()} }
func (c TCol[T]) Kurtosis() TCol[float64]  { return TCol[float64]{c.Expr.Kurtosis()} }
func (c TCol[T]) NUnique() TCol[int64]     { return TCol[int64]{c.Expr.NUnique()} }
func (c TCol[T]) ApproxNUnique() TCol[int64] {
	return TCol[int64]{c.Expr.ApproxNUnique()}
}

// RollingSum/Min/Max preserve T. RollingMean/Std/Var promote to
// float64 since they introduce fractional values even for integer
// inputs.
func (c TCol[T]) RollingSum(windowSize, minPeriods int) TCol[T] {
	return TCol[T]{c.Expr.RollingSum(windowSize, minPeriods)}
}
func (c TCol[T]) RollingMin(windowSize, minPeriods int) TCol[T] {
	return TCol[T]{c.Expr.RollingMin(windowSize, minPeriods)}
}
func (c TCol[T]) RollingMax(windowSize, minPeriods int) TCol[T] {
	return TCol[T]{c.Expr.RollingMax(windowSize, minPeriods)}
}
func (c TCol[T]) RollingMean(windowSize, minPeriods int) TCol[float64] {
	return TCol[float64]{c.Expr.RollingMean(windowSize, minPeriods)}
}
func (c TCol[T]) RollingStd(windowSize, minPeriods int) TCol[float64] {
	return TCol[float64]{c.Expr.RollingStd(windowSize, minPeriods)}
}
func (c TCol[T]) RollingVar(windowSize, minPeriods int) TCol[float64] {
	return TCol[float64]{c.Expr.RollingVar(windowSize, minPeriods)}
}

// Cumulative and shift operations preserve T.
func (c TCol[T]) CumSum() TCol[T]             { return TCol[T]{c.Expr.CumSum()} }
func (c TCol[T]) CumMin() TCol[T]             { return TCol[T]{c.Expr.CumMin()} }
func (c TCol[T]) CumMax() TCol[T]             { return TCol[T]{c.Expr.CumMax()} }
func (c TCol[T]) CumProd() TCol[T]            { return TCol[T]{c.Expr.CumProd()} }
func (c TCol[T]) CumCount() TCol[int64]       { return TCol[int64]{c.Expr.CumCount()} }
func (c TCol[T]) Diff(periods int) TCol[T]    { return TCol[T]{c.Expr.Diff(periods)} }
func (c TCol[T]) Shift(periods int) TCol[T]   { return TCol[T]{c.Expr.Shift(periods)} }
func (c TCol[T]) Reverse() TCol[T]            { return TCol[T]{c.Expr.Reverse()} }
func (c TCol[T]) Head(n int) TCol[T]          { return TCol[T]{c.Expr.Head(n)} }
func (c TCol[T]) Tail(n int) TCol[T]          { return TCol[T]{c.Expr.Tail(n)} }
func (c TCol[T]) Slice(off, length int) TCol[T] {
	return TCol[T]{c.Expr.Slice(off, length)}
}
func (c TCol[T]) Sort(descending bool) TCol[T] { return TCol[T]{c.Expr.Sort(descending)} }
func (c TCol[T]) Unique() TCol[T]              { return TCol[T]{c.Expr.Unique()} }
func (c TCol[T]) ForwardFill(limit int) TCol[T] {
	return TCol[T]{c.Expr.ForwardFill(limit)}
}
func (c TCol[T]) BackwardFill(limit int) TCol[T] {
	return TCol[T]{c.Expr.BackwardFill(limit)}
}

// Over broadcasts an aggregation over the given partition keys. The
// output keeps T because the underlying aggregation's dtype is
// preserved across the partition.
func (c TCol[T]) Over(keys ...string) TCol[T] {
	return TCol[T]{c.Expr.Over(keys...)}
}

// CastTo drops the typed wrapper and coerces to the target dtype at
// evaluation. Use this to bridge typed columns of different T into a
// common dtype before combining them.
func (c TCol[T]) CastTo(to dtype.DType) Expr { return c.Expr.Cast(to) }

// CastInt64, CastFloat64, CastBool, CastString are convenience
// wrappers over CastTo that also re-tag the returned TCol so the
// type parameter stays meaningful.
func (c TCol[T]) CastInt64() TCol[int64]     { return TCol[int64]{c.Expr.Cast(dtype.Int64())} }
func (c TCol[T]) CastFloat64() TCol[float64] { return TCol[float64]{c.Expr.Cast(dtype.Float64())} }
func (c TCol[T]) CastBool() TCol[bool]       { return TCol[bool]{c.Expr.Cast(dtype.Bool())} }
func (c TCol[T]) CastString() TCol[string]   { return TCol[string]{c.Expr.Cast(dtype.String())} }

// Not, All, Any are top-level helpers that read well in predicate
// chains:
//
//	expr.All(a.Gt(0), b.Lt(10), c.IsNotNull())
//	expr.Any(a.IsNull(), b.Eq(0))
//	expr.Not(a.Eq("x"))
//
// Not is a thin passthrough; All / Any fold a variadic list into
// And / Or chains.
func Not(e Expr) Expr { return e.Not() }

func All(preds ...Expr) Expr {
	if len(preds) == 0 {
		return Lit(true)
	}
	out := preds[0]
	for _, p := range preds[1:] {
		out = out.And(p)
	}
	return out
}

func Any(preds ...Expr) Expr {
	if len(preds) == 0 {
		return Lit(false)
	}
	out := preds[0]
	for _, p := range preds[1:] {
		out = out.Or(p)
	}
	return out
}
