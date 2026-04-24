package expr

// Coalesce returns the first non-null value across the given
// expressions at each row. Mirrors polars' pl.coalesce(...).
func Coalesce(exprs ...Expr) Expr {
	return Expr{FunctionNode{Name: "coalesce", Args: exprs}}
}

// ConcatStr concatenates the string representations of each expr
// row-wise, using sep as the delimiter. Nulls in any input propagate
// to the output (strictest polars behaviour). Mirrors
// pl.concat_str(..., separator=sep).
func ConcatStr(sep string, exprs ...Expr) Expr {
	return Expr{FunctionNode{Name: "concat_str", Args: exprs, Params: []any{sep}}}
}

// IntRange returns a new int64 Series of [start, end) with the given
// step. Produces a length-mismatched column which callers typically
// wrap in pl.int_range(...).alias("idx"). step may not be zero.
func IntRange(start, end, step int64) Expr {
	return Expr{FunctionNode{Name: "int_range", Args: nil, Params: []any{start, end, step}}}
}

// Ones returns a fresh float64 Series of length n filled with 1.0.
func Ones(n int) Expr { return Expr{FunctionNode{Name: "ones", Params: []any{n}}} }

// Zeros returns a fresh float64 Series of length n filled with 0.0.
func Zeros(n int) Expr { return Expr{FunctionNode{Name: "zeros", Params: []any{n}}} }

// Lit is already defined in expr.go but the Lit wrappers above mean
// callers can construct "frame-size" columns via Ones/Zeros without
// needing a Col reference.
