package expr

// More fluent Expr methods that lower to the series-level kernels at
// eval time. Every method here appends a FunctionNode carrying the
// kernel's canonical name plus a (possibly empty) scalar parameter
// list; the evaluator dispatches on Name.

// Sort returns the sorted values of the column. Ascending is the
// polars default; pass descending=true for reverse order.
func (e Expr) Sort(descending bool) Expr { return fn1p("sort", e, descending) }

// Unique returns the distinct non-null values of the column, first-
// occurrence order, with a single trailing null if the input has
// nulls.
func (e Expr) Unique() Expr { return fn1("unique", e) }

// HashValues returns a per-row FNV-64a hash as a uint64 column. Nulls
// propagate to nulls. Named HashValues (not Hash) because Expr.Hash
// already exists on the structural-hash side for CSE.
func (e Expr) HashValues() Expr { return fn1("hash", e) }

// CumSum returns the running sum of the column.
func (e Expr) CumSum() Expr { return fn1("cum_sum", e) }

// CumMin returns the running minimum.
func (e Expr) CumMin() Expr { return fn1("cum_min", e) }

// CumMax returns the running maximum.
func (e Expr) CumMax() Expr { return fn1("cum_max", e) }

// CumProd returns the running product (always float64 output).
func (e Expr) CumProd() Expr { return fn1("cum_prod", e) }

// CumCount returns the running count of non-null values.
func (e Expr) CumCount() Expr { return fn1("cum_count", e) }

// Diff returns element-wise differences between periods-step neighbours.
func (e Expr) Diff(periods int) Expr { return fn1p("diff", e, periods) }

// PctChange returns element-wise percent change against periods-ago.
func (e Expr) PctChange(periods int) Expr { return fn1p("pct_change", e, periods) }

// Rank returns a float64 rank column using the given method.
// method is the polars-style string: "average", "min", "max", "dense",
// "ordinal".
func (e Expr) Rank(method string) Expr { return fn1p("rank", e, method) }

// IsDuplicated is true where the value occurs more than once.
func (e Expr) IsDuplicated() Expr { return fn1("is_duplicated", e) }

// IsUnique is true where the value occurs exactly once.
func (e Expr) IsUnique() Expr { return fn1("is_unique", e) }

// IsFirstDistinct / IsLastDistinct mark the first/last occurrence of
// each distinct value.
func (e Expr) IsFirstDistinct() Expr { return fn1("is_first_distinct", e) }
func (e Expr) IsLastDistinct() Expr  { return fn1("is_last_distinct", e) }

// NUnique aggregates to the count of distinct non-null values.
func (e Expr) NUnique() Expr { return fn1("n_unique", e) }
