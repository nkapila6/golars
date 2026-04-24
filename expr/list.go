package expr

// ListOps is the expression-level mirror of series.ListOps. Reach
// it via Expr.List(); every method returns a fresh Expr so the
// kernel runs at evaluation time.
type ListOps struct{ e Expr }

// List opens the list namespace.
func (e Expr) List() ListOps { return ListOps{e: e} }

// Len returns the length of each inner list.
func (o ListOps) Len() Expr { return fn1("list.len", o.e) }

// Sum / Mean / Min / Max reduce each inner list.
func (o ListOps) Sum() Expr  { return fn1("list.sum", o.e) }
func (o ListOps) Mean() Expr { return fn1("list.mean", o.e) }
func (o ListOps) Min() Expr  { return fn1("list.min", o.e) }
func (o ListOps) Max() Expr  { return fn1("list.max", o.e) }

// First / Last / Get extract positional elements.
func (o ListOps) First() Expr       { return fn1("list.first", o.e) }
func (o ListOps) Last() Expr        { return fn1("list.last", o.e) }
func (o ListOps) Get(idx int) Expr  { return fn1p("list.get", o.e, idx) }

// Contains tests list membership for a literal needle.
func (o ListOps) Contains(needle any) Expr {
	return fn1p("list.contains", o.e, needle)
}

// Join concatenates string lists with sep.
func (o ListOps) Join(sep string) Expr { return fn1p("list.join", o.e, sep) }

// StructOps is the expression-level mirror of series.StructOps.
type StructOps struct{ e Expr }

// Struct opens the struct namespace.
func (e Expr) Struct() StructOps { return StructOps{e: e} }

// Field projects a single named field out of a struct column.
func (o StructOps) Field(name string) Expr { return fn1p("struct.field", o.e, name) }
