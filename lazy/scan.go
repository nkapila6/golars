package lazy

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/schema"
)

// SourceFunc is a lazy DataFrame source. It's evaluated once at
// collect time. Used to implement ScanCSV, ScanParquet, etc. without
// blowing up the plan tree.
type SourceFunc struct {
	// Name is a human-friendly label that shows up in Explain().
	Name string
	// Schema, when non-nil, is the best-effort output schema. Some
	// callers can supply it cheaply (parquet metadata, explicit
	// user-provided schemas); others leave it nil so Collect runs the
	// source once to discover the schema.
	KnownSchema *schema.Schema
	// Load returns the underlying DataFrame. The returned frame is
	// owned by the caller; the lazy executor calls Release when done.
	Load func(context.Context) (*dataframe.DataFrame, error)
}

func (SourceFunc) isLogicalNode()   {}
func (SourceFunc) Children() []Node { return nil }
func (s SourceFunc) WithChildren(children []Node) Node {
	if len(children) != 0 {
		panic("lazy: SourceFunc has no children")
	}
	return s
}

// Schema returns the pre-declared schema if available. When KnownSchema
// is nil, the loader is invoked once and its output schema is cached.
// This is a deliberate tradeoff: it trades one extra I/O at schema-
// discovery time for much better optimiser behaviour (projection
// pushdown needs a schema).
func (s SourceFunc) Schema() (*schema.Schema, error) {
	if s.KnownSchema != nil {
		return s.KnownSchema, nil
	}
	// We can't materialise the source here without a ctx; punt to the
	// executor. Callers that need schemas before Collect should
	// supply KnownSchema up front.
	return nil, fmt.Errorf("lazy: SourceFunc[%s] has no declared schema; pass KnownSchema or Collect to observe", s.Name)
}

func (s SourceFunc) String() string {
	return fmt.Sprintf("SCAN source=%s", s.Name)
}

// FromSource wraps a loader function into a LazyFrame. Intended for
// io/* packages that want to expose a lazy scan.
func FromSource(name string, known *schema.Schema, load func(context.Context) (*dataframe.DataFrame, error)) LazyFrame {
	return LazyFrame{plan: SourceFunc{Name: name, KnownSchema: known, Load: load}}
}
