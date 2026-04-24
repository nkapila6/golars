package sql

import (
	"context"
	"fmt"
	"sync"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// Session holds a registered set of tables so queries can reference
// them by name. Concurrent-safe: a single session may be queried from
// multiple goroutines once tables are registered.
type Session struct {
	mu     sync.RWMutex
	tables map[string]*dataframe.DataFrame
}

// NewSession returns an empty session.
func NewSession() *Session {
	return &Session{tables: map[string]*dataframe.DataFrame{}}
}

// Register adds df under the given table name. Session borrows a
// reference (via Clone), so the caller may Release df after
// registration. Replacing an existing name releases the previous
// frame.
func (s *Session) Register(name string, df *dataframe.DataFrame) error {
	if name == "" {
		return fmt.Errorf("sql: empty table name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.tables[name]; ok {
		prev.Release()
	}
	s.tables[name] = df.Clone()
	return nil
}

// Deregister drops a table. No-op when the name is unknown.
func (s *Session) Deregister(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.tables[name]; ok {
		prev.Release()
		delete(s.tables, name)
	}
}

// Close releases every registered table.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.tables {
		v.Release()
	}
	s.tables = nil
}

// Tables returns the list of registered table names.
func (s *Session) Tables() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.tables))
	for n := range s.tables {
		out = append(out, n)
	}
	return out
}

// Query compiles SQL into a lazy plan, executes it, and returns the
// resulting DataFrame. Caller owns the result and must Release.
func (s *Session) Query(ctx context.Context, query string) (*dataframe.DataFrame, error) {
	stmt, err := Parse(query)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	df, ok := s.tables[stmt.From]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("sql: unknown table %q", stmt.From)
	}
	return stmt.Execute(ctx, df)
}
