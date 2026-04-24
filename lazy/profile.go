package lazy

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Profiler records per-node wall-clock time and output row counts
// during plan execution. Attach one via `lf.Collect(ctx,
// lazy.WithProfiler(p))` and inspect via `p.Report()` or
// `p.ChromeTrace()` once the query finishes.
//
// A zero Profiler is ready to use. Concurrent safe; workers update
// distinct slots keyed by node type + unique ID.
type Profiler struct {
	mu    sync.Mutex
	spans []ProfileSpan
}

// ProfileSpan is a single node execution record.
type ProfileSpan struct {
	Name      string        // e.g. "Projection", "Filter", "Sort"
	Detail    string        // String() of the node, abbreviated
	Duration  time.Duration // wall-clock start->end
	Rows      int           // output row count
	StartedAt time.Time     // for chrome-trace export
}

// NewProfiler returns a fresh Profiler.
func NewProfiler() *Profiler { return &Profiler{} }

// WithProfiler attaches p to the execution run.
func WithProfiler(p *Profiler) ExecOption {
	return func(c *execConfig) { c.profiler = p }
}

// record appends a span. Safe to call from any goroutine.
func (p *Profiler) record(s ProfileSpan) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.spans = append(p.spans, s)
}

// Spans returns the recorded spans in start-time order.
func (p *Profiler) Spans() []ProfileSpan {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]ProfileSpan, len(p.spans))
	copy(out, p.spans)
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out
}

// Report returns a human-readable table of per-node timings.
func (p *Profiler) Report() string {
	spans := p.Spans()
	if len(spans) == 0 {
		return "no profile data"
	}
	var total time.Duration
	for _, s := range spans {
		total += s.Duration
	}
	var out strings.Builder
	out.WriteString(fmt.Sprintf("%-18s %-40s %10s %10s\n", "node", "detail", "wall", "rows"))
	out.WriteString(fmt.Sprintf("%-18s %-40s %10s %10s\n", "----", "------", "----", "----"))
	for _, s := range spans {
		detail := s.Detail
		if len(detail) > 40 {
			detail = detail[:37] + "..."
		}
		out.WriteString(fmt.Sprintf("%-18s %-40s %10s %10d\n",
			s.Name, detail, s.Duration.Round(time.Microsecond), s.Rows))
	}
	out.WriteString(fmt.Sprintf("\n%-18s %-40s %10s\n", "TOTAL", "", total.Round(time.Microsecond)))
	return out.String()
}

// ChromeTrace emits a JSON object in the Chrome tracing format so a
// profile can be loaded directly into chrome://tracing or
// https://ui.perfetto.dev. Each span becomes one "complete" event.
func (p *Profiler) ChromeTrace() string {
	spans := p.Spans()
	if len(spans) == 0 {
		return `{"traceEvents":[]}`
	}
	base := spans[0].StartedAt
	var buf []byte
	buf = append(buf, []byte(`{"traceEvents":[`)...)
	for i, s := range spans {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, fmt.Appendf(nil,
			`{"name":"%s","cat":"lazy","ph":"X","pid":1,"tid":1,"ts":%d,"dur":%d,"args":{"rows":%d,"detail":%q}}`,
			s.Name,
			s.StartedAt.Sub(base).Microseconds(),
			s.Duration.Microseconds(),
			s.Rows,
			s.Detail,
		)...)
	}
	buf = append(buf, []byte(`]}`)...)
	return string(buf)
}
