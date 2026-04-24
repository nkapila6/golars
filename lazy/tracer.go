package lazy

import "context"

// Tracer is the minimal interface golars calls into for observability.
// It's deliberately smaller than the otel Tracer API: we only need a
// start-span/end-span pair around each lazy plan node. That keeps the
// lazy package's import graph tight - users who want OpenTelemetry
// wire up a thin adapter that implements this interface.
//
// Example adapter (user-side):
//
//	type otelAdapter struct{ tracer trace.Tracer }
//	func (a otelAdapter) StartSpan(ctx context.Context, name string) (context.Context, lazy.Span) {
//	    ctx, span := a.tracer.Start(ctx, name)
//	    return ctx, &otelSpan{span: span}
//	}
//	type otelSpan struct{ span trace.Span }
//	func (s *otelSpan) End()                      { s.span.End() }
//	func (s *otelSpan) SetAttr(k string, v any) { s.span.SetAttributes(attribute.String(k, fmt.Sprint(v))) }
type Tracer interface {
	StartSpan(ctx context.Context, name string) (context.Context, Span)
}

// Span mirrors the minimum of an OTel trace.Span.
type Span interface {
	End()
	SetAttr(key string, value any)
}

// WithTracer attaches t to the execution so each node wraps its
// work in a span. Zero impact when no tracer is set.
func WithTracer(t Tracer) ExecOption {
	return func(c *execConfig) { c.tracer = t }
}

// noopSpan is returned when no tracer is attached; every method is a
// no-op so hot paths pay nothing.
type noopSpan struct{}

func (noopSpan) End()                    {}
func (noopSpan) SetAttr(_ string, _ any) {}

// tracerSpan starts a span or returns a noop when tracer is nil.
func tracerSpan(ctx context.Context, tracer Tracer, name string) (context.Context, Span) {
	if tracer == nil {
		return ctx, noopSpan{}
	}
	return tracer.StartSpan(ctx, name)
}
