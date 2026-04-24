package series

import "github.com/apache/arrow-go/v18/arrow"

// ToArrow returns the Series' first chunk as an arrow.Array. The
// returned array shares memory with the Series; call Retain if you
// want to outlive the Series. Single-chunk series (the current norm)
// return their sole backing array.
//
// For multi-chunk inputs use ToArrowChunked.
func (s *Series) ToArrow() arrow.Array {
	a := s.Chunk(0)
	a.Retain()
	return a
}

// ToArrowChunked returns the underlying *arrow.Chunked with a fresh
// reference. Safe to keep after the Series is released. This is the
// zero-copy interop point for cross-language Arrow pipelines.
func (s *Series) ToArrowChunked() *arrow.Chunked {
	s.data.Retain()
	return s.data
}

// FromArrowArray wraps an arrow.Array as a single-chunk Series.
// Consumes the caller's reference on success (pass Retain first if
// you want to keep your own).
func FromArrowArray(name string, a arrow.Array) (*Series, error) {
	return New(name, a)
}

// FromArrowChunked wraps an existing *arrow.Chunked. The function
// retains a reference; the caller's reference is unaffected.
func FromArrowChunked(name string, c *arrow.Chunked) *Series {
	return FromChunked(name, c)
}
