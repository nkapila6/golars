package series

import (
	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

// IsNull returns a new boolean Series where each position is true iff
// the corresponding value in s is null. Mirrors polars Series.is_null.
// The returned Series has no nulls itself.
func (s *Series) IsNull(opts ...Option) (*Series, error) {
	return s.nullMask(true, opts)
}

// IsNotNull is the elementwise inverse of IsNull.
func (s *Series) IsNotNull(opts ...Option) (*Series, error) {
	return s.nullMask(false, opts)
}

// nullMask builds a packed bitmap with bit i set iff
// isValid(i) XOR invert.
func (s *Series) nullMask(invert bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	return BuildBoolDirect(s.Name(), n, cfg.alloc, func(bits []byte) {
		if n == 0 {
			return
		}
		// Fast path: no nulls in source. All bits are known constant.
		// BuildBoolDirect already pre-zeroed; we only need to flip to
		// 0xff for IsNotNull.
		if chunk.NullN() == 0 {
			if !invert {
				for i := range bits {
					bits[i] = 0xff
				}
			}
			return
		}
		valid := chunk.NullBitmapBytes()
		off := chunk.Data().Offset()
		for i := range n {
			if bitutil.BitIsSet(valid, i+off) != invert {
				bits[i>>3] |= 1 << (i & 7)
			}
		}
	})
}
