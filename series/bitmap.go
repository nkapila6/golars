package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"unsafe"
)

// Shared validity-bitmap helpers. Most arith / compare / cast kernels
// used to round-trip through []bool:
//
//   bitmap -> []bool (per-row IsValid loop) -> FromInt64 -> bitmap
//
// The conversion to and from []bool is O(n) wasted work on top of the
// n/8-byte bitmap copy we actually need. These helpers let kernels
// stay in bitmap form: copy the bitmap bytes, hand to the builder.
//
// When an op preserves the input's null pattern (arithmetic, cast,
// bit ops on single column) the output bitmap == input bitmap, so we
// just copy bytes. For binary ops the output bitmap is the AND of the
// two inputs.

// CopyValidityBitmap returns a fresh memory.Buffer holding a's
// validity bitmap, or nil when a has no nulls. Caller owns the
// returned buffer.
//
// Offset-aware: if a is a slice view (offset > 0), the returned
// bitmap is rebased so bit 0 corresponds to element 0 of the slice.
func CopyValidityBitmap(a arrow.Array, alloc memory.Allocator) *memory.Buffer {
	if a.NullN() == 0 {
		return nil
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	n := a.Len()
	src := a.NullBitmapBytes()
	off := a.Data().Offset()

	buf := memory.NewResizableBuffer(alloc)
	buf.Resize(int(bitutil.BytesForBits(int64(n))))
	dst := buf.Bytes()
	if off == 0 {
		copy(dst, src[:len(dst)])
		return buf
	}
	// Rebased copy. Zero first, then set bits we want.
	for i := range dst {
		dst[i] = 0
	}
	for i := 0; i < n; i++ {
		abs := off + i
		if src[abs>>3]&(1<<uint(abs&7)) != 0 {
			dst[i>>3] |= 1 << uint(i&7)
		}
	}
	return buf
}

// AndValidityBitmap returns (buf, nullCount) where buf holds bit i set
// iff a.IsValid(i) AND b.IsValid(i). Returns (nil, 0) when neither
// input has nulls.
func AndValidityBitmap(a, b arrow.Array, alloc memory.Allocator) (*memory.Buffer, int) {
	if a.NullN() == 0 && b.NullN() == 0 {
		return nil, 0
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	n := a.Len()
	buf := memory.NewResizableBuffer(alloc)
	buf.Resize(int(bitutil.BytesForBits(int64(n))))
	dst := buf.Bytes()

	switch {
	case a.NullN() == 0:
		// Only b has nulls: output validity == b's validity.
		copyBitmapInto(dst, b, n)
	case b.NullN() == 0:
		copyBitmapInto(dst, a, n)
	default:
		av, bv := a.NullBitmapBytes(), b.NullBitmapBytes()
		aoff, boff := a.Data().Offset(), b.Data().Offset()
		if aoff == 0 && boff == 0 {
			nbytes := len(dst)
			for i := 0; i < nbytes; i++ {
				dst[i] = av[i] & bv[i]
			}
		} else {
			for i := range dst {
				dst[i] = 0
			}
			for i := 0; i < n; i++ {
				aa, bb := aoff+i, boff+i
				if (av[aa>>3]&(1<<uint(aa&7)) != 0) &&
					(bv[bb>>3]&(1<<uint(bb&7)) != 0) {
					dst[i>>3] |= 1 << uint(i&7)
				}
			}
		}
	}
	// Trim any padding bits that a byte-copy leaves set beyond n.
	if tail := n & 7; tail != 0 {
		mask := byte(1<<uint(tail)) - 1
		dst[len(dst)-1] &= mask
	}
	nulls := n - bitutil.CountSetBits(dst, 0, n)
	return buf, nulls
}

// copyBitmapInto copies src.NullBitmapBytes rebased to offset 0 into
// dst. Used by AndValidityBitmap's single-null fast paths.
func copyBitmapInto(dst []byte, src arrow.Array, n int) {
	bits := src.NullBitmapBytes()
	off := src.Data().Offset()
	if off == 0 {
		copy(dst, bits[:len(dst)])
		return
	}
	for i := range dst {
		dst[i] = 0
	}
	for i := 0; i < n; i++ {
		abs := off + i
		if bits[abs>>3]&(1<<uint(abs&7)) != 0 {
			dst[i>>3] |= 1 << uint(i&7)
		}
	}
}

// BuildInt64DirectWithValidity is BuildInt64Direct plus a caller-
// supplied validity bitmap. The buffer is consumed: the returned
// Series takes ownership and the caller must not Release nullBuf
// afterward.
//
// Passing nil nullBuf is legal and means "no nulls".
func BuildInt64DirectWithValidity(name string, n int, mem memory.Allocator,
	fill func(out []int64), nullBuf *memory.Buffer, nullCount int,
) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * arrow.Int64SizeBytes)
	defer buf.Release()
	if n > 0 {
		view := unsafe.Slice((*int64)(unsafe.Pointer(&buf.Bytes()[0])), n)
		fill(view)
	}
	data := array.NewData(arrow.PrimitiveTypes.Int64, n,
		[]*memory.Buffer{nullBuf, buf}, nil, nullCount, 0)
	arr := array.NewInt64Data(data)
	data.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}

// BuildBoolDirectWithValidity is BuildBoolDirect plus a caller-
// supplied validity bitmap. Data bits go in the `fill` callback's
// bitmap; validity stays in nullBuf. Use this to avoid the []bool
// round-trip in compare kernels that have both.
func BuildBoolDirectWithValidity(name string, n int, mem memory.Allocator,
	fill func(bits []byte), nullBuf *memory.Buffer, nullCount int,
) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	nb := bitutil.BytesForBits(int64(n))
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(nb))
	defer buf.Release()
	if nb > 0 {
		bytes := buf.Bytes()
		for i := range bytes {
			bytes[i] = 0
		}
		fill(bytes)
	}
	data := array.NewData(arrow.FixedWidthTypes.Boolean, n,
		[]*memory.Buffer{nullBuf, buf}, nil, nullCount, 0)
	arr := array.NewBooleanData(data)
	data.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}

// BuildFloat64DirectWithValidity is the float64 counterpart.
func BuildFloat64DirectWithValidity(name string, n int, mem memory.Allocator,
	fill func(out []float64), nullBuf *memory.Buffer, nullCount int,
) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * arrow.Float64SizeBytes)
	defer buf.Release()
	if n > 0 {
		view := unsafe.Slice((*float64)(unsafe.Pointer(&buf.Bytes()[0])), n)
		fill(view)
	}
	data := array.NewData(arrow.PrimitiveTypes.Float64, n,
		[]*memory.Buffer{nullBuf, buf}, nil, nullCount, 0)
	arr := array.NewFloat64Data(data)
	data.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}
