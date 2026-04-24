package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// StructOps is the namespace for operations over struct-typed Series.
// Reach it via `s.Struct()`. Mirrors polars' `s.struct.*` surface.
type StructOps struct{ s *Series }

// Struct returns the struct namespace. Every method fails with a
// clear error when the underlying Series is not struct-typed.
func (s *Series) Struct() StructOps { return StructOps{s: s} }

// Field returns the named field of the struct as a new top-level
// Series. Struct-level nulls propagate into the child's validity so
// downstream kernels see consistent null rows.
func (o StructOps) Field(name string) (*Series, error) {
	if !o.s.DType().IsStruct() {
		return nil, fmt.Errorf("series.struct.field: %q has dtype %s (need struct)",
			o.s.Name(), o.s.DType())
	}
	r := o.s.Rechunk()
	defer r.Release()
	sa, ok := r.Chunk(0).(*array.Struct)
	if !ok {
		return nil, fmt.Errorf("series.struct.field: chunk is not a struct array")
	}
	st := sa.DataType().(*arrow.StructType)
	idx, ok := st.FieldIdx(name)
	if !ok {
		return nil, fmt.Errorf("series.struct.field: no field %q (struct has %s)", name, st)
	}
	child := sa.Field(idx)
	folded := foldStructNullsInto(sa, child)
	return New(name, folded)
}

// FieldNames returns the struct's field names in declaration order.
func (o StructOps) FieldNames() ([]string, error) {
	if !o.s.DType().IsStruct() {
		return nil, fmt.Errorf("series.struct.field_names: %q has dtype %s (need struct)",
			o.s.Name(), o.s.DType())
	}
	st, ok := o.s.DType().Arrow().(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("series.struct.field_names: %q has no struct metadata", o.s.Name())
	}
	out := make([]string, st.NumFields())
	for i, f := range st.Fields() {
		out[i] = f.Name
	}
	return out, nil
}

// NumFields returns the field count of the struct.
func (o StructOps) NumFields() (int, error) {
	names, err := o.FieldNames()
	if err != nil {
		return 0, err
	}
	return len(names), nil
}

// foldStructNullsInto returns a child array whose validity buffer
// has the parent struct's nulls OR-ed in. No-op (Retain only) when
// the parent has no null rows.
func foldStructNullsInto(parent *array.Struct, child arrow.Array) arrow.Array {
	if parent.NullN() == 0 {
		child.Retain()
		return child
	}
	n := parent.Len()
	mem := memory.DefaultAllocator
	merged := memory.NewResizableBuffer(mem)
	merged.Resize((n + 7) / 8)
	dst := merged.Bytes()
	for i := range dst {
		dst[i] = 0
	}
	pBits := parent.NullBitmapBytes()
	pOff := parent.Data().Offset()
	cBits := child.NullBitmapBytes()
	cOff := child.Data().Offset()
	allChildValid := len(cBits) == 0
	nulls := 0
	for i := 0; i < n; i++ {
		pValid := bitGetFold(pBits, pOff+i)
		cValid := allChildValid || bitGetFold(cBits, cOff+i)
		if pValid && cValid {
			dst[i>>3] |= 1 << (uint(i) & 7)
		} else {
			nulls++
		}
	}
	oldData := child.Data()
	oldBufs := oldData.Buffers()
	newBufs := make([]*memory.Buffer, len(oldBufs))
	if len(newBufs) == 0 {
		newBufs = []*memory.Buffer{merged}
	} else {
		newBufs[0] = merged
		for i := 1; i < len(oldBufs); i++ {
			if b := oldBufs[i]; b != nil {
				b.Retain()
				newBufs[i] = b
			}
		}
	}
	children := oldData.Children()
	for _, cd := range children {
		cd.Retain()
	}
	newData := array.NewData(child.DataType(), n, newBufs, children, nulls, oldData.Offset())
	out := array.MakeFromData(newData)
	newData.Release()
	return out
}

func bitGetFold(bits []byte, i int) bool {
	if len(bits) == 0 {
		return true
	}
	return bits[i>>3]&(1<<(uint(i)&7)) != 0
}
