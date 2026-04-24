package dataframe

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// ErrNotStruct is returned when Unnest is called on a non-struct column.
var ErrNotStruct = errors.New("dataframe: column is not a struct")

// Unnest replaces a struct-typed column with its fields promoted to
// top-level columns. Mirrors polars' df.unnest.
//
// Each struct field becomes a top-level Series whose name is taken
// from the field. Name collisions with an existing column return
// ErrDuplicateColumn. The struct column itself is removed from the
// result; surrounding columns keep their relative position.
//
// The struct-level null bitmap (if any) is OR-ed into each child's
// validity so "row i is null on the struct as a whole" becomes
// "row i is null in every unnested child", matching polars.
func (df *DataFrame) Unnest(ctx context.Context, col string) (*DataFrame, error) {
	_ = ctx
	idx, ok := df.sch.Index(col)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, col)
	}
	s := df.cols[idx]
	if !s.DType().IsStruct() {
		return nil, fmt.Errorf("%w: %q has dtype %s", ErrNotStruct, col, s.DType())
	}
	children, err := unnestSeries(s)
	if err != nil {
		return nil, err
	}
	existing := make(map[string]struct{}, df.Width())
	for i, c := range df.cols {
		if i == idx {
			continue
		}
		existing[c.Name()] = struct{}{}
	}
	for _, c := range children {
		if _, dup := existing[c.Name()]; dup {
			name := c.Name()
			for _, k := range children {
				k.Release()
			}
			return nil, fmt.Errorf("%w: field %q collides with existing column",
				ErrDuplicateColumn, name)
		}
	}
	out := make([]*series.Series, 0, df.Width()-1+len(children))
	for i, c := range df.cols {
		if i == idx {
			out = append(out, children...)
			continue
		}
		out = append(out, c.Clone())
	}
	res, err := New(out...)
	if err != nil {
		for _, c := range out {
			c.Release()
		}
		return nil, err
	}
	return res, nil
}

// unnestSeries splits a struct-typed Series into one Series per
// field. Each returned Series has the struct-level null bitmap
// folded into its validity. Callers own the returned references.
func unnestSeries(s *series.Series) ([]*series.Series, error) {
	ch := s.ToArrowChunked()
	defer ch.Release()
	st, ok := ch.DataType().(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("dataframe: %q is not a struct array", s.Name())
	}
	if ch.Len() == 0 {
		out := make([]*series.Series, st.NumFields())
		for i, f := range st.Fields() {
			out[i] = series.Empty(f.Name, dtype.FromArrow(f.Type))
		}
		return out, nil
	}
	numFields := st.NumFields()
	fieldChunks := make([][]arrow.Array, numFields)
	for _, chunk := range ch.Chunks() {
		sa, ok := chunk.(*array.Struct)
		if !ok {
			return nil, fmt.Errorf("dataframe: chunk of %q is not a struct array", s.Name())
		}
		for f := 0; f < numFields; f++ {
			child := sa.Field(f)
			merged := foldStructNulls(sa, child)
			fieldChunks[f] = append(fieldChunks[f], merged)
		}
	}
	out := make([]*series.Series, numFields)
	for f := 0; f < numFields; f++ {
		ser, err := series.New(st.Field(f).Name, fieldChunks[f]...)
		if err != nil {
			for _, c := range out[:f] {
				c.Release()
			}
			for _, chunk := range fieldChunks[f] {
				chunk.Release()
			}
			return nil, err
		}
		out[f] = ser
	}
	return out, nil
}

// foldStructNulls OR-s the parent struct's null bitmap into the
// child's validity, returning a fresh arrow.Array. If the parent
// has no nulls, the child is returned unchanged with an extra
// Retain so the caller can release uniformly.
func foldStructNulls(parent *array.Struct, child arrow.Array) arrow.Array {
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
		pValid := bitSet(pBits, pOff+i)
		cValid := allChildValid || bitSet(cBits, cOff+i)
		if pValid && cValid {
			dst[i/8] |= 1 << (uint(i) & 7)
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
	childrenData := oldData.Children()
	for _, cd := range childrenData {
		cd.Retain()
	}
	newData := array.NewData(child.DataType(), n, newBufs, childrenData, nulls, oldData.Offset())
	out := array.MakeFromData(newData)
	newData.Release()
	return out
}

func bitSet(bits []byte, i int) bool {
	if len(bits) == 0 {
		return true
	}
	return bits[i>>3]&(1<<(uint(i)&7)) != 0
}
