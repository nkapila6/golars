package series

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ListOps is the namespace for operations over list-typed Series.
// Reach it via `s.List()`. Mirrors polars' `s.list.*` surface.
type ListOps struct{ s *Series }

// List returns the list namespace. Every method on the returned
// ListOps fails with a clear error when the underlying Series is
// not list-typed.
func (s *Series) List() ListOps { return ListOps{s: s} }

// unwrapList returns the single-chunk List view of the backing
// Series. Works for both List32 (`arrow.LIST`) and List64
// (`arrow.LARGE_LIST`).
func (o ListOps) unwrapList(op string) (listView, error) {
	if !o.s.DType().IsList() {
		return listView{}, fmt.Errorf("series.list.%s: %q has dtype %s (need list)",
			op, o.s.Name(), o.s.DType())
	}
	r := o.s.Rechunk()
	defer r.Release()
	return toListView(r.Chunk(0), op)
}

// listView is a uniform accessor over List / LargeList arrays.
type listView struct {
	arr      arrow.Array
	values   arrow.Array
	starts   []int32
	startsL  []int64
	isLarge  bool
	isNullFn func(int) bool
}

func toListView(a arrow.Array, op string) (listView, error) {
	switch v := a.(type) {
	case *array.List:
		offs := v.Offsets()
		v.Retain()
		return listView{
			arr: v, values: v.ListValues(), starts: offs,
			isNullFn: v.IsNull,
		}, nil
	case *array.LargeList:
		offs := v.Offsets()
		v.Retain()
		return listView{
			arr: v, values: v.ListValues(), startsL: offs,
			isLarge: true, isNullFn: v.IsNull,
		}, nil
	}
	return listView{}, fmt.Errorf("series.list.%s: unsupported array type %T", op, a)
}

func (v listView) Release() { v.arr.Release() }
func (v listView) Len() int { return v.arr.Len() }
func (v listView) IsNull(i int) bool {
	return v.isNullFn(i)
}
func (v listView) Range(i int) (int, int) {
	if v.isLarge {
		return int(v.startsL[i]), int(v.startsL[i+1])
	}
	return int(v.starts[i]), int(v.starts[i+1])
}

// Len returns the length of each inner list as an int64 Series.
// Nulls in the input are null in the output.
func (o ListOps) Len() (*Series, error) {
	lv, err := o.unwrapList("len")
	if err != nil {
		return nil, err
	}
	defer lv.Release()
	n := lv.Len()
	out := make([]int64, n)
	valid := make([]bool, n)
	for i := 0; i < n; i++ {
		if lv.IsNull(i) {
			continue
		}
		start, end := lv.Range(i)
		out[i] = int64(end - start)
		valid[i] = true
	}
	return FromInt64(o.s.Name(), out, valid)
}

// Sum returns the sum of each inner list. Numeric inner dtypes
// (int64, int32, float64, float32) are supported. Integer lists
// produce an int64 output; float lists produce float64.
func (o ListOps) Sum() (*Series, error) { return o.reduceNumeric("sum") }

// Mean returns the arithmetic mean of each inner list as float64.
// Empty and null lists yield null.
func (o ListOps) Mean() (*Series, error) { return o.reduceNumeric("mean") }

// Min returns the minimum of each inner list. Dtype matches the
// list's inner scalar type.
func (o ListOps) Min() (*Series, error) { return o.reduceNumeric("min") }

// Max returns the maximum of each inner list. Dtype matches the
// list's inner scalar type.
func (o ListOps) Max() (*Series, error) { return o.reduceNumeric("max") }

// Get returns the element at position idx from each inner list.
// Negative idx counts from the back. Out-of-range positions yield
// null. Supported inner dtypes: Int64, Int32, Float64, Float32,
// String, Boolean.
func (o ListOps) Get(idx int) (*Series, error) {
	lv, err := o.unwrapList("get")
	if err != nil {
		return nil, err
	}
	defer lv.Release()
	n := lv.Len()
	pick := make([]int, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if lv.IsNull(i) {
			continue
		}
		start, end := lv.Range(i)
		length := end - start
		j := idx
		if j < 0 {
			j += length
		}
		if j < 0 || j >= length {
			continue
		}
		pick[i] = start + j
		mask[i] = true
	}
	return gatherByMask(lv.values, pick, mask, o.s.Name())
}

// First returns the first element of each inner list (equivalent to
// Get(0)). Null/empty lists yield null.
func (o ListOps) First() (*Series, error) { return o.Get(0) }

// Last returns the last element of each inner list. Null/empty
// lists yield null.
func (o ListOps) Last() (*Series, error) { return o.Get(-1) }

// Contains returns a boolean Series marking rows whose inner list
// holds the given scalar. The scalar's Go type must match the
// list's inner dtype.
func (o ListOps) Contains(v any) (*Series, error) {
	lv, err := o.unwrapList("contains")
	if err != nil {
		return nil, err
	}
	defer lv.Release()
	n := lv.Len()
	out := make([]bool, n)
	valid := make([]bool, n)
	for i := 0; i < n; i++ {
		if lv.IsNull(i) {
			continue
		}
		valid[i] = true
		start, end := lv.Range(i)
		out[i] = scalarInSlice(lv.values, start, end, v)
	}
	return FromBool(o.s.Name(), out, valid)
}

// Join concatenates the string elements of each inner list with the
// given separator. Requires the inner dtype to be utf8. Null inner
// elements are skipped; null or empty lists produce "" (matching
// polars' default).
func (o ListOps) Join(sep string) (*Series, error) {
	lv, err := o.unwrapList("join")
	if err != nil {
		return nil, err
	}
	defer lv.Release()
	sa, ok := lv.values.(*array.String)
	if !ok {
		return nil, fmt.Errorf("series.list.join: inner dtype must be utf8, got %s",
			lv.values.DataType())
	}
	n := lv.Len()
	out := make([]string, n)
	valid := make([]bool, n)
	for i := 0; i < n; i++ {
		valid[i] = true
		if lv.IsNull(i) {
			continue
		}
		start, end := lv.Range(i)
		var b strings.Builder
		first := true
		for j := start; j < end; j++ {
			if sa.IsNull(j) {
				continue
			}
			if !first {
				b.WriteString(sep)
			}
			b.WriteString(sa.Value(j))
			first = false
		}
		out[i] = b.String()
	}
	return FromString(o.s.Name(), out, valid)
}

// --- private helpers ---------------------------------------------

func (o ListOps) reduceNumeric(op string) (*Series, error) {
	lv, err := o.unwrapList(op)
	if err != nil {
		return nil, err
	}
	defer lv.Release()
	switch v := lv.values.(type) {
	case *array.Int64:
		return reduceIntList(o.s.Name(), lv, v.Int64Values(), v, op)
	case *array.Int32:
		return reduceInt32List(o.s.Name(), lv, v, op)
	case *array.Float64:
		return reduceFloatList(o.s.Name(), lv, v.Float64Values(), v, op)
	case *array.Float32:
		return reduceFloat32List(o.s.Name(), lv, v, op)
	}
	return nil, fmt.Errorf("series.list.%s: unsupported inner dtype %s", op, lv.values.DataType())
}

func reduceIntList(name string, lv listView, raw []int64, src *array.Int64, op string) (*Series, error) {
	n := lv.Len()
	valid := make([]bool, n)
	if op == "mean" {
		out := make([]float64, n)
		for i := 0; i < n; i++ {
			if lv.IsNull(i) {
				continue
			}
			start, end := lv.Range(i)
			var sum int64
			var cnt int64
			for j := start; j < end; j++ {
				if src.IsNull(j) {
					continue
				}
				sum += raw[j]
				cnt++
			}
			if cnt == 0 {
				continue
			}
			out[i] = float64(sum) / float64(cnt)
			valid[i] = true
		}
		return FromFloat64(name, out, valid)
	}
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		if lv.IsNull(i) {
			continue
		}
		start, end := lv.Range(i)
		var acc int64
		init := false
		for j := start; j < end; j++ {
			if src.IsNull(j) {
				continue
			}
			if !init {
				acc = raw[j]
				init = true
				continue
			}
			switch op {
			case "sum":
				acc += raw[j]
			case "min":
				if raw[j] < acc {
					acc = raw[j]
				}
			case "max":
				if raw[j] > acc {
					acc = raw[j]
				}
			}
		}
		if !init {
			continue
		}
		out[i] = acc
		valid[i] = true
	}
	return FromInt64(name, out, valid)
}

func reduceInt32List(name string, lv listView, src *array.Int32, op string) (*Series, error) {
	n := lv.Len()
	valid := make([]bool, n)
	if op == "mean" {
		out := make([]float64, n)
		for i := 0; i < n; i++ {
			if lv.IsNull(i) {
				continue
			}
			start, end := lv.Range(i)
			var sum int64
			var cnt int64
			for j := start; j < end; j++ {
				if src.IsNull(j) {
					continue
				}
				sum += int64(src.Value(j))
				cnt++
			}
			if cnt == 0 {
				continue
			}
			out[i] = float64(sum) / float64(cnt)
			valid[i] = true
		}
		return FromFloat64(name, out, valid)
	}
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		if lv.IsNull(i) {
			continue
		}
		start, end := lv.Range(i)
		var acc int64
		init := false
		for j := start; j < end; j++ {
			if src.IsNull(j) {
				continue
			}
			v := int64(src.Value(j))
			if !init {
				acc = v
				init = true
				continue
			}
			switch op {
			case "sum":
				acc += v
			case "min":
				if v < acc {
					acc = v
				}
			case "max":
				if v > acc {
					acc = v
				}
			}
		}
		if !init {
			continue
		}
		out[i] = acc
		valid[i] = true
	}
	return FromInt64(name, out, valid)
}

func reduceFloatList(name string, lv listView, raw []float64, src *array.Float64, op string) (*Series, error) {
	n := lv.Len()
	out := make([]float64, n)
	valid := make([]bool, n)
	for i := 0; i < n; i++ {
		if lv.IsNull(i) {
			continue
		}
		start, end := lv.Range(i)
		var acc, sum float64
		var cnt int64
		init := false
		for j := start; j < end; j++ {
			if src.IsNull(j) {
				continue
			}
			v := raw[j]
			sum += v
			cnt++
			if !init {
				acc = v
				init = true
				continue
			}
			switch op {
			case "sum":
				acc += v
			case "min":
				if v < acc {
					acc = v
				}
			case "max":
				if v > acc {
					acc = v
				}
			}
		}
		if !init {
			continue
		}
		if op == "mean" {
			out[i] = sum / float64(cnt)
		} else {
			out[i] = acc
		}
		valid[i] = true
	}
	return FromFloat64(name, out, valid)
}

func reduceFloat32List(name string, lv listView, src *array.Float32, op string) (*Series, error) {
	n := lv.Len()
	out := make([]float64, n)
	valid := make([]bool, n)
	for i := 0; i < n; i++ {
		if lv.IsNull(i) {
			continue
		}
		start, end := lv.Range(i)
		var acc, sum float64
		var cnt int64
		init := false
		for j := start; j < end; j++ {
			if src.IsNull(j) {
				continue
			}
			v := float64(src.Value(j))
			sum += v
			cnt++
			if !init {
				acc = v
				init = true
				continue
			}
			switch op {
			case "sum":
				acc += v
			case "min":
				if v < acc {
					acc = v
				}
			case "max":
				if v > acc {
					acc = v
				}
			}
		}
		if !init {
			continue
		}
		if op == "mean" {
			out[i] = sum / float64(cnt)
		} else {
			out[i] = acc
		}
		valid[i] = true
	}
	return FromFloat64(name, out, valid)
}

// gatherByMask copies src[pick[i]] into a new Series where mask[i]
// is true; otherwise emits null. Used by ListOps.Get / First / Last.
func gatherByMask(src arrow.Array, pick []int, mask []bool, name string) (*Series, error) {
	n := len(pick)
	mem := memory.DefaultAllocator
	switch s := src.(type) {
	case *array.Int64:
		raw := s.Int64Values()
		out := make([]int64, n)
		for i := 0; i < n; i++ {
			if mask[i] && s.IsNull(pick[i]) {
				mask[i] = false
				continue
			}
			if mask[i] {
				out[i] = raw[pick[i]]
			}
		}
		return FromInt64(name, out, mask)
	case *array.Int32:
		out := make([]int32, n)
		for i := 0; i < n; i++ {
			if mask[i] && s.IsNull(pick[i]) {
				mask[i] = false
				continue
			}
			if mask[i] {
				out[i] = s.Value(pick[i])
			}
		}
		return FromInt32(name, out, mask)
	case *array.Float64:
		raw := s.Float64Values()
		out := make([]float64, n)
		for i := 0; i < n; i++ {
			if mask[i] && s.IsNull(pick[i]) {
				mask[i] = false
				continue
			}
			if mask[i] {
				out[i] = raw[pick[i]]
			}
		}
		return FromFloat64(name, out, mask)
	case *array.Float32:
		out := make([]float32, n)
		for i := 0; i < n; i++ {
			if mask[i] && s.IsNull(pick[i]) {
				mask[i] = false
				continue
			}
			if mask[i] {
				out[i] = s.Value(pick[i])
			}
		}
		return FromFloat32(name, out, mask)
	case *array.Boolean:
		out := make([]bool, n)
		for i := 0; i < n; i++ {
			if mask[i] && s.IsNull(pick[i]) {
				mask[i] = false
				continue
			}
			if mask[i] {
				out[i] = s.Value(pick[i])
			}
		}
		return FromBool(name, out, mask)
	case *array.String:
		out := make([]string, n)
		for i := 0; i < n; i++ {
			if mask[i] && s.IsNull(pick[i]) {
				mask[i] = false
				continue
			}
			if mask[i] {
				out[i] = s.Value(pick[i])
			}
		}
		return FromString(name, out, mask)
	}
	_ = mem
	return nil, fmt.Errorf("series.list.get: unsupported inner dtype %s", src.DataType())
}

// scalarInSlice returns true when the scalar v appears in
// src[start:end]. Needle type must match the array's element type.
func scalarInSlice(src arrow.Array, start, end int, v any) bool {
	switch s := src.(type) {
	case *array.Int64:
		needle, ok := toInt64(v)
		if !ok {
			return false
		}
		vals := s.Int64Values()
		for j := start; j < end; j++ {
			if !s.IsNull(j) && vals[j] == needle {
				return true
			}
		}
	case *array.Int32:
		needle, ok := toInt64(v)
		if !ok {
			return false
		}
		for j := start; j < end; j++ {
			if !s.IsNull(j) && int64(s.Value(j)) == needle {
				return true
			}
		}
	case *array.Float64:
		needle, ok := toFloat64(v)
		if !ok {
			return false
		}
		vals := s.Float64Values()
		for j := start; j < end; j++ {
			if !s.IsNull(j) && vals[j] == needle {
				return true
			}
		}
	case *array.Float32:
		needle, ok := toFloat64(v)
		if !ok {
			return false
		}
		for j := start; j < end; j++ {
			if !s.IsNull(j) && float64(s.Value(j)) == needle {
				return true
			}
		}
	case *array.String:
		needle, ok := v.(string)
		if !ok {
			return false
		}
		for j := start; j < end; j++ {
			if !s.IsNull(j) && s.Value(j) == needle {
				return true
			}
		}
	case *array.Boolean:
		needle, ok := v.(bool)
		if !ok {
			return false
		}
		for j := start; j < end; j++ {
			if !s.IsNull(j) && s.Value(j) == needle {
				return true
			}
		}
	}
	return false
}

