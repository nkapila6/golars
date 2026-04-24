package series

import (
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/intmap"
)

// ValueCounts returns two parallel Series: the distinct values in s
// (in first-occurrence order) and their uint32 counts. Nulls are
// dropped (like polars value_counts by default). Callers wanting a
// DataFrame can pass both Series to dataframe.New.
//
// Output column names: the value Series takes s.Name(), the count
// Series is named "count". Sorted by descending count when
// sortByCount is true.
func (s *Series) ValueCounts(sortByCount bool, opts ...Option) (values, counts *Series, err error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	switch a := chunk.(type) {
	case *array.Int64:
		return vcInt64(s.Name(), a, sortByCount, cfg.alloc)
	case *array.Int32:
		return vcInt32(s.Name(), a, sortByCount, cfg.alloc)
	case *array.String:
		return vcString(s.Name(), a, sortByCount, cfg.alloc)
	case *array.Boolean:
		return vcBool(s.Name(), a, sortByCount, cfg.alloc)
	}
	return nil, nil, fmt.Errorf("series: ValueCounts unsupported for dtype %s", s.DType())
}

func vcInt64(name string, a *array.Int64, sortByCount bool, mem memory.Allocator) (*Series, *Series, error) {
	n := a.Len()
	raw := a.Int64Values()
	seen := intmap.New(max(n/4, 16))
	defer seen.Release()
	uniq := make([]int64, 0, n/4)
	var countSlots []uint32
	for i := range n {
		if a.IsNull(i) {
			continue
		}
		v := raw[i]
		if slot, inserted := seen.InsertOrGet(v, int32(len(uniq))); inserted {
			uniq = append(uniq, v)
			countSlots = append(countSlots, 1)
		} else {
			countSlots[slot]++
		}
	}
	if sortByCount {
		sortByCountPaired(countSlots, func(i, j int) {
			uniq[i], uniq[j] = uniq[j], uniq[i]
		})
	}
	vs, err := FromInt64(name, uniq, nil, WithAllocator(mem))
	if err != nil {
		return nil, nil, err
	}
	cs, err := FromUint32("count", countSlots, nil, WithAllocator(mem))
	if err != nil {
		vs.Release()
		return nil, nil, err
	}
	return vs, cs, nil
}

func vcInt32(name string, a *array.Int32, sortByCount bool, mem memory.Allocator) (*Series, *Series, error) {
	n := a.Len()
	raw := a.Int32Values()
	seen := intmap.New(max(n/4, 16))
	defer seen.Release()
	uniq := make([]int32, 0, n/4)
	var countSlots []uint32
	for i := range n {
		if a.IsNull(i) {
			continue
		}
		v := raw[i]
		if slot, inserted := seen.InsertOrGet(int64(v), int32(len(uniq))); inserted {
			uniq = append(uniq, v)
			countSlots = append(countSlots, 1)
		} else {
			countSlots[slot]++
		}
	}
	if sortByCount {
		sortByCountPaired(countSlots, func(i, j int) {
			uniq[i], uniq[j] = uniq[j], uniq[i]
		})
	}
	vs, err := FromInt32(name, uniq, nil, WithAllocator(mem))
	if err != nil {
		return nil, nil, err
	}
	cs, err := FromUint32("count", countSlots, nil, WithAllocator(mem))
	if err != nil {
		vs.Release()
		return nil, nil, err
	}
	return vs, cs, nil
}

func vcString(name string, a *array.String, sortByCount bool, mem memory.Allocator) (*Series, *Series, error) {
	n := a.Len()
	seen := make(map[string]int, n/4)
	var uniq []string
	var countSlots []uint32
	for i := range n {
		if a.IsNull(i) {
			continue
		}
		v := a.Value(i)
		if slot, ok := seen[v]; ok {
			countSlots[slot]++
		} else {
			seen[v] = len(uniq)
			uniq = append(uniq, v)
			countSlots = append(countSlots, 1)
		}
	}
	if sortByCount {
		sortByCountPaired(countSlots, func(i, j int) {
			uniq[i], uniq[j] = uniq[j], uniq[i]
		})
	}
	vs, err := FromString(name, uniq, nil, WithAllocator(mem))
	if err != nil {
		return nil, nil, err
	}
	cs, err := FromUint32("count", countSlots, nil, WithAllocator(mem))
	if err != nil {
		vs.Release()
		return nil, nil, err
	}
	return vs, cs, nil
}

func vcBool(name string, a *array.Boolean, sortByCount bool, mem memory.Allocator) (*Series, *Series, error) {
	n := a.Len()
	var countsT, countsF uint32
	for i := range n {
		if a.IsNull(i) {
			continue
		}
		if a.Value(i) {
			countsT++
		} else {
			countsF++
		}
	}
	// Emit in first-occurrence order unless sortByCount flips it.
	first := true
	for i := range n {
		if !a.IsNull(i) {
			first = a.Value(i)
			break
		}
	}
	var uniq []bool
	var cnts []uint32
	if countsT == 0 && countsF == 0 {
		// empty
	} else if countsT > 0 && countsF == 0 {
		uniq = []bool{true}
		cnts = []uint32{countsT}
	} else if countsT == 0 && countsF > 0 {
		uniq = []bool{false}
		cnts = []uint32{countsF}
	} else if first {
		uniq = []bool{true, false}
		cnts = []uint32{countsT, countsF}
	} else {
		uniq = []bool{false, true}
		cnts = []uint32{countsF, countsT}
	}
	if sortByCount && len(cnts) == 2 && cnts[0] < cnts[1] {
		uniq[0], uniq[1] = uniq[1], uniq[0]
		cnts[0], cnts[1] = cnts[1], cnts[0]
	}
	vs, err := FromBool(name, uniq, nil, WithAllocator(mem))
	if err != nil {
		return nil, nil, err
	}
	cs, err := FromUint32("count", cnts, nil, WithAllocator(mem))
	if err != nil {
		vs.Release()
		return nil, nil, err
	}
	return vs, cs, nil
}

// sortByCountPaired sorts counts in descending order in place and
// applies the same permutation to the caller's value slice via the
// swap callback. Ties preserve first-occurrence order (stable sort).
func sortByCountPaired(counts []uint32, swap func(i, j int)) {
	n := len(counts)
	if n < 2 {
		return
	}
	// Build a stable-sorted index permutation on the ORIGINAL counts.
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	sort.SliceStable(perm, func(i, j int) bool {
		return counts[perm[i]] > counts[perm[j]]
	})
	// Write counts and values into temp layouts ordered by perm, then
	// copy back. Simpler and less error-prone than cycle decomposition.
	tmpCounts := make([]uint32, n)
	for i, p := range perm {
		tmpCounts[i] = counts[p]
	}
	// Apply the permutation to values by driving it through selection
	// swaps: for each target index i, pull the value originally at
	// perm[i] into slot i. Since swap() only moves two positions and
	// we don't track "which original value is now where", use a plain
	// cycle walk. Track per-index placement.
	applyPerm(perm, swap)
	copy(counts, tmpCounts)
}

// applyPerm rearranges the caller-managed slice so that slot i holds
// the element originally at perm[i], via swap(i, j) calls. Implemented
// as an in-place cycle-walk in O(n): for each i, walk the cycle
// containing it, pulling the right element into slot i and marking
// visited positions by setting perm[visited] = visited. perm is
// modified in-place.
func applyPerm(perm []int, swap func(i, j int)) {
	n := len(perm)
	for i := range n {
		current := i
		for perm[current] != i {
			next := perm[current]
			swap(current, next)
			perm[current] = current
			current = next
		}
		perm[current] = current
	}
}
