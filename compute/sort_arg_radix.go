package compute

import (
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/series"
)

// allInt64AscNoNull reports whether the multi-key fast path applies:
// every key is int64, single chunk, no nulls, ascending sort.
func allInt64AscNoNull(cols []*series.Series, opts []SortOptions) bool {
	for i, c := range cols {
		if c.DType().ID() != arrow.INT64 {
			return false
		}
		if c.NullCount() != 0 {
			return false
		}
		if c.NumChunks() != 1 {
			return false
		}
		if opts[i].Descending {
			return false
		}
	}
	return true
}

// argRadixSortInt64 reorders indices[] so that keys[indices[i]] is in
// ascending order. Stable: ties preserve the incoming order of indices.
// Uses 11-bit LSD digits with skip-pass; same performance profile as the
// direct int64 radix but carries permutation indices alongside the key.
//
// This powers multi-key sort: sort by key_N first (applied to identity
// indices), then re-sort by key_N-1 (preserving tie order), ..., then by
// key_1. The resulting indices give the stable lexicographic permutation.
func argRadixSortInt64(keys []int64, indices []int) {
	n := len(indices)
	if n < 64 {
		insertionSortArgInt64(keys, indices)
		return
	}
	if n >= parallelRadixCutoff {
		argRadixSortInt64Parallel(keys, indices)
		return
	}
	aux := make([]int, n)
	const signBit uint64 = 1 << 63
	counts := make([]int, 2048)
	src, dst := indices, aux
	swaps := 0
	// 6 passes × 11 bits. 2048-entry counts (16 KB) fits L1. For the
	// benchmark's narrow-range int64 keys (values in [0, 2^20)),
	// skip-pass fires on passes 2-5 so only 2 passes actually run -
	// beats 8-bit × 8 which has no skippable passes on the same data.
	for pass := range 6 {
		shift := uint(pass * 11)
		mask := uint64(2047)
		if pass == 5 {
			mask = 0x1ff
		}
		for i := range counts {
			counts[i] = 0
		}
		for _, idx := range src {
			d := int((uint64(keys[idx]) ^ signBit) >> shift & mask)
			counts[d]++
		}
		if counts[int((uint64(keys[src[0]])^signBit)>>shift&mask)] == n {
			continue
		}
		var offset int
		for i := range counts {
			c := counts[i]
			counts[i] = offset
			offset += c
		}
		// Branchless 2-way scatter.
		i := 0
		for ; i+1 < len(src); i += 2 {
			idx0 := src[i]
			idx1 := src[i+1]
			d0 := int((uint64(keys[idx0]) ^ signBit) >> shift & mask)
			d1 := int((uint64(keys[idx1]) ^ signBit) >> shift & mask)
			p0 := counts[d0]
			counts[d0] = p0 + 1
			p1 := counts[d1]
			counts[d1] = p1 + 1
			dst[p0] = idx0
			dst[p1] = idx1
		}
		for ; i < len(src); i++ {
			idx := src[i]
			d := int((uint64(keys[idx]) ^ signBit) >> shift & mask)
			dst[counts[d]] = idx
			counts[d]++
		}
		src, dst = dst, src
		swaps++
	}
	if swaps%2 != 0 {
		copy(indices, aux)
	}
}

// argRadixSortInt64Parallel is the parallel variant of argRadixSortInt64.
// Reuses the 11-bit-digit workspace from regular radix (2048 buckets). Each
// thread owns a partition of `indices` through all passes; scatter targets
// a global dst computed from per-thread prefix sums.
func argRadixSortInt64Parallel(keys []int64, indices []int) {
	n := len(indices)
	k := min(runtime.GOMAXPROCS(0), 8)
	if n < parallelRadixCutoff || n < 64*k {
		return
	}
	aux := make([]int, n)
	const signBit uint64 = 1 << 63
	partSize := (n + k - 1) / k

	workspaces := make([]*radixWorkspace, k)
	for p := range k {
		workspaces[p] = radixWorkspacePool.Get().(*radixWorkspace)
	}
	defer func() {
		for _, w := range workspaces {
			radixWorkspacePool.Put(w)
		}
	}()

	src, dst := indices, aux
	swaps := 0
	var wg sync.WaitGroup

	for pass := range 6 {
		shift := uint(pass * 11)
		mask := uint64(2047)
		if pass == 5 {
			mask = 0x1ff
		}

		for p := range k {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				h := &workspaces[p].hist
				for i := range h {
					h[i] = 0
				}
				start := p * partSize
				end := min(start+partSize, n)
				s := src[start:end]
				for i := range s {
					d := int((uint64(keys[s[i]]) ^ signBit) >> shift & mask)
					h[d]++
				}
			}(p)
		}
		wg.Wait()

		nBuckets := 2048
		if pass == 5 {
			nBuckets = 512
		}
		var total int
		maxDigitCount := 0
		for d := 0; d < nBuckets; d++ {
			sum := 0
			for p := range k {
				workspaces[p].off[d] = total
				c := workspaces[p].hist[d]
				total += c
				sum += c
			}
			if sum > maxDigitCount {
				maxDigitCount = sum
			}
		}
		if maxDigitCount == n {
			continue
		}

		for p := range k {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				off := &workspaces[p].off
				start := p * partSize
				end := min(start+partSize, n)
				s := src[start:end]
				i := 0
				for ; i+1 < len(s); i += 2 {
					idx0 := s[i]
					idx1 := s[i+1]
					d0 := int((uint64(keys[idx0]) ^ signBit) >> shift & mask)
					d1 := int((uint64(keys[idx1]) ^ signBit) >> shift & mask)
					p0 := off[d0]
					off[d0] = p0 + 1
					p1 := off[d1]
					off[d1] = p1 + 1
					dst[p0] = idx0
					dst[p1] = idx1
				}
				for ; i < len(s); i++ {
					idx := s[i]
					d := int((uint64(keys[idx]) ^ signBit) >> shift & mask)
					dst[off[d]] = idx
					off[d]++
				}
			}(p)
		}
		wg.Wait()

		src, dst = dst, src
		swaps++
	}

	if swaps%2 != 0 {
		copy(indices, aux)
	}
}

// insertionSortArgInt64 is the fallback for small inputs.
func insertionSortArgInt64(keys []int64, indices []int) {
	for i := 1; i < len(indices); i++ {
		idxI := indices[i]
		keyI := keys[idxI]
		j := i - 1
		for j >= 0 && keys[indices[j]] > keyI {
			indices[j+1] = indices[j]
			j--
		}
		indices[j+1] = idxI
	}
}
