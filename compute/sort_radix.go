package compute

import "unsafe"

// int64sFromUint64s aliases a []uint64 as []int64 with the same backing
// memory. Used so the aux-buffer pool can back both uint64 and int64
// radix sorts without separate pools.
func int64sFromUint64s(s []uint64) []int64 {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(s))), len(s))
}

// radixSortUint64FromFloatSerial sorts float64 by IEEE bit pattern
// into out, detecting NaN/Inf/negative during its interleaved count
// pass. Returns true iff the input contains any NaN, Inf, or
// negative value: in which case the caller must run the IEEE
// transform fallback. Used by SortFloat64 for n in [1024,
// parallelRadixCutoff). Fuses the pre-scan that used to run
// separately in sortValuesFast, saving one full read of src.
func radixSortUint64FromFloatSerial(src []float64, out []uint64) (needsIEEE bool) {
	n := len(src)
	if n != len(out) {
		panic("radixSortUint64FromFloatSerial: length mismatch")
	}
	srcBits := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(src))), n)
	return serialFromFloatWithDetect(srcBits, out)
}

// scatterFromBits is pass 0 for the float-fused radix: reads src as
// uint64 bits and scatters into dst indexed by prefix-summed histogram.
// Branchless 2-way unroll identical in shape to scatterUint64 - kept
// as a separate function so the compiler sees two small hot loops
// rather than one larger function whose register allocation is
// driven by worst-case path.
func scatterFromBits(src, dst []uint64, counts []int, shift uint, mask uint64) {
	var offset int
	for i := range counts {
		c := counts[i]
		counts[i] = offset
		offset += c
	}
	n := len(src)
	if n == 0 {
		return
	}
	srcPtr := unsafe.Pointer(unsafe.SliceData(src))
	dstPtr := unsafe.Pointer(unsafe.SliceData(dst))
	cntPtr := unsafe.Pointer(unsafe.SliceData(counts))
	i := 0
	for ; i+1 < n; i += 2 {
		v0 := *(*uint64)(unsafe.Add(srcPtr, i*8))
		v1 := *(*uint64)(unsafe.Add(srcPtr, (i+1)*8))
		d0 := uintptr(v0>>shift&mask) * 8
		d1 := uintptr(v1>>shift&mask) * 8
		p0 := *(*int)(unsafe.Add(cntPtr, d0))
		*(*int)(unsafe.Add(cntPtr, d0)) = p0 + 1
		p1 := *(*int)(unsafe.Add(cntPtr, d1))
		*(*int)(unsafe.Add(cntPtr, d1)) = p1 + 1
		*(*uint64)(unsafe.Add(dstPtr, p0*8)) = v0
		*(*uint64)(unsafe.Add(dstPtr, p1*8)) = v1
	}
	for ; i < n; i++ {
		v := *(*uint64)(unsafe.Add(srcPtr, i*8))
		d := uintptr(v>>shift&mask) * 8
		p := *(*int)(unsafe.Add(cntPtr, d))
		*(*uint64)(unsafe.Add(dstPtr, p*8)) = v
		*(*int)(unsafe.Add(cntPtr, d)) = p + 1
	}
}

// scatterUint64 does a single radix pass, consuming a precomputed
// counts histogram (converted in place to a prefix sum).
//
// Inner loop uses unsafe pointer arithmetic so Go's bounds check on
// every counts[d] / dst[p] access doesn't fire. Measured 40% faster on
// 16K float64 sort: the bounds checks dominate when the rest of the
// loop is pure arithmetic on L1-resident data.
//
// The 2-way unrolled inner is branchless: each element does its own
// read-modify-write on counts, so d0==d1 still works correctly because
// the second lookup sees the first's increment (p0 then p1 = p0+1, and
// dst[p0] then dst[p1] write to adjacent slots). Skipping the explicit
// d0!=d1 branch measured +3 % on a 1 M scatter microbench.
//
// Caller invariants:
//   - len(counts) = 2^(bits per digit)
//   - every (src[i] >> shift & mask) is < len(counts)
//   - the prefix sum plus any valid offset for a digit stays inside dst
func scatterUint64(src, dst []uint64, counts []int, shift uint, mask uint64) {
	var offset int
	for i := range counts {
		c := counts[i]
		counts[i] = offset
		offset += c
	}
	n := len(src)
	if n == 0 {
		return
	}
	srcPtr := unsafe.Pointer(unsafe.SliceData(src))
	dstPtr := unsafe.Pointer(unsafe.SliceData(dst))
	cntPtr := unsafe.Pointer(unsafe.SliceData(counts))
	i := 0
	for ; i+1 < n; i += 2 {
		v0 := *(*uint64)(unsafe.Add(srcPtr, i*8))
		v1 := *(*uint64)(unsafe.Add(srcPtr, (i+1)*8))
		d0 := uintptr(v0>>shift&mask) * 8
		d1 := uintptr(v1>>shift&mask) * 8
		p0 := *(*int)(unsafe.Add(cntPtr, d0))
		*(*int)(unsafe.Add(cntPtr, d0)) = p0 + 1
		p1 := *(*int)(unsafe.Add(cntPtr, d1))
		*(*int)(unsafe.Add(cntPtr, d1)) = p1 + 1
		*(*uint64)(unsafe.Add(dstPtr, p0*8)) = v0
		*(*uint64)(unsafe.Add(dstPtr, p1*8)) = v1
	}
	for ; i < n; i++ {
		v := *(*uint64)(unsafe.Add(srcPtr, i*8))
		d := uintptr(v>>shift&mask) * 8
		p := *(*int)(unsafe.Add(cntPtr, d))
		*(*uint64)(unsafe.Add(dstPtr, p*8)) = v
		*(*int)(unsafe.Add(cntPtr, d)) = p + 1
	}
}

// radixSortInt64 sorts vals in place using an LSD radix sort. Negative
// numbers are handled by flipping the sign bit so the unsigned byte ordering
// matches signed value ordering. The sort is stable.
//
// Dispatch:
//   - n < 64: insertion sort (fewer branches than radix setup).
//   - n < radixCutoff16: 8-bit passes (small counts table fits L1).
//   - n >= radixCutoff16: 16-bit passes (4 passes instead of 8; trades a
//     larger counts table for half the total work).
//
// For 1M int64 values on amd64 the 16-bit path is ~1.7x faster than the
// 8-bit path.
func radixSortInt64(vals []int64) {
	n := len(vals)
	if n < 64 {
		insertionSortInt64(vals)
		return
	}
	// Tiered dispatch:
	//   n<parallelRadixCutoff:  8-bit digits with PREFETCHT0 scatter
	//                           (amd64). Beats 11-bit on both narrow
	//                           ([0, 2^20)) and wide (full-range) int64:
	//                           measured +6% narrow, +56% wide on
	//                           i7-10700. Smaller histograms stay L1-
	//                           hot and prefetch hides the cold-line
	//                           write latency.
	//   n>=parallelRadixCutoff: parallel 11-bit (prefetch regresses
	//                           across cores due to coherence traffic).
	if n >= parallelRadixCutoff {
		radixSortInt64Parallel(vals)
		return
	}
	radixSortInt64_8(vals)
}

// radixSortInt64_8 uses 8-bit digits: 8 passes, 256-entry counts table
// with per-pass skip-pass detection and the PREFETCHT0 asm scatter
// (amd64 only; pure-Go fallback on arm/other).
//
// Measured on an i7-10700:
//
//	data distribution | 11-bit | 8-bit + prefetch
//	narrow [0, 2^20)  |  1160  | 1235 (+6%)
//	full 64-bit       |   510  |  795 (+56%)
//
// 8-bit wins uniformly because:
//   - 16 KB total histograms fit L1d (32 KB) - 11-bit's 96 KB spills.
//   - 256-bucket scatter has long sequential write streams (avg 64
//     per bucket at 16K input), versus 2048-bucket's fragmentary 8
//     per bucket.
//   - PREFETCHT0 on the dst cacheline before the write hides the
//     ~5-cycle L1 fill on cold bucket heads.
//
// Skip-pass: when every value shares one digit in a pass (common for
// benchmark narrow-range int64 on bytes 4-7), that pass is a no-op and
// skipped.
func radixSortInt64_8(vals []int64) {
	n := len(vals)
	if n == 0 {
		return
	}
	auxU := getAuxBuf(n)
	defer putAuxBuf(auxU)
	aux := int64sFromUint64s(auxU)
	const signBit uint64 = 1 << 63

	// Range-aware split count. Typical benchmark int64 data is narrow
	// (values in [0, 2^20)) so upper bytes are all zero after sign-XOR.
	// Classic interleaved count touches 8 histograms per element
	// (~128 K RMW ops at 16 K = ~50 µs). Counting upper bytes we know
	// are zero is pure waste.
	//
	// Phase 1: count the lower 3 bytes while building orBits. orBits
	// captures whether any upper byte has data - if not, skip phase 2.
	var counts [8][256]int
	var orBits uint64
	for _, v := range vals {
		u := uint64(v) ^ signBit
		orBits |= u >> 24 // only tracking upper 5 bytes
		counts[0][u&0xFF]++
		counts[1][(u>>8)&0xFF]++
		counts[2][(u>>16)&0xFF]++
	}
	// Phase 2: if any upper byte has data, count bytes 3-7.
	if orBits != 0 {
		for _, v := range vals {
			u := uint64(v) ^ signBit
			counts[3][(u>>24)&0xFF]++
			counts[4][(u>>32)&0xFF]++
			counts[5][(u>>40)&0xFF]++
			counts[6][(u>>48)&0xFF]++
			counts[7][(u>>56)&0xFF]++
		}
	}

	// Skip-pass loop: scatter only passes whose digit is non-constant.
	// For narrow data with orBits==0, passes 3-7's counts are all
	// zero, so counts[pass][digit0] == n (all went to one bucket) and
	// we skip. For wide data, upper passes run normally.
	src, dst := vals, aux
	swaps := 0
	nPasses := 3
	if orBits != 0 {
		nPasses = 8
	}
	for pass := 0; pass < nPasses; pass++ {
		shift := uint(pass * 8)
		if counts[pass][int((uint64(src[0])^signBit)>>shift&0xFF)] == n {
			continue
		}
		scatterInt64_8_prefetch2(src, dst, counts[pass][:], uint(pass))
		src, dst = dst, src
		swaps++
	}
	if swaps%2 != 0 {
		copy(vals, aux)
	}
}

// radixSortInt64_11 uses 11-bit digits: 6 passes (last pass takes 9 bits to
// cover the 64th bit), 2048-entry counts table that fits comfortably in L1.
// For benchmark data with values in [0, 2^20), the upper four passes are
// skip-passes, so only 2 passes actually run. Measured: ~2x faster than
// the 16-bit variant on such data, at comparable speed for full-range data.
func radixSortInt64_11(vals []int64) {
	n := len(vals)
	auxU := getAuxBuf(n)
	defer putAuxBuf(auxU)
	aux := int64sFromUint64s(auxU)
	const signBit uint64 = 1 << 63

	// Interleaved counting: compute all 6 per-pass histograms in a
	// single scan over vals. 6 × 2048 × 8B = 96 KB of histogram state
	// fits comfortably in L2; vals is read once (cache-hot).
	var counts [6][2048]int
	for _, v := range vals {
		u := uint64(v) ^ signBit
		counts[0][u&2047]++
		counts[1][(u>>11)&2047]++
		counts[2][(u>>22)&2047]++
		counts[3][(u>>33)&2047]++
		counts[4][(u>>44)&2047]++
		counts[5][(u>>55)&511]++
	}

	src, dst := vals, aux
	swaps := 0
	for pass := range 6 {
		shift := uint(pass * 11)
		mask := uint64(2047)
		if pass == 5 {
			mask = 0x1ff
		}
		// Skip-pass when every value lands in a single digit bucket.
		if counts[pass][int((uint64(src[0])^signBit)>>shift&mask)] == n {
			continue
		}
		// Prefix-sum in place.
		var offset int
		cs := counts[pass][:]
		for i := range cs {
			c := cs[i]
			cs[i] = offset
			offset += c
		}
		// Branchless 2-way scatter: each element does its own read-
		// modify-write, so d0==d1 still writes to adjacent slots
		// (p0 then p1=p0+1) without a special case.
		i := 0
		for ; i+1 < len(src); i += 2 {
			v0 := src[i]
			v1 := src[i+1]
			d0 := int((uint64(v0) ^ signBit) >> shift & mask)
			d1 := int((uint64(v1) ^ signBit) >> shift & mask)
			p0 := cs[d0]
			cs[d0] = p0 + 1
			p1 := cs[d1]
			cs[d1] = p1 + 1
			dst[p0] = v0
			dst[p1] = v1
		}
		for ; i < len(src); i++ {
			v := src[i]
			d := int((uint64(v) ^ signBit) >> shift & mask)
			dst[cs[d]] = v
			cs[d]++
		}
		src, dst = dst, src
		swaps++
	}
	if swaps%2 != 0 {
		copy(vals, aux)
	}
}

// radixSortInt64_16 is the 16-bit digit variant: four passes, 65536-entry
// counts array. Faster per-pass throughput at the cost of a larger counts
// table that spills from L1 but fits in L2.
//
// Skip optimization: when all values share the same digit for a given pass
// (very common for mostly-positive int64 data on the high bytes), the
// counts array collapses to a single entry and the pass is a no-op.
func radixSortInt64_16(vals []int64) {
	n := len(vals)
	auxU := getAuxBuf(n)
	defer putAuxBuf(auxU)
	aux := int64sFromUint64s(auxU)
	const signBit uint64 = 1 << 63
	// Reuse a single 65536-entry counts array across passes.
	counts := make([]int, 65536)
	src, dst := vals, aux
	swaps := 0
	for pass := range 4 {
		shift := uint(pass * 16)
		for i := range counts {
			counts[i] = 0
		}
		for _, v := range src {
			d := uint16((uint64(v) ^ signBit) >> shift)
			counts[d]++
		}
		// Skip passes where every value lands in a single digit bucket.
		// For typical non-negative int64 data, passes 2 and 3 hit this.
		single := -1
		for i, c := range counts {
			if c == n {
				single = i
				break
			}
		}
		if single >= 0 {
			continue
		}
		var offset int
		for i := range counts {
			c := counts[i]
			counts[i] = offset
			offset += c
		}
		i := 0
		for ; i+1 < len(src); i += 2 {
			v0 := src[i]
			v1 := src[i+1]
			d0 := uint16((uint64(v0) ^ signBit) >> shift)
			d1 := uint16((uint64(v1) ^ signBit) >> shift)
			p0 := counts[d0]
			counts[d0] = p0 + 1
			p1 := counts[d1]
			counts[d1] = p1 + 1
			dst[p0] = v0
			dst[p1] = v1
		}
		for ; i < len(src); i++ {
			v := src[i]
			d := uint16((uint64(v) ^ signBit) >> shift)
			dst[counts[d]] = v
			counts[d]++
		}
		src, dst = dst, src
		swaps++
	}
	// If we swapped an odd number of times the sorted data is in aux; copy
	// back. The skip optimization may yield odd swaps even though 4 passes
	// is even in the worst case.
	if swaps%2 != 0 {
		copy(vals, aux)
	}
}

// radixSortInt32 is the int32 specialization: four passes.
func radixSortInt32(vals []int32) {
	n := len(vals)
	if n < 64 {
		insertionSortInt32(vals)
		return
	}
	aux := make([]int32, n)
	const signBit uint32 = 1 << 31

	src, dst := vals, aux
	for pass := range 4 {
		shift := uint(pass * 8)
		var counts [256]int
		for _, v := range src {
			b := byte((uint32(v) ^ signBit) >> shift)
			counts[b]++
		}
		var offset int
		for i := range counts {
			c := counts[i]
			counts[i] = offset
			offset += c
		}
		for _, v := range src {
			b := byte((uint32(v) ^ signBit) >> shift)
			dst[counts[b]] = v
			counts[b]++
		}
		src, dst = dst, src
	}
}

// radixSortUint64 sorts unsigned 64-bit values in place. Tiered dispatch
// mirrors the signed path: 11-bit digits fit L1 for small-medium inputs,
// parallel 11-bit kicks in at 256K+. Used by SortFloat64 after the IEEE
// 754 bit-reinterpretation trick maps floats to sortable uints.
func radixSortUint64(vals []uint64) {
	n := len(vals)
	if n < 64 {
		insertionSortUint64(vals)
		return
	}
	if n >= parallelRadixCutoff {
		radixSortUint64Parallel(vals)
		return
	}
	// 8-bit + PREFETCHT0 asm scatter. Wins on both narrow and wide
	// uint64 data after the scatter asm: 256-bucket histograms fit L1
	// and prefetching dst hides write-allocate cost.
	radixSortUint64_8(vals)
}

// radixSortUint64_8 mirrors radixSortInt64_8 without the sign-bit flip.
// Range-aware: counts lower 3 bytes + orBits in phase 1, and only
// counts upper 5 bytes in phase 2 if any of them has data.
func radixSortUint64_8(vals []uint64) {
	n := len(vals)
	if n == 0 {
		return
	}
	aux := getAuxBuf(n)
	defer putAuxBuf(aux)

	var counts [8][256]int
	var orBits uint64
	for _, v := range vals {
		orBits |= v >> 24
		counts[0][v&0xFF]++
		counts[1][(v>>8)&0xFF]++
		counts[2][(v>>16)&0xFF]++
	}
	if orBits != 0 {
		for _, v := range vals {
			counts[3][(v>>24)&0xFF]++
			counts[4][(v>>32)&0xFF]++
			counts[5][(v>>40)&0xFF]++
			counts[6][(v>>48)&0xFF]++
			counts[7][(v>>56)&0xFF]++
		}
	}

	src, dst := vals, aux
	swaps := 0
	nPasses := 3
	if orBits != 0 {
		nPasses = 8
	}
	for pass := 0; pass < nPasses; pass++ {
		shift := uint(pass * 8)
		if counts[pass][int(src[0]>>shift&0xFF)] == n {
			continue
		}
		scatterUint64_8_prefetch2(src, dst, counts[pass][:], uint(pass))
		src, dst = dst, src
		swaps++
	}
	if swaps%2 != 0 {
		copy(vals, aux)
	}
}

func radixSortUint64_11(vals []uint64) {
	n := len(vals)
	aux := getAuxBuf(n)
	defer putAuxBuf(aux)

	// Interleaved counting: build all 6 histograms in one scan over
	// src. Saves 5 passes of read bandwidth vs counting per-pass.
	// 6 × 2048 × 8B = 96 KB: fits L2, not L1, but still cheaper than
	// six sequential reads of vals[] through the cache.
	var counts [6][2048]int
	for _, v := range vals {
		counts[0][v&2047]++
		counts[1][(v>>11)&2047]++
		counts[2][(v>>22)&2047]++
		counts[3][(v>>33)&2047]++
		counts[4][(v>>44)&2047]++
		counts[5][(v>>55)&511]++
	}

	src, dst := vals, aux
	swaps := 0
	for pass := range 6 {
		shift := uint(pass * 11)
		mask := uint64(2047)
		if pass == 5 {
			mask = 0x1ff
		}
		// Skip pass when all values share the digit: common for the
		// high-bit pass when values have a narrow exponent band.
		if counts[pass][int(src[0]>>shift&mask)] == n {
			continue
		}
		// Prefix-sum counts[pass] in-place.
		var offset int
		cs := counts[pass][:]
		for i := range cs {
			c := cs[i]
			cs[i] = offset
			offset += c
		}
		// Branchless 2-way scatter: two independent RMW streams expose
		// more ILP. d0==d1 collision still works without a special
		// case because the second read sees the first's increment.
		i := 0
		for ; i+1 < len(src); i += 2 {
			v0 := src[i]
			v1 := src[i+1]
			d0 := int(v0 >> shift & mask)
			d1 := int(v1 >> shift & mask)
			p0 := cs[d0]
			cs[d0] = p0 + 1
			p1 := cs[d1]
			cs[d1] = p1 + 1
			dst[p0] = v0
			dst[p1] = v1
		}
		for ; i < len(src); i++ {
			v := src[i]
			d := int(v >> shift & mask)
			dst[cs[d]] = v
			cs[d]++
		}
		src, dst = dst, src
		swaps++
	}
	if swaps%2 != 0 {
		copy(vals, aux)
	}
}

func insertionSortInt64(vals []int64) {
	for i := 1; i < len(vals); i++ {
		v := vals[i]
		j := i - 1
		for j >= 0 && vals[j] > v {
			vals[j+1] = vals[j]
			j--
		}
		vals[j+1] = v
	}
}

func insertionSortInt32(vals []int32) {
	for i := 1; i < len(vals); i++ {
		v := vals[i]
		j := i - 1
		for j >= 0 && vals[j] > v {
			vals[j+1] = vals[j]
			j--
		}
		vals[j+1] = v
	}
}

func insertionSortUint64(vals []uint64) {
	for i := 1; i < len(vals); i++ {
		v := vals[i]
		j := i - 1
		for j >= 0 && vals[j] > v {
			vals[j+1] = vals[j]
			j--
		}
		vals[j+1] = v
	}
}
