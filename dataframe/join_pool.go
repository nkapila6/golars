package dataframe

import "sync"

// Workspace pools for the hash-join inner loop. The table and next
// arrays are short-lived buffers whose size depends on input shape;
// sync.Pool lets repeated joins (streaming pipelines, benchmarks)
// recycle the same memory instead of churning the mallocgc heap.
//
// Entries are indexed by capacity to avoid returning a tiny pooled
// array to a caller asking for a big one.
var (
	joinTablePool sync.Pool // returns *[]int32 with cap >= requested
	joinNextPool  sync.Pool
)

func getJoinTable(span int) []int32 {
	if v := joinTablePool.Get(); v != nil {
		s := *v.(*[]int32)
		if cap(s) >= span {
			return s[:span]
		}
		joinTablePool.Put(v)
	}
	return make([]int32, span)
}

func putJoinTable(s []int32) {
	if cap(s) == 0 {
		return
	}
	s = s[:0]
	joinTablePool.Put(&s)
}

func getJoinNext(n int) []int32 {
	if v := joinNextPool.Get(); v != nil {
		s := *v.(*[]int32)
		if cap(s) >= n {
			return s[:n]
		}
		joinNextPool.Put(v)
	}
	return make([]int32, n)
}

func putJoinNext(s []int32) {
	if cap(s) == 0 {
		return
	}
	s = s[:0]
	joinNextPool.Put(&s)
}
