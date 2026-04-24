package compute

import (
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/mempool"
)

// The pool itself lives in internal/mempool so sibling packages
// (series, dataframe, lazy) can share one set of buckets rather than
// each keeping its own. The tiny wrappers below preserve the existing
// in-package call sites and the exported PoolingMem surface for
// callers outside this package.

// poolingMem returns the hot pool when userMem is nil or the arrow
// default allocator. Test-use CheckedAllocator instances are passed
// through so leak detection keeps working.
func poolingMem(userMem memory.Allocator) memory.Allocator {
	return mempool.Pooling(userMem)
}

// PoolingMem is the exported form for dataframe / lazy / series
// callers.
func PoolingMem(userMem memory.Allocator) memory.Allocator {
	return mempool.Pooling(userMem)
}
