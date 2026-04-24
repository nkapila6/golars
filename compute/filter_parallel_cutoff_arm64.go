//go:build arm64

package compute

// parallelFilterCutoff: Mac profiling showed that raising this above
// 256K actually regressed DropNulls 262K because GC pressure from the
// output-buffer allocator started to dominate the serial timing. Keep
// the amd64-compatible 256K threshold; the real win for Mac will come
// from a NEON scatter kernel (see fused_filter_scatter_arm64.s).
const parallelFilterCutoff = 256 * 1024
