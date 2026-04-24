//go:build !goexperiment.simd

package main

// simdBuildStatus reports whether the binary was compiled with
// GOEXPERIMENT=simd. Useful for `golars doctor` diagnostics.
func simdBuildStatus() string { return "disabled (build with GOEXPERIMENT=simd for AVX2/AVX-512)" }
