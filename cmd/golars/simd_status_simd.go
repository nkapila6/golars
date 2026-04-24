//go:build goexperiment.simd

package main

// simdBuildStatus reports whether the binary was compiled with
// GOEXPERIMENT=simd.
func simdBuildStatus() string { return "enabled (GOEXPERIMENT=simd)" }
