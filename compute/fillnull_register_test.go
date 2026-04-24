//go:build amd64 && !noasm

package compute_test

import (
	"testing"

	"golang.org/x/sys/cpu"

	_ "github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestFillNullAccelRegistered verifies the amd64 init hook actually
// wires the SIMD kernel when the CPU supports AVX2. Catches the
// embarrassing case where a build-tag typo leaves the kernel
// registered under the wrong condition and users silently get the
// pure-Go fallback.
func TestFillNullAccelRegistered(t *testing.T) {
	if !cpu.X86.HasAVX2 {
		t.Skip("AVX2 not supported on this host; no kernel expected")
	}
	if series.FillNullInt64Accel == nil {
		t.Fatalf("FillNullInt64Accel nil on AVX2-capable host")
	}
	if series.FillNullFloat64Accel == nil {
		t.Fatalf("FillNullFloat64Accel nil on AVX2-capable host")
	}
}
