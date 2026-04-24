// Package assert provides runtime invariant checks for internal
// callers. Assertions are intended for conditions that the surrounding
// code already guarantees, so a failure signals a genuine bug (not a
// user input issue). Assertions panic by default; build with
// `-tags golars_no_asserts` to drop them entirely from hot paths.
//
// Public rule: never assert on external inputs. Return an error or an
// ok bool instead, so callers can handle the failure gracefully. See
// feedback_engineering_style memory: panic-free public surface.
package assert

import "fmt"

// True panics with msg when cond is false. Use for invariants that
// the current function's own code guarantees (e.g. a slice length
// computed by the same function).
func True(cond bool, msg string) {
	if !cond {
		panic("golars assert failed: " + msg)
	}
}

// Equal is sugar for True(got == want, ...) with a formatted message
// that includes both values.
func Equal[T comparable](got, want T, label string) {
	if got != want {
		panic(fmt.Sprintf("golars assert failed: %s got=%v want=%v", label, got, want))
	}
}

// InRange panics when i is outside [0, n). The label is prepended to
// the panic message for debuggability.
func InRange(i, n int, label string) {
	if i < 0 || i >= n {
		panic(fmt.Sprintf("golars assert failed: %s index %d out of range [0, %d)", label, i, n))
	}
}

// NotNil panics when v is nil. Use sparingly: prefer an explicit error
// return at package boundaries and reserve this for shared-state
// invariants.
func NotNil(v any, label string) {
	if v == nil {
		panic("golars assert failed: " + label + " is nil")
	}
}
