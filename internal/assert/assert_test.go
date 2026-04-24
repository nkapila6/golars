package assert_test

import (
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/internal/assert"
)

func TestTruePanicsOnFalse(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic")
		}
		if !strings.Contains(r.(string), "golars assert failed: invariant") {
			t.Errorf("panic message = %v", r)
		}
	}()
	assert.True(false, "invariant")
}

func TestTruePassesOnTrue(t *testing.T) {
	// Must not panic.
	assert.True(true, "identity")
}

func TestEqual(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on inequality")
		}
	}()
	assert.Equal(1, 2, "n")
}

func TestInRange(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on out-of-range")
		}
	}()
	assert.InRange(10, 5, "idx")
}

func TestInRangeInBounds(t *testing.T) {
	assert.InRange(0, 1, "idx")
	assert.InRange(4, 5, "idx")
}

func TestNotNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil")
		}
	}()
	assert.NotNil(nil, "x")
}
