package compute_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

func benchSize(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMi", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKi", n>>10)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// int64Values reads the values of a single-chunk int64 Series. If any value is
// null, the corresponding slot holds the underlying buffer value (usually
// zero). Use the valid slice to distinguish.
func int64Values(s *series.Series) []int64 {
	if s.NumChunks() == 0 {
		return nil
	}
	return s.Chunk(0).(*array.Int64).Int64Values()
}

func float64Values(s *series.Series) []float64 {
	if s.NumChunks() == 0 {
		return nil
	}
	return s.Chunk(0).(*array.Float64).Float64Values()
}

func boolValuesAt(s *series.Series) []bool {
	arr := s.Chunk(0).(*array.Boolean)
	out := make([]bool, arr.Len())
	for i := range out {
		out[i] = arr.Value(i)
	}
	return out
}

func stringValuesAt(s *series.Series) []string {
	arr := s.Chunk(0).(*array.String)
	out := make([]string, arr.Len())
	for i := range out {
		out[i] = arr.Value(i)
	}
	return out
}

func validityBools(s *series.Series) []bool {
	if s.NumChunks() == 0 {
		return nil
	}
	arr := s.Chunk(0)
	out := make([]bool, arr.Len())
	for i := range out {
		out[i] = arr.IsValid(i)
	}
	return out
}

// assertInt64Values checks values and optional validity. If wantValid is nil,
// every slot must be valid.
func assertInt64Values(t *testing.T, got *series.Series, wantVals []int64, wantValid []bool) {
	t.Helper()
	if got.Len() != len(wantVals) {
		t.Errorf("Len = %d, want %d", got.Len(), len(wantVals))
		return
	}
	vs := int64Values(got)
	vv := validityBools(got)
	for i := range wantVals {
		valid := true
		if wantValid != nil {
			valid = wantValid[i]
		}
		if valid != vv[i] {
			t.Errorf("[%d] valid = %v, want %v", i, vv[i], valid)
			continue
		}
		if valid && vs[i] != wantVals[i] {
			t.Errorf("[%d] value = %d, want %d", i, vs[i], wantVals[i])
		}
	}
}

func assertFloat64Values(t *testing.T, got *series.Series, wantVals []float64, wantValid []bool) {
	t.Helper()
	if got.Len() != len(wantVals) {
		t.Errorf("Len = %d, want %d", got.Len(), len(wantVals))
		return
	}
	vs := float64Values(got)
	vv := validityBools(got)
	for i := range wantVals {
		valid := true
		if wantValid != nil {
			valid = wantValid[i]
		}
		if valid != vv[i] {
			t.Errorf("[%d] valid = %v, want %v", i, vv[i], valid)
			continue
		}
		if valid && vs[i] != wantVals[i] {
			t.Errorf("[%d] value = %v, want %v", i, vs[i], wantVals[i])
		}
	}
}

func assertBoolValues(t *testing.T, got *series.Series, wantVals []bool, wantValid []bool) {
	t.Helper()
	if got.Len() != len(wantVals) {
		t.Errorf("Len = %d, want %d", got.Len(), len(wantVals))
		return
	}
	vs := boolValuesAt(got)
	vv := validityBools(got)
	for i := range wantVals {
		valid := true
		if wantValid != nil {
			valid = wantValid[i]
		}
		if valid != vv[i] {
			t.Errorf("[%d] valid = %v, want %v", i, vv[i], valid)
			continue
		}
		if valid && vs[i] != wantVals[i] {
			t.Errorf("[%d] value = %v, want %v", i, vs[i], wantVals[i])
		}
	}
}
