package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// EWMMean returns the adjusted exponentially-weighted moving mean
// with smoothing parameter alpha (0 < alpha <= 1). The recursion
// matches polars' default `ewm_mean(alpha=alpha, adjust=True)`:
//
//	y[t] = sum_{i=0..t} w_i * x_i   /   sum_{i=0..t} w_i
//	w_i  = (1 - alpha)^(t - i)
//
// Null values do not contribute; their output slot is null.
// Supported dtypes: Int64, Int32, Float64, Float32. Integer inputs
// are promoted to float64.
func (s *Series) EWMMean(alpha float64) (*Series, error) {
	return ewmScalar(s, "ewm_mean", alpha, ewmMeanKernel)
}

// EWMVar returns the adjusted exponentially-weighted moving (sample)
// variance using West's online recursion.
func (s *Series) EWMVar(alpha float64) (*Series, error) {
	return ewmScalar(s, "ewm_var", alpha, ewmVarKernel)
}

// EWMStd returns sqrt of EWMVar.
func (s *Series) EWMStd(alpha float64) (*Series, error) {
	return ewmScalar(s, "ewm_std", alpha, ewmStdKernel)
}

type ewmKernel func(alpha float64, vals []float64, valid []bool, out []float64, outValid []bool)

func ewmScalar(s *Series, op string, alpha float64, fn ewmKernel) (*Series, error) {
	if !(alpha > 0 && alpha <= 1) {
		return nil, fmt.Errorf("%s: alpha=%v not in (0, 1]", op, alpha)
	}
	vals, valid, err := ewmFloat64s(s, op)
	if err != nil {
		return nil, err
	}
	n := len(vals)
	out := make([]float64, n)
	outValid := make([]bool, n)
	fn(alpha, vals, valid, out, outValid)

	mem := memory.DefaultAllocator
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.AppendValues(out, outValid)
	return New(s.Name(), b.NewArray())
}

// ewmFloat64s extracts values and validity as a float64 pair. Used
// as the common prep so the EWM kernels live in plain float64 math.
func ewmFloat64s(s *Series, op string) ([]float64, []bool, error) {
	r := s.Rechunk()
	defer r.Release()
	arr := r.Chunk(0)
	n := arr.Len()
	out := make([]float64, n)
	valid := make([]bool, n)
	switch a := arr.(type) {
	case *array.Float64:
		values := a.Float64Values()
		for i := 0; i < n; i++ {
			valid[i] = !a.IsNull(i)
			if valid[i] {
				out[i] = values[i]
			}
		}
	case *array.Float32:
		for i := 0; i < n; i++ {
			valid[i] = !a.IsNull(i)
			if valid[i] {
				out[i] = float64(a.Value(i))
			}
		}
	case *array.Int64:
		values := a.Int64Values()
		for i := 0; i < n; i++ {
			valid[i] = !a.IsNull(i)
			if valid[i] {
				out[i] = float64(values[i])
			}
		}
	case *array.Int32:
		for i := 0; i < n; i++ {
			valid[i] = !a.IsNull(i)
			if valid[i] {
				out[i] = float64(a.Value(i))
			}
		}
	default:
		return nil, nil, fmt.Errorf("%s: unsupported dtype %s", op, s.DType())
	}
	return out, valid, nil
}

// ewmMeanKernel runs the adjusted weighted-mean recursion with
// null-skipping. Running numerator and denominator decay by (1-alpha)
// per step, and a new observation contributes weight 1.
func ewmMeanKernel(alpha float64, vals []float64, valid []bool, out []float64, outValid []bool) {
	decay := 1 - alpha
	var sNum, sDen float64
	for i := range vals {
		if !valid[i] {
			outValid[i] = false
			continue
		}
		sNum = sNum*decay + vals[i]
		sDen = sDen*decay + 1
		out[i] = sNum / sDen
		outValid[i] = true
	}
}

// ewmVarKernel runs a weighted West-style online recursion. Prior
// accumulators decay by (1-alpha) per observation; the output at
// each step is the unbiased weighted sample variance:
//
//	var = M2 / (W - sumSquaredWeights / W)
func ewmVarKernel(alpha float64, vals []float64, valid []bool, out []float64, outValid []bool) {
	decay := 1 - alpha
	var sDen, mean, m2, sumSqWeights float64
	for i := range vals {
		if !valid[i] {
			outValid[i] = false
			continue
		}
		sDen *= decay
		m2 *= decay
		sumSqWeights *= decay * decay
		const w = 1.0
		newDen := sDen + w
		delta := vals[i] - mean
		newMean := mean + (w/newDen)*delta
		m2 += w * delta * (vals[i] - newMean)
		sumSqWeights += w * w
		sDen = newDen
		mean = newMean
		bias := sDen - sumSqWeights/sDen
		if bias <= 0 {
			out[i] = 0
		} else {
			out[i] = m2 / bias
		}
		outValid[i] = true
	}
}

// ewmStdKernel = sqrt(ewmVarKernel).
func ewmStdKernel(alpha float64, vals []float64, valid []bool, out []float64, outValid []bool) {
	ewmVarKernel(alpha, vals, valid, out, outValid)
	for i := range out {
		if outValid[i] {
			out[i] = math.Sqrt(out[i])
		}
	}
}
