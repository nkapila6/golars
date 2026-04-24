// Stats: skew, kurtosis, corr, cov, approx_n_unique.
// Run: go run ./examples/stats
package main

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	x, _ := series.FromFloat64("x", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
	y, _ := series.FromFloat64("y", []float64{2, 4, 6, 8, 10, 12, 14, 16, 18}, nil)
	z, _ := series.FromFloat64("z", []float64{9, 8, 7, 6, 5, 4, 3, 2, 1}, nil)
	df, _ := dataframe.New(x, y, z)
	defer df.Release()

	sk, _ := x.Skew()
	kurt, _ := x.Kurtosis()
	xy, _ := x.PearsonCorr(y)
	xz, _ := x.PearsonCorr(z)
	cov, _ := x.Covariance(y, 1)
	approx, _ := x.ApproxNUnique()
	fmt.Printf("x: skew=%.3f  kurtosis=%.3f  approx_n_unique=%d\n", sk, kurt, approx)
	fmt.Printf("corr(x,y)=%.3f (perfect positive)\n", xy)
	fmt.Printf("corr(x,z)=%.3f (perfect negative)\n", xz)
	fmt.Printf("cov(x,y)=%.3f\n\n", cov)

	corrMat, _ := df.Corr(ctx)
	defer corrMat.Release()
	fmt.Println("correlation matrix:")
	fmt.Println(corrMat)
}
