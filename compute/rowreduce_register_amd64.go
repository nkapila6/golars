//go:build amd64 && !noasm

package compute

import "golang.org/x/sys/cpu"

func init() {
	if !cpu.X86.HasAVX2 {
		return
	}
	MaxInt64PairFold = simdMaxInt64PairFoldAVX2
	MinInt64PairFold = simdMinInt64PairFoldAVX2
	MaxInt64PairFoldNT = simdMaxInt64PairFoldNTAVX2
	MinInt64PairFoldNT = simdMinInt64PairFoldNTAVX2
}
