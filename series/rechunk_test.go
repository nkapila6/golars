package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestRechunkMergesChunks(t *testing.T) {
	mem := memory.DefaultAllocator
	b1 := array.NewInt64Builder(mem)
	b1.AppendValues([]int64{1, 2, 3}, nil)
	a1 := b1.NewArray()
	b1.Release()
	b2 := array.NewInt64Builder(mem)
	b2.AppendValues([]int64{4, 5}, nil)
	a2 := b2.NewArray()
	b2.Release()

	// FromChunked retains; caller can release originals.
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{a1, a2})
	a1.Release()
	a2.Release()
	s := series.FromChunked("x", chunked)
	chunked.Release()
	defer s.Release()

	if s.NumChunks() != 2 {
		t.Fatalf("setup: want 2 chunks, got %d", s.NumChunks())
	}

	r := s.Rechunk()
	defer r.Release()
	if r.NumChunks() != 1 {
		t.Fatalf("Rechunk: want 1 chunk, got %d", r.NumChunks())
	}
	if r.Len() != 5 {
		t.Errorf("Rechunk: len=%d want 5", r.Len())
	}
}

func TestRechunkSingleChunkNoop(t *testing.T) {
	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil)
	defer s.Release()
	r := s.Rechunk()
	defer r.Release()
	if r.NumChunks() != 1 {
		t.Errorf("single-chunk source should stay single-chunk")
	}
	if r.Len() != 3 {
		t.Errorf("len=%d want 3", r.Len())
	}
}
