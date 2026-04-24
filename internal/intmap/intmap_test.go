package intmap_test

import (
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/internal/intmap"
)

func TestInsertOrGetBasic(t *testing.T) {
	t.Parallel()
	m := intmap.New(16)

	id, inserted := m.InsertOrGet(42, 0)
	if !inserted || id != 0 {
		t.Errorf("first insert = (%d, %v), want (0, true)", id, inserted)
	}
	id, inserted = m.InsertOrGet(42, 99)
	if inserted || id != 0 {
		t.Errorf("second insert = (%d, %v), want (0, false)", id, inserted)
	}
	id, inserted = m.InsertOrGet(100, 1)
	if !inserted || id != 1 {
		t.Errorf("new key = (%d, %v), want (1, true)", id, inserted)
	}
}

func TestGetMissing(t *testing.T) {
	t.Parallel()
	m := intmap.New(4)
	m.InsertOrGet(1, 0)
	m.InsertOrGet(2, 1)

	if _, found := m.Get(1); !found {
		t.Error("Get(1) should find it")
	}
	if _, found := m.Get(42); found {
		t.Error("Get(42) should miss")
	}
}

func TestGrow(t *testing.T) {
	t.Parallel()
	m := intmap.New(4)
	for i := range int32(1000) {
		id, inserted := m.InsertOrGet(int64(i*17+3), i)
		if !inserted || id != i {
			t.Fatalf("insert %d: id=%d inserted=%v", i, id, inserted)
		}
	}
	if m.Len() != 1000 {
		t.Errorf("Len = %d, want 1000", m.Len())
	}
	for i := range int32(1000) {
		id, found := m.Get(int64(i*17 + 3))
		if !found || id != i {
			t.Errorf("Get %d: id=%d found=%v", i, id, found)
		}
	}
}

func TestNegativeKeys(t *testing.T) {
	t.Parallel()
	m := intmap.New(8)
	for _, k := range []int64{-1, -2, -1 << 30, -(1 << 62), 0, 1, 1 << 62} {
		m.InsertOrGet(k, 0)
	}
	for _, k := range []int64{-1, -2, -1 << 30, -(1 << 62), 0, 1, 1 << 62} {
		if _, found := m.Get(k); !found {
			t.Errorf("Get(%d) missing", k)
		}
	}
}

func TestCollisionsUnderFibonacciHash(t *testing.T) {
	t.Parallel()
	// Sequential keys should distribute well under Fibonacci hashing.
	m := intmap.New(16)
	for i := range int32(10_000) {
		m.InsertOrGet(int64(i), i)
	}
	for i := range int32(10_000) {
		v, found := m.Get(int64(i))
		if !found || v != i {
			t.Errorf("Get(%d) = %d, %v", i, v, found)
			return
		}
	}
}

func TestRandomizedStress(t *testing.T) {
	t.Parallel()
	m := intmap.New(64)
	ref := make(map[int64]int32)
	r := rand.New(rand.NewPCG(1, 2))
	for range 50_000 {
		k := r.Int64N(1 << 40)
		if _, found := ref[k]; !found {
			nextID := int32(len(ref))
			v, inserted := m.InsertOrGet(k, nextID)
			if !inserted || v != nextID {
				t.Fatalf("k=%d: InsertOrGet(%d) = (%d, %v), want (%d, true)",
					k, nextID, v, inserted, nextID)
			}
			ref[k] = nextID
		} else {
			v, found := m.Get(k)
			if !found || v != ref[k] {
				t.Fatalf("k=%d: Get = (%d, %v), want (%d, true)", k, v, found, ref[k])
			}
		}
	}
	if int(m.Len()) != len(ref) {
		t.Errorf("Len = %d, want %d", m.Len(), len(ref))
	}
}

func BenchmarkIntMapInsert(b *testing.B) {
	const n = 16384
	keys := make([]int64, n)
	for i := range keys {
		keys[i] = int64(i * 17)
	}
	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	for b.Loop() {
		m := intmap.New(n)
		for i, k := range keys {
			m.InsertOrGet(k, int32(i))
		}
	}
}

func BenchmarkGoMapInsert(b *testing.B) {
	const n = 16384
	keys := make([]int64, n)
	for i := range keys {
		keys[i] = int64(i * 17)
	}
	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	for b.Loop() {
		m := make(map[int64]int32, n)
		for i, k := range keys {
			if _, ok := m[k]; !ok {
				m[k] = int32(i)
			}
		}
	}
}
