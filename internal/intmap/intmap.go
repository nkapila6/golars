// Package intmap provides a fast, purpose-built open-addressing hash map
// keyed on int64 with int32 values. It exists because Go's built-in
// map[int64]int32 spends ~30% of its time in rehash/split on the join path
// (confirmed via pprof). This implementation fits into that niche:
//
//   - Power-of-two slot count for mask-based index wrapping.
//   - Linear probing (cache-friendly; O(1) expected with load factor < 0.5).
//   - Fibonacci multiplicative hashing (x * golden64): good mixing, cheap.
//   - Generation-tagged slots so Reset/Release is O(1) instead of O(n
//     memset). Each slot holds the map generation at which it was
//     written; a slot whose generation differs from the map's current
//     generation is treated as empty. Release() increments the
//     generation (no memset) and returns the map to a sync.Pool.
//     Profiled at 12% of Unique total runtime before this optimisation.
//   - Insertion-ordered ID assignment for use as a group/row-index table.
//
// API is intentionally narrow: InsertOrGet and Get. The rest of Go's map
// surface is not needed here.
package intmap

import "sync"

// intMapPool recycles *Int64 instances across calls. Combined with the
// generation-tag reuse scheme, pooling lets parallel groupby / unique /
// join paths cycle hash-table memory with zero memset cost per call.
var intMapPool sync.Pool

// GoldenRatio64 is the Fibonacci hash multiplier, exposed for callers
// that need to probe the hash table via RawSlots with inlined loops.
const GoldenRatio64 uint64 = goldenRatio64

// goldenRatio64 is the reciprocal of the golden ratio scaled to 2^64. Used
// for multiplicative hashing; mixes low bits of the input well so linear
// probing does not cluster on sequential keys.
const goldenRatio64 uint64 = 0x9E3779B97F4A7C15

// Slot is the public wire shape of an intmap entry. Two-field struct so
// callers can inline lookup loops without going through the Get method.
// The Gen field holds the map generation at which this slot was last
// written - a slot is empty iff Gen != m.CurrentGen(). Inlined probe
// callers must filter on Gen, not on Value.
type Slot struct {
	Key   int64
	Value int32
	Gen   int32
}

type slot = Slot

// Int64 is an int64 → int32 map. The zero value is not ready; use New.
type Int64 struct {
	slots   []slot
	mask    uint64
	size    int32
	maxFill int32
	gen     int32 // current generation; slot "empty" iff slot.Gen != gen
}

// New returns a map sized to hold hint entries without growing. Final load
// factor target is ~70%.
//
// Returns from a shared sync.Pool when possible. On a pool hit the
// generation is bumped so every existing slot looks empty without any
// memset - this is the key win: previously allocate() spent ~10% of
// total runtime memsetting 256 KB+ slot arrays on every bench iter.
func New(hint int) *Int64 {
	minN := max((hint*3+1)/2, 16) // ceil(1.5 * hint)
	n := 16
	for n < minN {
		n <<= 1
	}
	if v := intMapPool.Get(); v != nil {
		m := v.(*Int64)
		if cap(m.slots) >= n {
			m.slots = m.slots[:n]
			m.mask = uint64(n - 1)
			m.maxFill = int32(n * 3 / 4)
			m.size = 0
			m.bumpGen()
			return m
		}
		// Pooled map was too small; fall through and allocate fresh.
		intMapPool.Put(m)
	}
	m := &Int64{gen: 1}
	m.allocate(n)
	return m
}

// bumpGen increments the generation, wrapping safely: when int32
// overflow threatens, zero every slot so the old gen==0 state is
// unambiguous. Wrap happens every 2^31 Reset calls - practically never
// in one process lifetime, but the check keeps the invariant correct.
func (m *Int64) bumpGen() {
	if m.gen == maxGen {
		clear(m.slots)
		m.gen = 1
		return
	}
	m.gen++
}

const maxGen int32 = 0x7FFFFFFF

func (m *Int64) allocate(n int) {
	m.slots = make([]slot, n)
	m.mask = uint64(n - 1)
	m.maxFill = int32(n * 3 / 4)
}

// Release drops the backing storage into the shared pool. Safe to call
// in defer. The map must not be used after Release.
func (m *Int64) Release() {
	if cap(m.slots) == 0 {
		return
	}
	intMapPool.Put(m)
}

// Reset clears the map in place via a generation bump. O(1) - slots
// are not memset. Subsequent operations see every slot as empty.
func (m *Int64) Reset() {
	m.size = 0
	m.bumpGen()
}

// Len returns the number of entries.
func (m *Int64) Len() int32 { return m.size }

// CurrentGen returns the generation callers should compare against the
// Slot.Gen field when inlining probe loops over RawSlots.
func (m *Int64) CurrentGen() int32 { return m.gen }

// RawSlots exposes the backing slot slice and mask for callers that want
// to inline probe loops rather than call Get. The returned slice has
// len=nextPow2; mask = len-1. Slots where Gen != m.CurrentGen() are empty.
func (m *Int64) RawSlots() ([]Slot, uint64) { return m.slots, m.mask }

// InsertOrGet returns the existing value for key k, or, if absent, inserts
// nextID and returns (nextID, true). The inserted bool tells the caller
// whether they should persist side effects (e.g. append to a key slice).
func (m *Int64) InsertOrGet(k int64, nextID int32) (int32, bool) {
	if m.size+1 >= m.maxFill {
		m.grow()
	}
	gen := m.gen
	h := uint64(k) * goldenRatio64
	idx := h & m.mask
	for {
		s := &m.slots[idx]
		if s.Gen != gen {
			s.Key = k
			s.Value = nextID
			s.Gen = gen
			m.size++
			return nextID, true
		}
		if s.Key == k {
			return s.Value, false
		}
		idx = (idx + 1) & m.mask
	}
}

// Overwrite sets the value for an already-inserted key. Caller must have
// previously InsertOrGet'd the key; no-op if absent.
func (m *Int64) Overwrite(k int64, v int32) {
	gen := m.gen
	h := uint64(k) * goldenRatio64
	idx := h & m.mask
	for {
		s := &m.slots[idx]
		if s.Gen != gen {
			return
		}
		if s.Key == k {
			s.Value = v
			return
		}
		idx = (idx + 1) & m.mask
	}
}

// Get returns (value, found). Missing key returns (0, false).
func (m *Int64) Get(k int64) (int32, bool) {
	gen := m.gen
	h := uint64(k) * goldenRatio64
	idx := h & m.mask
	for {
		s := &m.slots[idx]
		if s.Gen != gen {
			return 0, false
		}
		if s.Key == k {
			return s.Value, true
		}
		idx = (idx + 1) & m.mask
	}
}

func (m *Int64) grow() {
	old := m.slots
	oldGen := m.gen
	m.allocate(len(old) * 2)
	m.gen = 1
	for i := range old {
		s := &old[i]
		if s.Gen != oldGen {
			continue
		}
		h := uint64(s.Key) * goldenRatio64
		idx := h & m.mask
		for m.slots[idx].Gen == m.gen {
			idx = (idx + 1) & m.mask
		}
		m.slots[idx] = slot{Key: s.Key, Value: s.Value, Gen: m.gen}
	}
}

// EmptyValue is retained for backward compatibility with callers that
// may still reference it. New code should filter on Gen via RawSlots.
const EmptyValue int32 = 0
