package pool_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Gaurav-Gosain/golars/internal/pool"
)

func TestParallelForCoversRange(t *testing.T) {
	t.Parallel()
	const n = 1024
	seen := make([]atomic.Int32, n)
	err := pool.ParallelFor(context.Background(), n, 4, func(ctx context.Context, start, end int) error {
		for i := start; i < end; i++ {
			seen[i].Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ParallelFor: %v", err)
	}
	for i := range seen {
		if v := seen[i].Load(); v != 1 {
			t.Errorf("index %d seen %d times, want 1", i, v)
		}
	}
}

func TestParallelForZeroN(t *testing.T) {
	t.Parallel()
	called := false
	err := pool.ParallelFor(context.Background(), 0, 4, func(ctx context.Context, start, end int) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("ParallelFor: %v", err)
	}
	if called {
		t.Error("fn should not be called when n=0")
	}
}

func TestParallelForSingleWorker(t *testing.T) {
	t.Parallel()
	calls := 0
	err := pool.ParallelFor(context.Background(), 100, 1, func(ctx context.Context, start, end int) error {
		calls++
		if start != 0 || end != 100 {
			t.Errorf("expected single call [0, 100), got [%d, %d)", start, end)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ParallelFor: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestParallelForPropagatesError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("boom")
	err := pool.ParallelFor(context.Background(), 100, 4, func(ctx context.Context, start, end int) error {
		if start == 0 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel, got %v", err)
	}
}

func TestParallelForCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	err := pool.ParallelFor(ctx, 100, 4, func(ctx context.Context, start, end int) error {
		// Every worker should observe the cancellation.
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got %v", err)
	}
}

func TestMapChunks(t *testing.T) {
	t.Parallel()
	in := []int{1, 2, 3, 4, 5}
	out, err := pool.MapChunks(context.Background(), in, 3, func(ctx context.Context, i, v int) (int, error) {
		return v * 10, nil
	})
	if err != nil {
		t.Fatalf("MapChunks: %v", err)
	}
	want := []int{10, 20, 30, 40, 50}
	if len(out) != len(want) {
		t.Fatalf("len = %d, want %d", len(out), len(want))
	}
	for i := range out {
		if out[i] != want[i] {
			t.Errorf("out[%d] = %d, want %d", i, out[i], want[i])
		}
	}
}

func TestMapChunksError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("halt")
	_, err := pool.MapChunks(context.Background(), []int{1, 2, 3}, 2,
		func(ctx context.Context, i, v int) (int, error) {
			if v == 2 {
				return 0, sentinel
			}
			return v, nil
		})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel, got %v", err)
	}
}

func TestGroup(t *testing.T) {
	t.Parallel()
	g := pool.NewGroup(context.Background(), 2)
	var counter atomic.Int32
	for range 10 {
		g.Go(func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := counter.Load(); got != 10 {
		t.Errorf("counter = %d, want 10", got)
	}
}

func TestGroupParallelismBound(t *testing.T) {
	t.Parallel()
	g := pool.NewGroup(context.Background(), 3)
	var (
		mu        sync.Mutex
		active    int
		maxActive int
	)
	for range 20 {
		g.Go(func(ctx context.Context) error {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			mu.Lock()
			active--
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if maxActive > 3 {
		t.Errorf("maxActive = %d, want <= 3", maxActive)
	}
}

func BenchmarkParallelFor(b *testing.B) {
	const n = 1 << 20
	data := make([]int64, n)
	for i := range data {
		data[i] = int64(i)
	}

	b.Run("serial", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var sum int64
			for _, v := range data {
				sum += v
			}
			_ = sum
		}
	})
	b.Run("parallel-4", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var total atomic.Int64
			_ = pool.ParallelFor(context.Background(), n, 4, func(ctx context.Context, start, end int) error {
				var local int64
				for i := start; i < end; i++ {
					local += data[i]
				}
				total.Add(local)
				return nil
			})
		}
	})
}
