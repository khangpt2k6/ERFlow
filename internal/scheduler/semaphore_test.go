package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSemaphore_Capacity(t *testing.T) {
	sem := NewBedSemaphore("ICU", 3)

	// Acquire all 3 permits
	for i := 0; i < 3; i++ {
		if !sem.TryAcquire() {
			t.Fatalf("should be able to acquire permit %d", i+1)
		}
	}

	// 4th should fail
	if sem.TryAcquire() {
		t.Fatal("should not be able to acquire beyond capacity")
	}

	stats := sem.Stats()
	if stats.Acquired != 3 {
		t.Errorf("expected 3 acquired, got %d", stats.Acquired)
	}
	if stats.Available != 0 {
		t.Errorf("expected 0 available, got %d", stats.Available)
	}
}

func TestSemaphore_Release(t *testing.T) {
	sem := NewBedSemaphore("test", 1)

	sem.TryAcquire()
	if sem.TryAcquire() {
		t.Fatal("should be full")
	}

	sem.Release()
	if !sem.TryAcquire() {
		t.Fatal("should be able to acquire after release")
	}
}

func TestSemaphore_BlockingAcquire(t *testing.T) {
	sem := NewBedSemaphore("test", 1)
	sem.TryAcquire() // fill it

	done := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		got := sem.Acquire(ctx)
		done <- got
	}()

	// Release after 100ms
	time.Sleep(100 * time.Millisecond)
	sem.Release()

	select {
	case got := <-done:
		if !got {
			t.Fatal("acquire should have succeeded after release")
		}
	case <-time.After(time.Second):
		t.Fatal("acquire timed out")
	}
}

func TestSemaphore_ContextCancellation(t *testing.T) {
	sem := NewBedSemaphore("test", 1)
	sem.TryAcquire() // fill it

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	if sem.Acquire(ctx) {
		t.Fatal("should have failed due to cancelled context")
	}
}

func TestSemaphore_Concurrent(t *testing.T) {
	sem := NewBedSemaphore("test", 5)
	var wg sync.WaitGroup

	acquired := make(chan struct{}, 100)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			if sem.Acquire(ctx) {
				acquired <- struct{}{}
				time.Sleep(10 * time.Millisecond)
				sem.Release()
			}
		}()
	}

	wg.Wait()
	close(acquired)

	count := 0
	for range acquired {
		count++
	}
	if count != 20 {
		t.Errorf("expected all 20 goroutines to eventually acquire, got %d", count)
	}
}
