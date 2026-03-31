package scheduler

import (
	"context"
	"sync/atomic"
)

// BedSemaphore is a counting semaphore backed by a buffered channel.
// OS parallel: sem_init(sem, capacity); sem_wait() blocks when 0; sem_post() releases.
// Go implementation: a buffered chan struct{} of size `capacity`.
// Each receive (<-sem) is P/wait/acquire. Each send (sem<-) is V/signal/release.
type BedSemaphore struct {
	sem       chan struct{}
	capacity  int
	name      string
	waitCount atomic.Int64 // goroutines currently blocked on Acquire
	acquired  atomic.Int64 // permits currently held (capacity - available)
}

// SemaphoreStats is returned to the frontend for visualization.
type SemaphoreStats struct {
	Name      string `json:"name"`
	Capacity  int    `json:"capacity"`
	Acquired  int64  `json:"acquired"`
	Waiting   int64  `json:"waiting"`
	Available int64  `json:"available"`
}

// NewBedSemaphore creates a semaphore with the given capacity.
// The channel is pre-filled: each struct{} in the buffer is an available permit.
func NewBedSemaphore(name string, capacity int) *BedSemaphore {
	s := &BedSemaphore{
		sem:      make(chan struct{}, capacity),
		capacity: capacity,
		name:     name,
	}
	// Fill the buffer — each slot is one available permit
	for i := 0; i < capacity; i++ {
		s.sem <- struct{}{}
	}
	return s
}

// Acquire blocks until a permit is available or ctx is cancelled.
// Returns true if acquired, false if context cancelled.
func (s *BedSemaphore) Acquire(ctx context.Context) bool {
	s.waitCount.Add(1)
	defer s.waitCount.Add(-1)

	select {
	case <-s.sem:
		s.acquired.Add(1)
		return true
	case <-ctx.Done():
		return false
	}
}

// TryAcquire attempts a non-blocking acquire. Returns true if a permit was obtained.
func (s *BedSemaphore) TryAcquire() bool {
	select {
	case <-s.sem:
		s.acquired.Add(1)
		return true
	default:
		return false
	}
}

// Release returns a permit to the semaphore.
func (s *BedSemaphore) Release() {
	s.acquired.Add(-1)
	s.sem <- struct{}{}
}

// Stats returns the current semaphore state for dashboard display.
func (s *BedSemaphore) Stats() SemaphoreStats {
	acq := s.acquired.Load()
	return SemaphoreStats{
		Name:      s.name,
		Capacity:  s.capacity,
		Acquired:  acq,
		Waiting:   s.waitCount.Load(),
		Available: int64(s.capacity) - acq,
	}
}
