package engine

import (
	"math"
	"sort"
	"sync"
	"time"
)

// LatencyTracker records task latencies in a fixed-size circular buffer
// and computes percentiles (P50, P95, P99) on demand.
// Thread-safe — called from multiple worker goroutines concurrently.
type LatencyTracker struct {
	mu      sync.Mutex
	samples []float64 // milliseconds
	size    int
	head    int
	full    bool
}

func NewLatencyTracker(size int) *LatencyTracker {
	return &LatencyTracker{
		samples: make([]float64, size),
		size:    size,
	}
}

// Record adds a latency sample. Called from worker goroutines.
func (lt *LatencyTracker) Record(d time.Duration) {
	ms := float64(d.Microseconds()) / 1000.0
	lt.mu.Lock()
	lt.samples[lt.head] = ms
	lt.head = (lt.head + 1) % lt.size
	if lt.head == 0 {
		lt.full = true
	}
	lt.mu.Unlock()
}

// Percentiles returns P50, P95, P99 in milliseconds.
func (lt *LatencyTracker) Percentiles() (p50, p95, p99 float64) {
	lt.mu.Lock()
	n := lt.count()
	if n == 0 {
		lt.mu.Unlock()
		return 0, 0, 0
	}
	buf := make([]float64, n)
	if lt.full {
		copy(buf, lt.samples)
	} else {
		copy(buf, lt.samples[:n])
	}
	lt.mu.Unlock()

	sort.Float64s(buf)
	p50 = buf[percentileIdx(n, 0.50)]
	p95 = buf[percentileIdx(n, 0.95)]
	p99 = buf[percentileIdx(n, 0.99)]
	return
}

// Count returns the number of recorded samples.
func (lt *LatencyTracker) Count() int {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.count()
}

func (lt *LatencyTracker) count() int {
	if lt.full {
		return lt.size
	}
	return lt.head
}

// Snapshot returns a JSON-friendly snapshot of current latency stats.
func (lt *LatencyTracker) Snapshot() LatencySnapshot {
	p50, p95, p99 := lt.Percentiles()
	return LatencySnapshot{
		P50:   math.Round(p50*100) / 100,
		P95:   math.Round(p95*100) / 100,
		P99:   math.Round(p99*100) / 100,
		Count: lt.Count(),
	}
}

// Reset clears all samples.
func (lt *LatencyTracker) Reset() {
	lt.mu.Lock()
	lt.head = 0
	lt.full = false
	lt.mu.Unlock()
}

type LatencySnapshot struct {
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Count int     `json:"count"`
}

func percentileIdx(n int, pct float64) int {
	idx := int(float64(n) * pct)
	if idx >= n {
		idx = n - 1
	}
	return idx
}
