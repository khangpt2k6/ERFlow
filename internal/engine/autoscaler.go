package engine

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// AutoScaler monitors the worker pool's job queue depth and dynamically
// adjusts the number of workers. This is the core scaling mechanism —
// analogous to Kubernetes HPA or cloud auto-scaling groups.
//
// Scale-up: when queue fills past scaleUpAt threshold, spawn workers aggressively.
// Scale-down: when queue drains below scaleDownAt threshold, remove workers conservatively.
// Cooldown prevents flapping between scale-up and scale-down.
type AutoScaler struct {
	pool       *WorkerPool
	minWorkers int
	maxWorkers int

	// Thresholds (fraction of queue capacity, 0.0–1.0)
	scaleUpAt   float64 // e.g., 0.7 = scale up when queue 70% full
	scaleDownAt float64 // e.g., 0.3 = scale down when queue 30% full

	// Cooldown between scale actions
	lastScaleUp   time.Time
	lastScaleDown time.Time
	cooldownUp    time.Duration
	cooldownDown  time.Duration

	// Stats
	TotalScaleUps   atomic.Int64
	TotalScaleDowns atomic.Int64

	mu sync.Mutex
}

func NewAutoScaler(pool *WorkerPool, min, max int) *AutoScaler {
	return &AutoScaler{
		pool:         pool,
		minWorkers:   min,
		maxWorkers:   max,
		scaleUpAt:    0.7,
		scaleDownAt:  0.3,
		cooldownUp:   300 * time.Millisecond,
		cooldownDown: 2 * time.Second,
	}
}

// Run is the auto-scaler goroutine. Evaluates every 100ms.
func (as *AutoScaler) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			as.evaluate(ctx)
		}
	}
}

func (as *AutoScaler) evaluate(ctx context.Context) {
	as.mu.Lock()
	defer as.mu.Unlock()

	queueDepth := as.pool.QueueDepth()
	queueCap := as.pool.QueueCapacity()
	if queueCap == 0 {
		return
	}
	ratio := float64(queueDepth) / float64(queueCap)
	current := as.pool.WorkerCount()

	if ratio > as.scaleUpAt && current < as.maxWorkers {
		if time.Since(as.lastScaleUp) < as.cooldownUp {
			return
		}
		// Scale up aggressively: +50% or at least 2
		toAdd := current / 2
		if toAdd < 2 {
			toAdd = 2
		}
		if current+toAdd > as.maxWorkers {
			toAdd = as.maxWorkers - current
		}
		if toAdd > 0 {
			as.pool.ScaleUp(ctx, toAdd)
			as.lastScaleUp = time.Now()
			as.TotalScaleUps.Add(1)
			log.Printf("[AUTOSCALER] ▲ SCALE UP: %d → %d workers (queue: %d/%d = %.0f%%)",
				current, current+toAdd, queueDepth, queueCap, ratio*100)
		}
	} else if ratio < as.scaleDownAt && current > as.minWorkers {
		if time.Since(as.lastScaleDown) < as.cooldownDown {
			return
		}
		// Scale down conservatively: -1 at a time
		toRemove := 1
		if current-toRemove < as.minWorkers {
			toRemove = current - as.minWorkers
		}
		if toRemove > 0 {
			as.pool.ScaleDown(toRemove)
			as.lastScaleDown = time.Now()
			as.TotalScaleDowns.Add(1)
			log.Printf("[AUTOSCALER] ▼ SCALE DOWN: %d → %d workers (queue: %d/%d = %.0f%%)",
				current, current-toRemove, queueDepth, queueCap, ratio*100)
		}
	}
}

// Stats returns current auto-scaler metrics.
func (as *AutoScaler) Stats() AutoScalerStats {
	as.mu.Lock()
	defer as.mu.Unlock()
	return AutoScalerStats{
		MinWorkers:      as.minWorkers,
		MaxWorkers:      as.maxWorkers,
		CurrentWorkers:  as.pool.WorkerCount(),
		ScaleUpAt:       as.scaleUpAt,
		ScaleDownAt:     as.scaleDownAt,
		TotalScaleUps:   as.TotalScaleUps.Load(),
		TotalScaleDowns: as.TotalScaleDowns.Load(),
	}
}

type AutoScalerStats struct {
	MinWorkers      int     `json:"minWorkers"`
	MaxWorkers      int     `json:"maxWorkers"`
	CurrentWorkers  int     `json:"currentWorkers"`
	ScaleUpAt       float64 `json:"scaleUpAt"`
	ScaleDownAt     float64 `json:"scaleDownAt"`
	TotalScaleUps   int64   `json:"totalScaleUps"`
	TotalScaleDowns int64   `json:"totalScaleDowns"`
}
