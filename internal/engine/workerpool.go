package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/store"
)

// TreatmentJob is a unit of work for the worker pool.
type TreatmentJob struct {
	Patient    *models.Patient
	BedID      string
	DoctorID   string
	EnqueuedAt time.Time // set at submit time — for end-to-end latency
}

// WorkerPoolStats is returned to the frontend for visualization.
type WorkerPoolStats struct {
	Workers        int             `json:"workers"`
	ActiveJobs     int32           `json:"activeJobs"`
	QueuedJobs     int             `json:"queuedJobs"`
	QueueCapacity  int             `json:"queueCapacity"`
	TotalCompleted int64           `json:"totalCompleted"`
	TotalSubmitted int64           `json:"totalSubmitted"`
	Latency        LatencySnapshot `json:"latency"`
}

// discharge signals a completed treatment back to the engine.
type discharge struct {
	patient *models.Patient
	bedID   string
	docID   string
	latency time.Duration
}

// WorkerPool implements a dynamically-sized goroutine pool.
// Workers can be added (ScaleUp) or removed (ScaleDown) at runtime
// without stopping the pool or losing in-flight jobs.
//
// Each worker reads from the shared jobs channel. When a worker's
// individual context is cancelled (ScaleDown), it finishes its current
// job and exits gracefully.
type WorkerPool struct {
	jobs    chan TreatmentJob
	results chan discharge

	activeJobs     atomic.Int32
	totalCompleted atomic.Int64
	totalSubmitted atomic.Int64
	workers        atomic.Int32

	latency *LatencyTracker

	store         *store.MemStore
	scaledSleepFn func(ctx context.Context, base time.Duration) bool

	// Dynamic scaling: each worker has its own cancellable context.
	workerCancels []context.CancelFunc
	workerMu      sync.Mutex
	wg            sync.WaitGroup
}

func NewWorkerPool(
	queueSize int,
	results chan discharge,
	st *store.MemStore,
	sleepFn func(ctx context.Context, base time.Duration) bool,
) *WorkerPool {
	return &WorkerPool{
		jobs:          make(chan TreatmentJob, queueSize),
		results:       results,
		store:         st,
		scaledSleepFn: sleepFn,
		latency:       NewLatencyTracker(10000), // 10K sample circular buffer
	}
}

// Start launches the initial set of worker goroutines.
func (wp *WorkerPool) Start(ctx context.Context, numWorkers int) {
	wp.ScaleUp(ctx, numWorkers)
	log.Printf("[POOL] Worker pool started: %d workers, queue capacity: %d",
		numWorkers, cap(wp.jobs))
}

// Stop signals all workers to finish and waits for them to exit.
func (wp *WorkerPool) Stop() {
	wp.workerMu.Lock()
	for _, cancel := range wp.workerCancels {
		cancel()
	}
	wp.workerCancels = nil
	wp.workerMu.Unlock()
	wp.wg.Wait()
	wp.workers.Store(0)
}

// ScaleUp spawns n additional worker goroutines.
func (wp *WorkerPool) ScaleUp(ctx context.Context, n int) {
	wp.workerMu.Lock()
	defer wp.workerMu.Unlock()

	for i := 0; i < n; i++ {
		workerCtx, cancel := context.WithCancel(ctx)
		wp.workerCancels = append(wp.workerCancels, cancel)
		wp.wg.Add(1)
		wp.workers.Add(1)
		go wp.worker(workerCtx)
	}
}

// ScaleDown removes n workers by cancelling their contexts.
// Workers finish their current job before exiting (graceful).
func (wp *WorkerPool) ScaleDown(n int) {
	wp.workerMu.Lock()
	defer wp.workerMu.Unlock()

	for i := 0; i < n && len(wp.workerCancels) > 0; i++ {
		last := len(wp.workerCancels) - 1
		wp.workerCancels[last]()
		wp.workerCancels = wp.workerCancels[:last]
	}
}

// Submit sends a job to the pool. Non-blocking — returns false if queue is full.
func (wp *WorkerPool) Submit(job TreatmentJob) bool {
	job.EnqueuedAt = time.Now()
	select {
	case wp.jobs <- job:
		wp.totalSubmitted.Add(1)
		return true
	default:
		return false // backpressure — queue full
	}
}

// Stats returns current pool state for the frontend.
func (wp *WorkerPool) Stats() WorkerPoolStats {
	return WorkerPoolStats{
		Workers:        int(wp.workers.Load()),
		ActiveJobs:     wp.activeJobs.Load(),
		QueuedJobs:     len(wp.jobs),
		QueueCapacity:  cap(wp.jobs),
		TotalCompleted: wp.totalCompleted.Load(),
		TotalSubmitted: wp.totalSubmitted.Load(),
		Latency:        wp.latency.Snapshot(),
	}
}

// QueueDepth returns the current number of jobs waiting in the queue.
func (wp *WorkerPool) QueueDepth() int { return len(wp.jobs) }

// QueueCapacity returns the maximum queue size.
func (wp *WorkerPool) QueueCapacity() int { return cap(wp.jobs) }

// WorkerCount returns the current number of active workers.
func (wp *WorkerPool) WorkerCount() int { return int(wp.workers.Load()) }

// Latency returns the latency tracker for external access.
func (wp *WorkerPool) Latency() *LatencyTracker { return wp.latency }

// worker is the goroutine function — one per "doctor" slot.
// Reads jobs from the shared channel, processes them, records latency.
func (wp *WorkerPool) worker(ctx context.Context) {
	defer func() {
		wp.workers.Add(-1)
		wp.wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-wp.jobs:
			if !ok {
				return // channel closed
			}
			wp.activeJobs.Add(1)
			latency := wp.processJob(ctx, job)
			wp.activeJobs.Add(-1)
			wp.totalCompleted.Add(1)

			if latency > 0 {
				wp.latency.Record(latency)
			}
		}
	}
}

func (wp *WorkerPool) processJob(ctx context.Context, job TreatmentJob) time.Duration {
	p := job.Patient
	doc, _ := wp.store.GetDoctor(job.DoctorID)

	if doc != nil {
		doc.Busy = true
	}

	// Treatment: simulate work proportional to severity.
	// At high speed, these become very short — allowing high throughput.
	duration := p.RemainingTreatment
	if duration <= 0 {
		duration = p.EstimatedDuration
	}

	wp.scaledSleepFn(ctx, duration)

	// Mark doctor done
	if doc != nil {
		doc.Busy = false
		doc.TotalTreated++
	}

	// Compute end-to-end latency: from enqueue to completion
	e2e := time.Since(job.EnqueuedAt)

	// Send discharge through results channel
	d := discharge{
		patient: p,
		bedID:   job.BedID,
		docID:   job.DoctorID,
		latency: e2e,
	}
	select {
	case wp.results <- d:
	case <-ctx.Done():
	}

	return e2e
}

// PoolStatus is a helper for logging.
func (wp *WorkerPool) PoolStatus() string {
	return fmt.Sprintf("workers=%d active=%d queued=%d/%d completed=%d",
		wp.workers.Load(), wp.activeJobs.Load(),
		len(wp.jobs), cap(wp.jobs), wp.totalCompleted.Load())
}
