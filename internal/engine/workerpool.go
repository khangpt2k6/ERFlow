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

// TreatmentJob is a unit of work for the doctor worker pool.
type TreatmentJob struct {
	Patient  *models.Patient
	BedID    string
	DoctorID string
}

// WorkerPoolStats is returned to the frontend for visualization.
type WorkerPoolStats struct {
	Workers        int   `json:"workers"`
	ActiveJobs     int32 `json:"activeJobs"`
	QueuedJobs     int   `json:"queuedJobs"`
	TotalCompleted int64 `json:"totalCompleted"`
}

// WorkerPool implements a bounded goroutine pool where each worker represents a doctor.
// OS parallel: a thread pool — N worker threads pull tasks from a shared work queue.
// When all workers are busy, new jobs queue up in the channel (backpressure).
type WorkerPool struct {
	jobs           chan TreatmentJob
	results        chan discharge
	workers        int
	activeJobs     atomic.Int32
	totalCompleted atomic.Int64
	store          *store.MemStore
	scaledSleepFn  func(ctx context.Context, base time.Duration) bool
	wg             sync.WaitGroup
}

// NewWorkerPool creates a pool with one goroutine per doctor.
func NewWorkerPool(
	numWorkers int,
	results chan discharge,
	st *store.MemStore,
	sleepFn func(ctx context.Context, base time.Duration) bool,
) *WorkerPool {
	return &WorkerPool{
		jobs:          make(chan TreatmentJob, numWorkers*2), // buffer = 2x workers
		results:       results,
		workers:       numWorkers,
		store:         st,
		scaledSleepFn: sleepFn,
	}
}

// Start launches N worker goroutines.
func (wp *WorkerPool) Start(ctx context.Context) {
	wp.wg.Add(wp.workers)
	for i := 0; i < wp.workers; i++ {
		go wp.worker(ctx, i)
	}
	log.Printf("%s Worker pool started: %d doctor workers", tagEngine, wp.workers)
}

// Stop closes the jobs channel and waits for all workers to finish.
func (wp *WorkerPool) Stop() {
	close(wp.jobs)
	wp.wg.Wait()
}

// Submit sends a job to the pool. Non-blocking if channel has room.
func (wp *WorkerPool) Submit(job TreatmentJob) bool {
	select {
	case wp.jobs <- job:
		return true
	default:
		return false // pool exhausted — channel full
	}
}

// Stats returns current pool state.
func (wp *WorkerPool) Stats() WorkerPoolStats {
	return WorkerPoolStats{
		Workers:        wp.workers,
		ActiveJobs:     wp.activeJobs.Load(),
		QueuedJobs:     len(wp.jobs),
		TotalCompleted: wp.totalCompleted.Load(),
	}
}

// worker is the goroutine function — one per doctor.
// Reads TreatmentJobs from the shared channel, simulates treatment, sends discharges.
func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-wp.jobs:
			if !ok {
				return // channel closed
			}
			wp.activeJobs.Add(1)
			wp.processJob(ctx, job)
			wp.activeJobs.Add(-1)
			wp.totalCompleted.Add(1)
		}
	}
}

func (wp *WorkerPool) processJob(ctx context.Context, job TreatmentJob) {
	p := job.Patient

	// Mark doctor as busy
	if doc, ok := wp.store.GetDoctor(job.DoctorID); ok {
		doc.Busy = true
	}

	// Simulate treatment by sleeping for the remaining treatment duration
	duration := p.RemainingTreatment
	if duration <= 0 {
		duration = p.EstimatedDuration
	}

	wp.scaledSleepFn(ctx, duration)

	// Mark doctor as idle
	if doc, ok := wp.store.GetDoctor(job.DoctorID); ok {
		doc.Busy = false
		doc.TotalTreated++
	}

	// Send discharge through results channel
	d := discharge{patient: p, bedID: job.BedID, docID: job.DoctorID}
	select {
	case wp.results <- d:
	case <-ctx.Done():
	}
}

func formatPoolEvent(stats WorkerPoolStats) string {
	return fmt.Sprintf("Worker pool: %d/%d active, %d queued, %d completed",
		stats.ActiveJobs, stats.Workers, stats.QueuedJobs, stats.TotalCompleted)
}
