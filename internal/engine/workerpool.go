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

	// Context switch tracking — shared with engine
	totalContextSwitches *atomic.Int64

	// Thrashing reference — applies overhead multiplier
	thrashing *ThrashingMonitor
}

// NewWorkerPool creates a pool with one goroutine per doctor.
func NewWorkerPool(
	numWorkers int,
	results chan discharge,
	st *store.MemStore,
	sleepFn func(ctx context.Context, base time.Duration) bool,
	ctxSwitches *atomic.Int64,
	thrashing *ThrashingMonitor,
) *WorkerPool {
	return &WorkerPool{
		jobs:                 make(chan TreatmentJob, numWorkers*2),
		results:              results,
		workers:              numWorkers,
		store:                st,
		scaledSleepFn:        sleepFn,
		totalContextSwitches: ctxSwitches,
		thrashing:            thrashing,
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

// Submit sends a job to the pool. Non-blocking — returns false if channel is full.
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
// Reads TreatmentJobs from the shared channel, handles context switch overhead,
// applies thrashing multiplier, then simulates treatment.
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
	doc, _ := wp.store.GetDoctor(job.DoctorID)

	if doc != nil {
		doc.Busy = true

		// --- Context Switch Overhead ---
		// If this doctor was treating a different patient, there's overhead
		// for switching context (reviewing new chart, setting up, etc.)
		if doc.LastPatientID != "" && doc.LastPatientID != p.ID {
			doc.ContextSwitches++
			wp.totalContextSwitches.Add(1)

			overhead := 2 * time.Second // base context switch cost
			log.Printf("%s %s⇄ CONTEXT SWITCH:%s %s switching from %s to %s (overhead: %v)",
				tagTreatment, colorYellow, colorReset,
				doc.Name, doc.LastPatientID, p.ID, overhead)

			wp.store.AddEvent("context-switch",
				fmt.Sprintf("CONTEXT SWITCH: %s switching patients (overhead %v) — switches: %d",
					doc.Name, overhead, doc.ContextSwitches),
				"context-switch",
				map[string]any{
					"doctorId":  doc.ID,
					"fromPatient": doc.LastPatientID,
					"toPatient":   p.ID,
					"switches":    doc.ContextSwitches,
				},
			)

			wp.scaledSleepFn(ctx, overhead) // actual delay
		}
		doc.LastPatientID = p.ID
	}

	// --- Treatment Duration with Thrashing Multiplier ---
	duration := p.RemainingTreatment
	if duration <= 0 {
		duration = p.EstimatedDuration
	}

	// Apply thrashing overhead — when system is thrashing, treatment takes longer
	multiplier := wp.thrashing.OverheadMultiplier()
	if multiplier > 1.0 {
		duration = time.Duration(float64(duration) * multiplier)
	}

	wp.scaledSleepFn(ctx, duration)

	// Mark doctor done
	if doc != nil {
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
