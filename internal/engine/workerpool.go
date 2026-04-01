package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erflow/backend/internal/metrics"
	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
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
		jobs:                 make(chan TreatmentJob, 20), // enough for max doctor capacity (5+4+4=13)
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
			metrics.WorkerPoolActive.Inc()
			wp.processJob(ctx, job)
			wp.activeJobs.Add(-1)
			metrics.WorkerPoolActive.Dec()
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
			metrics.ContextSwitchesTotal.Inc()

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

	// --- Resource acquisition ---
	// Optimal: skip shared resources entirely (deadlock prevention by elimination).
	// Other algorithms: acquire resources to demonstrate deadlock detection.
	algo := wp.store.Queue.Name()
	primaryRes, secondaryRes := resourcePlanForDoctor(job.DoctorID, algo)

	if algo != scheduler.AlgoOptimal && primaryRes != "" {
		if !wp.acquireWithWait(ctx, doc, p, primaryRes, true) {
			if doc != nil {
				doc.Busy = false
			}
			return
		}
		if secondaryRes != "" && !wp.acquireWithWait(ctx, doc, p, secondaryRes, false) {
			wp.releaseResource(doc, primaryRes, p.ID)
			if doc != nil {
				doc.Busy = false
			}
			return
		}
	}

	// Reset treatment clock to when the worker ACTUALLY starts.
	p.TreatmentStarted = time.Now()

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

	if algo != scheduler.AlgoOptimal && primaryRes != "" {
		if secondaryRes != "" {
			wp.releaseResource(doc, secondaryRes, p.ID)
		}
		wp.releaseResource(doc, primaryRes, p.ID)
	}

	// Track doctor visit
	p.DoctorVisits++
	if doc != nil {
		doc.Busy = false
		doc.TotalTreated++
	}

	// Assign nurse if not yet assigned (nurse handles vitals, meds)
	if p.AssignedNurse == "" {
		if nurse := wp.store.FindAvailableNurse(); nurse != nil {
			wp.store.AssignNurse(nurse.ID, p.ID)
			nurse.TotalAssisted++
		}
	}

	// After first visit: doctor may order lab/imaging (ESI 1-3)
	if p.DoctorVisits == 1 && !p.LabOrdered && p.TriageLevel <= 3 {
		p.LabOrdered = true
		p.LabOrderedAt = time.Now()
		p.Status = models.StatusAwaitingLab
		labTypes := []string{"blood", "x-ray", "ct-scan"}
		p.LabType = labTypes[int(p.TriageLevel)-1] // critical→blood, emergency→x-ray, urgent→ct-scan
		log.Printf("%s %s📋 LAB ORDERED:%s %s — %s for %s",
			tagTreatment, colorCyan, colorReset, doc.Name, p.LabType, p.Name)
		wp.store.AddEvent("lab.ordered",
			fmt.Sprintf("LAB ORDERED: %s ordered %s for %s", doc.Name, p.LabType, p.Name),
			"resource-management",
			map[string]any{"patientId": p.ID, "labType": p.LabType, "doctorId": doc.ID},
		)
		// Don't discharge — patient stays in bed waiting for results
		return
	}

	// If more visits needed, don't discharge yet
	if p.DoctorVisits < p.MaxDoctorVisits {
		// Reset treatment for next visit (shorter follow-up)
		p.RemainingTreatment = p.EstimatedDuration / 3
		p.TreatmentStarted = time.Now()
		return
	}

	// All visits complete — send to discharge/disposition
	d := discharge{patient: p, bedID: job.BedID, docID: job.DoctorID}
	select {
	case wp.results <- d:
	case <-ctx.Done():
	}
}

func resourcePlanForDoctor(doctorID string, algo scheduler.Algorithm) (scheduler.ResourceType, scheduler.ResourceType) {
	// Optimal: ordered acquisition prevents circular wait (classic OS deadlock prevention).
	// All doctors acquire in the same order: lab → or, so no cycle is possible.
	if algo == scheduler.AlgoOptimal {
		switch doctorID {
		case "doc-1":
			return scheduler.ResLab, scheduler.ResOR
		case "doc-2":
			return scheduler.ResLab, scheduler.ResOR
		default:
			return scheduler.ResImaging, scheduler.ResLab
		}
	}

	// Other algorithms: intentionally opposing order for doc-1/doc-2 to allow circular wait.
	// This lets us demonstrate deadlock detection & resolution as an OS concept.
	switch doctorID {
	case "doc-1":
		return scheduler.ResLab, scheduler.ResOR
	case "doc-2":
		return scheduler.ResOR, scheduler.ResLab
	default:
		return scheduler.ResImaging, scheduler.ResLab
	}
}

func (wp *WorkerPool) acquireWithWait(
	ctx context.Context,
	doc *models.Doctor,
	p *models.Patient,
	res scheduler.ResourceType,
	primary bool,
) bool {
	if doc == nil {
		return false
	}

	rm := wp.store.Resources
	stage := "secondary"
	if primary {
		stage = "primary"
	}
	for retries := 0; retries < 12; retries++ {
		if rm.TryAcquire(res, doc.ID) {
			doc.WaitingFor = ""
			if !containsResource(doc.HeldResources, string(res)) {
				doc.HeldResources = append(doc.HeldResources, string(res))
			}
			wp.store.AddEvent(
				"resource.acquired",
				fmt.Sprintf("%s acquired %s (%s resource) for %s", doc.Name, res, stage, p.Name),
				"deadlock",
				map[string]any{
					"doctorId":  doc.ID,
					"patientId": p.ID,
					"resource":  string(res),
					"stage":     stage,
				},
			)
			return true
		}

		doc.WaitingFor = string(res)
		rm.RequestAndWait(res, doc.ID)
		if retries == 0 {
			wp.store.AddEvent(
				"resource.wait",
				fmt.Sprintf("%s waiting for %s while treating %s", doc.Name, res, p.Name),
				"deadlock",
				map[string]any{
					"doctorId":  doc.ID,
					"patientId": p.ID,
					"resource":  string(res),
					"stage":     stage,
				},
			)
		}
		if !wp.scaledSleepFn(ctx, 600*time.Millisecond) {
			return false
		}
	}
	// Timeout — proceed without resource to prevent permanent blocking
	doc.WaitingFor = ""
	rm.ForceRelease(res, doc.ID)
	return true
}

func (wp *WorkerPool) releaseResource(doc *models.Doctor, res scheduler.ResourceType, patientID string) {
	if doc == nil || res == "" {
		return
	}
	wp.store.Resources.Release(res, doc.ID)
	removeFromSlice(&doc.HeldResources, string(res))
	if doc.WaitingFor == string(res) {
		doc.WaitingFor = ""
	}
	wp.store.AddEvent(
		"resource.released",
		fmt.Sprintf("%s released %s", doc.Name, res),
		"resource-management",
		map[string]any{
			"doctorId":  doc.ID,
			"patientId": patientID,
			"resource":  string(res),
		},
	)
}

func containsResource(arr []string, resource string) bool {
	for _, item := range arr {
		if item == resource {
			return true
		}
	}
	return false
}
