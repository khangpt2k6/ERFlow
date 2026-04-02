package engine

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/store"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
)

// arrival is sent through the channel from load generator to scheduler.
type arrival struct {
	patient   *models.Patient
	complaint string
}

// Engine orchestrates the task scheduler pipeline.
//
// Architecture:
//   LoadGenerator → [arrivals chan] → Scheduler → [jobs chan] → WorkerPool → [discharges chan] → DischargeHandler
//                                                                   ↑
//                                                              AutoScaler
//                                                                   ↑
//                                                          MetricsCollector
//
// 5 goroutines + N dynamic workers. Focused on scaling, not breadth.
type Engine struct {
	store *store.MemStore

	mu      sync.RWMutex
	running bool
	speed   float64
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Atomic stats
	totalArrivals   atomic.Int64
	totalDischarged atomic.Int64
	totalShed       atomic.Int64 // load-shed rejected count

	// Channel pipeline
	arrivals   chan arrival
	discharges chan discharge

	// Core components
	pool       *WorkerPool
	scaler     *AutoScaler
	loadGen    *LoadGenerator
	throughput *ThroughputRecorder

	// Load shedding: reject low-priority tasks when queue > shedThreshold
	shedding     atomic.Bool
	shedThreshold float64 // fraction of queue capacity (e.g., 0.8)
}

func New(s *store.MemStore) *Engine {
	return &Engine{
		store:         s,
		speed:         1.0,
		shedThreshold: 0.8, // start shedding when queue 80% full
	}
}

// Start launches the engine pipeline.
func (e *Engine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true

	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.arrivals = make(chan arrival, 10000)   // large buffer for burst absorption
	e.discharges = make(chan discharge, 10000)
	e.mu.Unlock()

	log.Printf("[ENGINE] ━━━ ENGINE STARTED ━━━ Speed: %.1fx", e.speed)

	// Worker pool: start with 3, auto-scale to 50
	e.pool = NewWorkerPool(5000, e.discharges, e.store, e.scaledSleep)
	numDoctors := len(e.store.GetAllDoctors())
	if numDoctors < 3 {
		numDoctors = 3
	}
	e.pool.Start(ctx, numDoctors)

	// Auto-scaler
	e.scaler = NewAutoScaler(e.pool, numDoctors, 50)

	// Load generator
	e.loadGen = NewLoadGenerator(e.store, e.arrivals)

	// Throughput recorder
	e.throughput = NewThroughputRecorder(300) // 5 min at 1s intervals

	// Launch goroutines
	e.wg.Add(4)
	go e.schedulerLoop(ctx)
	go e.dischargeHandler(ctx)
	go e.scaler.Run(ctx, &e.wg)
	go e.metricsCollector(ctx)

	// Load generator in its own goroutine
	e.wg.Add(1)
	go e.loadGen.Run(ctx, &e.wg)

	e.store.AddEvent("engine.started",
		"Engine started — dynamic task scheduler active",
		"scaling",
		map[string]any{"speed": e.speed, "workers": numDoctors},
	)
}

// Stop gracefully shuts down all goroutines and the worker pool.
func (e *Engine) Stop() {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	e.running = false
	e.cancel()
	e.mu.Unlock()

	if e.pool != nil {
		e.pool.Stop()
	}
	e.wg.Wait()

	log.Printf("[ENGINE] ━━━ ENGINE STOPPED ━━━ Arrivals: %d, Discharged: %d, Shed: %d",
		e.totalArrivals.Load(), e.totalDischarged.Load(), e.totalShed.Load())
	e.store.AddEvent("engine.stopped", "Engine stopped", "scaling", nil)
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

func (e *Engine) SetSpeed(s float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s < 0.1 {
		s = 0.1
	}
	if s > 100 {
		s = 100
	}
	e.speed = s
}

func (e *Engine) GetSpeed() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.speed
}

// SetRPS sets the load generator's target requests per second.
func (e *Engine) SetRPS(rps int64) {
	if e.loadGen != nil {
		e.loadGen.SetRPS(rps)
	}
}

// GetRPS returns the current load generator RPS.
func (e *Engine) GetRPS() int64 {
	if e.loadGen != nil {
		return e.loadGen.GetRPS()
	}
	return 0
}

// Stats returns a comprehensive snapshot for the frontend.
func (e *Engine) Stats() map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := map[string]any{
		"running":         e.running,
		"speed":           e.speed,
		"totalArrivals":   e.totalArrivals.Load(),
		"totalDischarged": e.totalDischarged.Load(),
		"totalShed":       e.totalShed.Load(),
		"shedding":        e.shedding.Load(),
		"algorithm":       "priority",
	}
	if e.pool != nil {
		stats["workerPool"] = e.pool.Stats()
	}
	if e.scaler != nil {
		stats["autoScaler"] = e.scaler.Stats()
	}
	if e.loadGen != nil {
		stats["loadGen"] = e.loadGen.Stats()
	}
	if e.throughput != nil {
		stats["throughputSamples"] = e.throughput.Samples()
		stats["currentTPS"] = e.throughput.CurrentTPS()
	}
	return stats
}

// scaledSleep respects context cancellation and speed multiplier.
func (e *Engine) scaledSleep(ctx context.Context, base time.Duration) bool {
	e.mu.RLock()
	spd := e.speed
	e.mu.RUnlock()
	actual := time.Duration(float64(base) / spd)
	select {
	case <-time.After(actual):
		return true
	case <-ctx.Done():
		return false
	}
}

// --- Data ---

var patientNames = []string{
	"Sarah Mitchell", "Miguel Santos", "Aiko Tanaka", "James O'Brien",
	"Priya Sharma", "David Kim", "Fatima Al-Hassan", "Carlos Rivera",
	"Elena Volkov", "Marcus Johnson", "Liam Foster", "Zara Okafor",
	"Robert Chen", "Maria Gonzalez", "Ahmed Hassan", "Sofia Reyes",
	"Kenji Nakamura", "Isabella Torres", "Dmitri Orlov", "Amara Diallo",
	"Grace Liu", "Henry Clark", "Nina Petrov", "Oscar Mendez",
	"Yuki Sato", "Ravi Patel", "Chloe Anderson", "Wei Zhang",
	"Leila Khoury", "Jake Murphy", "Bella Rossi", "Sam Washington",
}

var complaints = map[models.TriageLevel][]string{
	models.Critical:   {"Cardiac arrest", "Massive internal bleeding", "Anaphylactic shock", "Severe stroke symptoms", "Unresponsive, not breathing"},
	models.Emergency:  {"Severe chest pain", "Difficulty breathing", "Major head trauma", "Heavy uncontrolled bleeding", "Suspected heart attack"},
	models.Urgent:     {"Broken arm", "Deep laceration", "Allergic reaction with swelling", "High fever with confusion", "Severe abdominal pain"},
	models.SemiUrgent: {"Sprained ankle", "Moderate back pain", "Persistent vomiting", "Minor burn", "Ear infection symptoms"},
	models.NonUrgent:  {"Common cold", "Minor headache", "Small cut", "Prescription refill", "Routine follow-up"},
}

func weightedTriage() models.TriageLevel {
	r := rand.Intn(100)
	switch {
	case r < 5:
		return models.Critical
	case r < 15:
		return models.Emergency
	case r < 40:
		return models.Urgent
	case r < 75:
		return models.SemiUrgent
	default:
		return models.NonUrgent
	}
}

// --- Goroutine 1: Scheduler ---
// Reads arrivals, applies admission control (load shedding), assigns beds + doctors.

func (e *Engine) schedulerLoop(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("[SCHEDULER] Started — reading arrivals, assigning workers")

	ticker := time.NewTicker(50 * time.Millisecond) // fast polling for high throughput
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case a, ok := <-e.arrivals:
			if !ok {
				return
			}
			e.handleArrival(ctx, a)

		case <-ticker.C:
			// Periodic tick — currently handled inline via handleArrival
		}
	}
}

func (e *Engine) handleArrival(_ context.Context, a arrival) {
	p := a.patient
	e.totalArrivals.Add(1)

	// --- Load Shedding ---
	// When worker pool queue is over threshold, reject low-priority (ESI 4-5) patients.
	// Critical/Emergency always admitted — priority-based admission control.
	queueRatio := float64(e.pool.QueueDepth()) / float64(e.pool.QueueCapacity())
	if queueRatio > e.shedThreshold && p.TriageLevel >= models.SemiUrgent {
		e.shedding.Store(true)
		e.totalShed.Add(1)
		p.Status = models.StatusDischarged
		return
	}
	if queueRatio < e.shedThreshold*0.8 {
		e.shedding.Store(false)
	}

	// Submit directly to worker pool — workers ARE the processing units.
	// No bed/doctor bottleneck: the pool itself is the bounded resource.
	p.Status = models.StatusInTreatment
	p.TreatmentStarted = time.Now()

	if !e.pool.Submit(TreatmentJob{
		Patient:  p,
		BedID:    "",
		DoctorID: "",
	}) {
		// Pool queue full — backpressure
		e.totalShed.Add(1)
		p.Status = models.StatusDischarged
	}
}

// --- Goroutine 2: Discharge Handler ---
// Reads completed tasks from the worker pool.

func (e *Engine) dischargeHandler(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("[DISCHARGE] Started — processing completed tasks")

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-e.discharges:
			if !ok {
				return
			}
			e.processDischarge(d)
		}
	}
}

func (e *Engine) processDischarge(d discharge) {
	p := d.patient
	if p.Status == models.StatusDischarged {
		return
	}
	p.Status = models.StatusDischarged
	e.totalDischarged.Add(1)
}

// --- Goroutine 3: Metrics Collector ---
// Periodically snapshots throughput for the dashboard charts.

func (e *Engine) metricsCollector(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("[METRICS] Started — recording throughput every 1s")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if e.pool != nil && e.throughput != nil {
				e.throughput.Record(
					e.pool.totalCompleted.Load(),
					e.pool.QueueDepth(),
					e.pool.WorkerCount(),
					e.shedding.Load(),
				)
			}
		}
	}
}

// Pool returns the worker pool for external stat access.
func (e *Engine) Pool() *WorkerPool { return e.pool }

// Scaler returns the auto-scaler for external stat access.
func (e *Engine) Scaler() *AutoScaler { return e.scaler }

// Throughput returns the throughput recorder.
func (e *Engine) Throughput() *ThroughputRecorder { return e.throughput }

// LoadGen returns the load generator.
func (e *Engine) LoadGen() *LoadGenerator { return e.loadGen }
