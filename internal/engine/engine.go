package engine

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erflow/backend/internal/metrics"
	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
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

const (
	tagGenerator  = colorCyan + "[GENERATOR]" + colorReset
	tagScheduler  = colorPurple + "[SCHEDULER]" + colorReset
	tagTreatment  = colorGreen + "[TREATMENT]" + colorReset
	tagAging      = colorYellow + "[AGING]    " + colorReset
	tagPreemption = colorRed + "[PREEMPT]  " + colorReset
	tagEngine     = colorBlue + "[ENGINE]   " + colorReset
)

// Channels carry patients between goroutines instead of shared memory.
// This is Go's core concurrency philosophy: "share memory by communicating."
type arrival struct {
	patient   *models.Patient
	complaint string
}

type discharge struct {
	patient *models.Patient
	bedID   string
	docID   string
}

type Engine struct {
	store *store.MemStore

	mu      sync.RWMutex
	running bool
	speed   float64
	cancel  context.CancelFunc
	wg      sync.WaitGroup // tracks all goroutines for graceful shutdown

	// Atomic stats — no mutex needed for counters
	totalArrivals        atomic.Int64
	totalDischarged      atomic.Int64
	totalPreemptions     atomic.Int64
	totalContextSwitches atomic.Int64

	// Channel pipeline: generator -> scheduler -> treatment -> discharge
	arrivals   chan arrival
	discharges chan discharge

	// Worker pool — doctors as bounded goroutine workers
	pool *WorkerPool

	// Thrashing monitor — detects when demand > capacity
	thrashing *ThrashingMonitor
}

func New(s *store.MemStore) *Engine {
	return &Engine{
		store:     s,
		speed:     1.0,
		thrashing: NewThrashingMonitor(2.0), // thrash when patients > 2x beds
	}
}

func (e *Engine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true

	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.arrivals = make(chan arrival, 10)
	e.discharges = make(chan discharge, 10)
	e.mu.Unlock()

	log.Printf("%s %sENGINE STARTED — Speed: %.1fx%s", tagEngine, colorBold, e.speed, colorReset)
	log.Printf("%s Pipeline: generator → arrivals chan → scheduler → discharges chan → cleanup", tagEngine)

	e.store.AddEvent("engine.started",
		"Simulation engine started — channel pipeline active",
		"priority-scheduling",
		map[string]any{"speed": e.speed},
	)

	// Create worker pool — one goroutine per doctor (bounded concurrency)
	numDoctors := len(e.store.GetAllDoctors())
	e.pool = NewWorkerPool(numDoctors, e.discharges, e.store, e.scaledSleep, &e.totalContextSwitches, e.thrashing)
	e.pool.Start(ctx)

	e.wg.Add(7)
	go e.patientGenerator(ctx)
	go e.schedulerLoop(ctx)
	go e.treatmentSimulator(ctx)
	go e.agingDaemon(ctx)
	go e.throughputTracker(ctx)
	go e.dischargeHandler(ctx)
	go e.deadlockDetector(ctx)
}

func (e *Engine) Stop() {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	e.running = false
	e.cancel()
	e.mu.Unlock()

	// Stop worker pool first (closes jobs channel, waits for workers)
	if e.pool != nil {
		e.pool.Stop()
	}

	// Wait for all goroutines to finish — graceful shutdown
	e.wg.Wait()

	log.Printf("%s %sENGINE STOPPED%s — Arrivals: %d, Discharged: %d, Preemptions: %d",
		tagEngine, colorBold, colorReset, e.totalArrivals.Load(), e.totalDischarged.Load(), e.totalPreemptions.Load())

	e.store.AddEvent("engine.stopped", "Simulation engine paused", "priority-scheduling", nil)
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
	if s > 10 {
		s = 10
	}
	old := e.speed
	e.speed = s
	log.Printf("%s Speed changed: %.1fx → %.1fx", tagEngine, old, s)
	e.store.AddEvent("engine.speed", fmt.Sprintf("Simulation speed changed to %.1fx", s), "priority-scheduling", map[string]any{"speed": s})
}

func (e *Engine) GetSpeed() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.speed
}

func (e *Engine) Stats() map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := map[string]any{
		"running":              e.running,
		"speed":                e.speed,
		"totalArrivals":        e.totalArrivals.Load(),
		"totalDischarged":      e.totalDischarged.Load(),
		"totalPreemptions":     e.totalPreemptions.Load(),
		"totalContextSwitches": e.totalContextSwitches.Load(),
		"algorithm":            string(e.store.Queue.Name()),
		"thrashing":            e.thrashing.Stats(),
	}
	if e.pool != nil {
		stats["workerPool"] = e.pool.Stats()
	}
	return stats
}

// SetScheduler swaps the scheduling algorithm. Drains waiting patients from the
// old queue into the new one, preserving their state.
func (e *Engine) SetScheduler(algo scheduler.Algorithm) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.store.Queue.Name() == algo {
		return
	}

	// Drain old queue
	var patients []*models.Patient
	for {
		p := e.store.Queue.Dequeue()
		if p == nil {
			break
		}
		patients = append(patients, p)
	}

	// Create new scheduler and re-enqueue
	e.store.Queue = scheduler.NewScheduler(algo)
	for _, p := range patients {
		e.store.Queue.Enqueue(p)
	}

	metrics.SetActiveScheduler(string(algo))
	log.Printf("%s Scheduler changed to %s (%d patients migrated)", tagEngine, algo, len(patients))
	e.store.AddEvent("engine.scheduler",
		fmt.Sprintf("Scheduling algorithm changed to %s — %d patients re-queued", algo, len(patients)),
		"priority-scheduling",
		map[string]any{"algorithm": string(algo), "migrated": len(patients)},
	)
}

// GetScheduler returns the current scheduling algorithm name.
func (e *Engine) GetScheduler() scheduler.Algorithm {
	return e.store.Queue.Name()
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

func triageColor(t models.TriageLevel) string {
	switch t {
	case models.Critical:
		return colorRed + colorBold
	case models.Emergency:
		return colorRed
	case models.Urgent:
		return colorYellow
	case models.SemiUrgent:
		return colorBlue
	default:
		return colorDim
	}
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

func treatmentDuration(triage models.TriageLevel) time.Duration {
	switch triage {
	case models.Critical:
		return time.Duration(15+rand.Intn(10)) * time.Second
	case models.Emergency:
		return time.Duration(10+rand.Intn(8)) * time.Second
	case models.Urgent:
		return time.Duration(8+rand.Intn(6)) * time.Second
	case models.SemiUrgent:
		return time.Duration(6+rand.Intn(4)) * time.Second
	default:
		return time.Duration(4+rand.Intn(4)) * time.Second
	}
}

// --- Goroutine 1: Patient Generator ---
// Produces patients and sends them through the arrivals channel.
// The scheduler receives from this channel — no shared memory needed.

func (e *Engine) patientGenerator(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("%s Goroutine started — sending patients to arrivals channel", tagGenerator)

	nameIdx := 0
	for {
		delay := time.Duration(2000+rand.Intn(4000)) * time.Millisecond
		if !e.scaledSleep(ctx, delay) {
			log.Printf("%s Goroutine exiting", tagGenerator)
			return
		}

		triage := weightedTriage()
		name := patientNames[nameIdx%len(patientNames)]
		nameIdx++
		complaintList := complaints[triage]
		complaint := complaintList[rand.Intn(len(complaintList))]

		id := e.store.NextPatientID()
		p := models.NewPatient(id, name, triage, complaint)
		e.store.AddPatient(p)
		e.store.Queue.Enqueue(p)

		arrivals := e.totalArrivals.Add(1)
		metrics.PatientsTotal.WithLabelValues(triage.String()).Inc()

		tc := triageColor(triage)
		log.Printf("%s %s+ %s%s — %s [%s%s%s, pri=%d] (queue: %d, total: %d)",
			tagGenerator, colorGreen, name, colorReset,
			complaint, tc, p.TriageLevelName, colorReset,
			p.EffectivePri, e.store.Queue.Len(), arrivals)

		e.store.AddEvent("patient.arrival",
			fmt.Sprintf("NEW ARRIVAL: %s — %s [%s, priority %d]", p.Name, complaint, p.TriageLevelName, p.EffectivePri),
			"priority-scheduling",
			map[string]any{"patientId": p.ID, "triage": int(triage), "priority": p.EffectivePri},
		)

		// Send to arrivals channel — scheduler picks it up
		select {
		case e.arrivals <- arrival{patient: p, complaint: complaint}:
		case <-ctx.Done():
			return
		}
	}
}

// --- Goroutine 2: Scheduler ---
// Reads from arrivals channel as a signal to schedule, then assigns beds.
// Sends completed treatments to the discharges channel.

func (e *Engine) schedulerLoop(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("%s Goroutine started — reading from arrivals channel + polling", tagScheduler)

	ticker := time.NewTicker(800 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("%s Goroutine exiting", tagScheduler)
			return

		case <-e.arrivals:
			// New patient arrived — try to schedule immediately
			e.scheduleNext(ctx)

		case <-ticker.C:
			// Periodic check for any unscheduled patients
			e.scheduleNext(ctx)
		}
	}
}

// preemptForCritical finds the lowest-priority patient currently being treated,
// removes them from their doctor and bed, and returns the freed doctor.
// This ensures critical/emergency patients are never left waiting while
// non-urgent patients occupy resources.
func (e *Engine) preemptForCritical(ctx context.Context, incoming *models.Patient) (*models.Doctor, *models.Bed) {
	// Find the lowest-priority (highest priority number) patient in treatment
	var victim *models.Patient
	var victimDoc *models.Doctor
	var victimBed *models.Bed

	for _, p := range e.store.GetAllPatients() {
		if p.Status != models.StatusInTreatment || p.AssignedDoc == "" || p.AssignedBed == "" {
			continue
		}
		// Only preempt if incoming is higher priority (lower number)
		if p.TriageLevel <= incoming.TriageLevel {
			continue
		}
		if victim == nil || p.EffectivePri > victim.EffectivePri {
			victim = p
			if d, ok := e.store.GetDoctor(p.AssignedDoc); ok {
				victimDoc = d
			}
			if b, ok := e.store.GetBed(p.AssignedBed); ok {
				victimBed = b
			}
		}
	}

	if victim == nil || victimDoc == nil || victimBed == nil {
		return nil, nil
	}

	// Preempt: remove victim from doctor and bed, put back in queue
	e.store.RemovePatientFromDoctor(victimDoc.ID, victim.ID)
	_, _ = e.store.ReleaseBed(victimBed.ID)
	victim.Status = models.StatusWaiting
	victim.AssignedBed = ""
	victim.AssignedDoc = ""
	victim.Preempted = true
	e.store.Queue.Enqueue(victim)

	n := e.totalPreemptions.Add(1)
	metrics.PreemptionsTotal.Inc()
	log.Printf("%s %s⚡ PREEMPTION #%d: CRITICAL %s bumped %s from %s + Bed %s%s",
		tagPreemption, colorRed+colorBold, n,
		incoming.Name, victim.Name, victimDoc.Name, victimBed.ID, colorReset)

	e.store.AddEvent("preemption",
		fmt.Sprintf("PREEMPTION: %s [%s] bumped %s [%s] — freed %s + Bed %s",
			incoming.Name, incoming.TriageLevelName, victim.Name, victim.TriageLevelName,
			victimDoc.Name, victimBed.ID),
		"preemption",
		map[string]any{
			"incomingPatientName":  incoming.Name,
			"preemptedPatientName": victim.Name,
			"doctorId":             victimDoc.ID,
			"bedId":                victimBed.ID,
		},
	)

	return victimDoc, victimBed
}

func (e *Engine) scheduleNext(ctx context.Context) {
	// Only assign ONE patient per scheduling cycle — gives the UI time to show movement
	select {
	case <-ctx.Done():
		return
	default:
	}

	next := e.store.Queue.Peek()
	if next == nil {
		return
	}

	// CONSTRAINT 1: Must have an available doctor (doctors are the CPU cores)
	// Without a doctor, treatment cannot start — patient must wait.
	// EXCEPTION: Critical/Emergency patients PREEMPT the lowest-priority patient.
	var preemptedBed *models.Bed
	doc := e.store.FindAvailableDoctor()
	if doc == nil {
		if next.TriageLevel <= models.Emergency {
			// Critical or Emergency: preempt lowest-priority patient from a doctor
			doc, preemptedBed = e.preemptForCritical(ctx, next)
			if doc == nil {
				log.Printf("%s %s⚠ No doctors for CRITICAL %s — preemption failed%s",
					tagScheduler, colorRed, next.Name, colorReset)
				return
			}
		} else {
			log.Printf("%s No doctors available — %d patient(s) waiting",
				tagScheduler, e.store.Queue.Len())
			return
		}
	}

	// CONSTRAINT 2: Must have an available bed (preemption may have already freed one)
	bed := preemptedBed
	if bed == nil {
		bed = e.store.FindAvailableBed("")
	}
	if bed == nil {
		if next.TriageLevel == models.Critical {
			log.Printf("%s %s⚠ No beds for CRITICAL %s — attempting PREEMPTION%s",
				tagScheduler, colorRed+colorBold, next.Name, colorReset)

			results := scheduler.CheckPreemption(
				e.store.Queue,
				e.store.GetAllPatients(),
				e.store.GetBedsMap(),
				e.store.GetDoctorsMap(),
			)
			for _, pr := range results {
				n := e.totalPreemptions.Add(1)
				metrics.PreemptionsTotal.Inc()
				log.Printf("%s %s⚡ PREEMPTION #%d: %s bumped %s from bed %s%s",
					tagPreemption, colorRed+colorBold, n,
					pr.IncomingPatientName, pr.PreemptedPatientName,
					pr.BedID, colorReset)
				e.store.AddEvent("preemption", scheduler.FormatPreemptionMessage(pr), "preemption", pr)
			}
		} else {
			log.Printf("%s All beds full — %s (pri=%d) waits (queue: %d)",
				tagScheduler, next.Name, next.EffectivePri, e.store.Queue.Len())
		}
		return
	}

	// Both bed AND doctor available — assign the patient
	patient := e.store.Queue.Dequeue()
	if patient == nil {
		return
	}

	ok, _ := e.store.AssignBedSafe(bed.ID, patient.ID)
	if !ok {
		e.store.Queue.Enqueue(patient)
		log.Printf("%s Bed %s taken (mutex prevented race) — %s re-queued",
			tagScheduler, bed.ID, patient.Name)
		return
	}

	patient.Status = models.StatusInTreatment // patient goes to bed, doctor comes to them
	patient.TreatmentStarted = time.Now()
	_ = e.store.AssignDoctor(doc.ID, patient.ID)

	// Record wait duration: time from check-in to treatment start
	waitSec := time.Since(patient.CheckInTime).Seconds()
	metrics.WaitDuration.WithLabelValues(patient.TriageLevel.String(), string(e.store.Queue.Name())).Observe(waitSec)

	// Submit treatment job to worker pool — doctor goroutine handles it
	if e.pool != nil {
		if !e.pool.Submit(TreatmentJob{
			Patient:  patient,
			BedID:    bed.ID,
			DoctorID: doc.ID,
		}) {
			// Pool channel full — undo assignment so resources aren't leaked
			e.store.RemovePatientFromDoctor(doc.ID, patient.ID)
			_, _ = e.store.ReleaseBed(bed.ID)
			patient.Status = models.StatusWaiting
			e.store.Queue.Enqueue(patient)
			log.Printf("%s Worker pool full — %s re-queued", tagScheduler, patient.Name)
			return
		}
	}

	tc := triageColor(patient.TriageLevel)
	log.Printf("%s %s→ ASSIGNED:%s %s → %sBed %s%s + %s [%s%s%s, pri=%d]",
		tagScheduler, colorGreen, colorReset,
		patient.Name, colorCyan, bed.ID, colorReset,
		doc.Name, tc, patient.TriageLevelName, colorReset,
		patient.EffectivePri)

	concept := "priority-scheduling"
	if patient.TriageLevel == models.Critical {
		concept = "preemption"
	}

	e.store.AddEvent("patient.assigned",
		fmt.Sprintf("SCHEDULED: %s → Bed %s, %s [%s, priority %d]",
			patient.Name, bed.ID, doc.Name, patient.TriageLevelName, patient.EffectivePri),
		concept,
		map[string]any{"patientId": patient.ID, "bedId": bed.ID, "doctorId": doc.ID},
	)
}

// --- Goroutine 3: Treatment Monitor ---
// Updates RemainingTreatment for frontend progress bars and handles RR/MLFQ quantum interrupts.
// NOTE: Actual discharge is handled by the worker pool → dischargeHandler pipeline.
// This goroutine does NOT discharge patients — it only monitors and interrupts.

func (e *Engine) treatmentSimulator(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("%s Treatment monitor started — progress bars + quantum interrupts", tagTreatment)

	for {
		if !e.scaledSleep(ctx, 1500*time.Millisecond) {
			log.Printf("%s Goroutine exiting", tagTreatment)
			return
		}

		algo := e.store.Queue.Name()
		patients := e.store.GetAllPatients()
		for _, p := range patients {
			if p.Status != models.StatusInTreatment {
				continue
			}

			elapsed := time.Since(p.TreatmentStarted)

			// Update RemainingTreatment so the frontend can show a live progress bar
			newRemaining := p.EstimatedDuration - elapsed
			if newRemaining < 0 {
				newRemaining = 0
			}
			p.RemainingTreatment = newRemaining

			// Round Robin: check quantum expiry before completion
			if algo == scheduler.AlgoRoundRobin {
				rr, ok := e.store.Queue.(*scheduler.RoundRobinQueue)
				quantum := scheduler.DefaultQuantum
				if ok {
					quantum = rr.GetQuantum()
				}
				if elapsed >= quantum && p.RemainingTreatment > quantum {
					e.interruptPatient(p, quantum)
					continue
				}
			}

			// MLFQ: check quantum expiry per level
			if algo == scheduler.AlgoMLFQ {
				levelQuantum := scheduler.GetMLFQQuantum(p.MLFQLevel)
				if levelQuantum > 0 && elapsed >= levelQuantum && p.RemainingTreatment > levelQuantum {
					e.interruptPatientMLFQ(p, levelQuantum)
					continue
				}
			}

			// Discharge is handled by worker pool → dischargeHandler. NOT here.
		}
	}
}

// interruptPatient handles Round Robin quantum expiry — patient goes back to queue.
func (e *Engine) interruptPatient(p *models.Patient, quantum time.Duration) {
	bedID := p.AssignedBed
	docID := p.AssignedDoc

	p.RemainingTreatment -= quantum
	p.Status = models.StatusWaiting
	p.AssignedBed = ""
	p.AssignedDoc = ""

	if docID != "" {
		e.store.RemovePatientFromDoctor(docID, p.ID)
	}
	if bedID != "" {
		_, _ = e.store.ReleaseBed(bedID)
	}

	e.store.Queue.Enqueue(p)

	log.Printf("%s %s↻ QUANTUM EXPIRED:%s %s — %s remaining, back to queue (Round Robin)",
		tagTreatment, colorYellow, colorReset, p.Name, p.RemainingTreatment)
	e.store.AddEvent("quantum.expired",
		fmt.Sprintf("ROUND ROBIN: %s quantum expired — %s treatment remaining, rotated to back of queue", p.Name, p.RemainingTreatment),
		"round-robin",
		map[string]any{"patientId": p.ID, "remaining": p.RemainingTreatment.String()},
	)
}

// interruptPatientMLFQ handles MLFQ quantum expiry — patient gets demoted to lower queue.
func (e *Engine) interruptPatientMLFQ(p *models.Patient, quantum time.Duration) {
	bedID := p.AssignedBed
	docID := p.AssignedDoc
	oldLevel := p.MLFQLevel

	p.RemainingTreatment -= quantum
	p.Status = models.StatusWaiting
	p.AssignedBed = ""
	p.AssignedDoc = ""

	if docID != "" {
		e.store.RemovePatientFromDoctor(docID, p.ID)
	}
	if bedID != "" {
		_, _ = e.store.ReleaseBed(bedID)
	}

	// Demote: increase MLFQ level (lower priority queue)
	if p.MLFQLevel < 2 {
		p.MLFQLevel++
	}
	e.store.Queue.Enqueue(p)

	log.Printf("%s %s↓ MLFQ DEMOTION:%s %s — Q%d→Q%d, %s remaining",
		tagTreatment, colorYellow, colorReset, p.Name, oldLevel, p.MLFQLevel, p.RemainingTreatment)
	e.store.AddEvent("mlfq.demotion",
		fmt.Sprintf("MLFQ: %s demoted Q%d→Q%d — used full quantum, %s remaining", p.Name, oldLevel, p.MLFQLevel, p.RemainingTreatment),
		"mlfq",
		map[string]any{"patientId": p.ID, "oldLevel": oldLevel, "newLevel": p.MLFQLevel},
	)
}

func (e *Engine) processDischarge(d discharge) {
	p := d.patient

	// Guard: prevent double-discharge
	if p.Status == models.StatusDischarged {
		return
	}

	// Use d.docID (saved BEFORE discharge) — ReleaseBed wipes p.AssignedDoc
	if d.docID != "" {
		e.store.RemovePatientFromDoctor(d.docID, p.ID)
	}

	if d.bedID != "" {
		_, _ = e.store.ReleaseBed(d.bedID)
	}

	p.Status = models.StatusDischarged
	p.AssignedBed = ""
	p.AssignedDoc = ""

	discharged := e.totalDischarged.Add(1)
	metrics.DischargesTotal.WithLabelValues(p.TriageLevel.String()).Inc()
	if !p.TreatmentStarted.IsZero() {
		treatSec := time.Since(p.TreatmentStarted).Seconds()
		metrics.TreatmentDuration.WithLabelValues(p.TriageLevel.String()).Observe(treatSec)
	}

	elapsed := time.Since(p.CheckInTime).Round(time.Second)
	tc := triageColor(p.TriageLevel)
	log.Printf("%s %s✓ DISCHARGED:%s %s — Bed %s freed [%s%s%s] (%s, total: %d)",
		tagTreatment, colorGreen, colorReset,
		p.Name, d.bedID, tc, p.TriageLevelName, colorReset,
		elapsed, discharged)

	e.store.AddEvent("patient.discharged",
		fmt.Sprintf("DISCHARGED: %s — treatment complete [was %s]", p.Name, p.TriageLevelName),
		"resource-management",
		map[string]any{"patientId": p.ID},
	)
}

// --- Goroutine: Discharge Handler ---
// Reads completed treatments from the worker pool's results channel
// and processes them (free bed, remove doctor, mark discharged).

func (e *Engine) dischargeHandler(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("%s Discharge handler started — reading from worker pool results", tagTreatment)

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

// --- Goroutine 4: Aging Daemon ---

func (e *Engine) agingDaemon(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("%s Goroutine started — scanning for stale patients every 5s", tagAging)

	for {
		if !e.scaledSleep(ctx, 5*time.Second) {
			log.Printf("%s Goroutine exiting", tagAging)
			return
		}

		patients := e.store.GetAllPatients()
		results := scheduler.ApplyAging(e.store.Queue, patients, 0)
		if len(results) > 0 {
			metrics.AgingBoosts.Add(float64(len(results)))
			log.Printf("%s %s↑ AGING: %d patient(s) boosted%s",
				tagAging, colorYellow, len(results), colorReset)
			for _, ar := range results {
				log.Printf("%s   %s: priority %d → %d (waited %dm)",
					tagAging, ar.PatientName, ar.OldPriority, ar.NewPriority, ar.WaitMinutes)
				e.store.AddEvent("aging",
					fmt.Sprintf("AGING: %s priority %d → %d (waited %dm)",
						ar.PatientName, ar.OldPriority, ar.NewPriority, ar.WaitMinutes),
					"aging", ar,
				)
			}
		}
	}
}

// --- Goroutine 5: Throughput Tracker ---
// Monitors patient-to-bed ratio and detects thrashing.

func (e *Engine) throughputTracker(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("%s Throughput tracker started — monitoring for thrashing", tagEngine)

	for {
		if !e.scaledSleep(ctx, 5*time.Second) {
			return
		}

		patients := e.store.GetAllPatients()
		active := 0
		for _, p := range patients {
			if p.Status != models.StatusDischarged {
				active++
			}
		}
		totalBeds := len(e.store.GetAllBeds())

		// Update Prometheus gauges
		metrics.ActivePatients.Set(float64(active))
		metrics.QueueLength.WithLabelValues(string(e.store.Queue.Name())).Set(float64(e.store.Queue.Len()))
		if totalBeds > 0 {
			metrics.PatientBedRatio.Set(float64(active) / float64(totalBeds))
		}

		stateChanged := e.thrashing.Check(active, totalBeds, e.totalDischarged.Load())
		if stateChanged {
			if e.thrashing.IsThrashing() {
				metrics.ThrashingActive.Set(1)
				log.Printf("%s %s⚠ THRASHING DETECTED — patient/bed ratio > %.1f, treatment slowing%s",
					tagEngine, colorRed+colorBold, e.thrashing.threshold, colorReset)
				e.store.AddEvent("thrashing.started",
					fmt.Sprintf("THRASHING: %d active patients vs %d beds (ratio %.1f) — treatment times increased %.1fx",
						active, totalBeds, float64(active)/float64(totalBeds), e.thrashing.overheadFactor),
					"thrashing",
					e.thrashing.Stats(),
				)
			} else {
				metrics.ThrashingActive.Set(0)
				log.Printf("%s %s✓ Thrashing resolved — ratio back to normal%s",
					tagEngine, colorGreen, colorReset)
				e.store.AddEvent("thrashing.resolved",
					"Thrashing resolved — patient load reduced, treatment times normalized",
					"thrashing",
					e.thrashing.Stats(),
				)
			}
		}
	}
}

// --- Goroutine 6: Deadlock Detector ---
// Continuously monitors the resource manager for circular waits between doctors.
// OS parallel: the kernel's deadlock detection thread that periodically scans
// the wait-for graph for cycles.

func (e *Engine) deadlockDetector(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("%s Deadlock detector started — scanning every 2s", tagEngine)

	for {
		if !e.scaledSleep(ctx, 2*time.Second) {
			return
		}

		rm := e.store.Resources
		graph := scheduler.NewWaitForGraph()
		graph.BuildFromResources(rm)

		detected, cycle := graph.DetectCycle()
		if detected {
			metrics.DeadlocksDetected.Inc()
			log.Printf("%s %s⚠ DEADLOCK DETECTED: cycle %v%s", tagEngine, colorRed+colorBold, cycle, colorReset)

			e.store.AddEvent("deadlock.detected",
				fmt.Sprintf("DEADLOCK DETECTED: Circular wait among %v", cycle),
				"deadlock",
				map[string]any{"cycle": cycle, "edges": graph.Edges()},
			)

			// Resolve: pick victim, force release
			victim, action := scheduler.ResolveDeadlock(cycle, rm)
			if victim != "" {
				metrics.DeadlocksResolved.Inc()
				// Clean up doctor state
				if doc, ok := e.store.GetDoctor(victim); ok {
					doc.HeldResources = nil
					doc.WaitingFor = ""

					log.Printf("%s %s✓ DEADLOCK RESOLVED: %s — %s%s",
						tagEngine, colorGreen, doc.Name, action, colorReset)

					e.store.AddEvent("deadlock.resolved",
						fmt.Sprintf("DEADLOCK RESOLVED: %s selected as victim — %s", doc.Name, action),
						"deadlock",
						map[string]any{"victim": victim, "action": action},
					)
				}

				// Clear waiting state on other doctors in the cycle
				for _, id := range cycle {
					if id != victim {
						if doc, ok := e.store.GetDoctor(id); ok {
							doc.WaitingFor = ""
						}
					}
				}
			}
		}
	}
}

func removeFromSlice(s *[]string, val string) {
	for i, v := range *s {
		if v == val {
			*s = append((*s)[:i], (*s)[i+1:]...)
			return
		}
	}
}

func safeDocID(d *models.Doctor) string {
	if d == nil {
		return ""
	}
	return d.ID
}
