package engine

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

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

	totalArrivals    int
	totalDischarged  int
	totalPreemptions int

	// Channel pipeline: generator -> scheduler -> treatment -> discharge
	arrivals   chan arrival
	discharges chan discharge
}

func New(s *store.MemStore) *Engine {
	return &Engine{
		store: s,
		speed: 1.0,
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

	e.wg.Add(4)
	go e.patientGenerator(ctx)
	go e.schedulerLoop(ctx)
	go e.treatmentSimulator(ctx)
	go e.agingDaemon(ctx)
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

	// Wait for all goroutines to finish — graceful shutdown
	e.wg.Wait()

	log.Printf("%s %sENGINE STOPPED%s — Arrivals: %d, Discharged: %d, Preemptions: %d",
		tagEngine, colorBold, colorReset, e.totalArrivals, e.totalDischarged, e.totalPreemptions)

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
	return map[string]any{
		"running":          e.running,
		"speed":            e.speed,
		"totalArrivals":    e.totalArrivals,
		"totalDischarged":  e.totalDischarged,
		"totalPreemptions": e.totalPreemptions,
	}
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

		e.mu.Lock()
		e.totalArrivals++
		arrivals := e.totalArrivals
		e.mu.Unlock()

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

func (e *Engine) scheduleNext(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		next := e.store.Queue.Peek()
		if next == nil {
			return
		}

		bed := e.store.FindAvailableBed("")
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
					e.mu.Lock()
					e.totalPreemptions++
					n := e.totalPreemptions
					e.mu.Unlock()

					log.Printf("%s %s⚡ PREEMPTION #%d: %s bumped %s from bed %s%s",
						tagPreemption, colorRed+colorBold, n,
						pr.IncomingPatientName, pr.PreemptedPatientName,
						pr.BedID, colorReset)

					e.store.AddEvent("preemption", scheduler.FormatPreemptionMessage(pr), "preemption", pr)
				}
			}
			return
		}

		patient := e.store.Queue.Dequeue()
		if patient == nil {
			return
		}

		ok, _ := e.store.AssignBedSafe(bed.ID, patient.ID)
		if !ok {
			e.store.Queue.Enqueue(patient)
			log.Printf("%s Bed %s taken (mutex prevented race) — %s re-queued",
				tagScheduler, bed.ID, patient.Name)
			continue
		}

		patient.Status = models.StatusInTreatment

		doc := e.store.FindAvailableDoctor()
		docName := "no doctor available"
		if doc != nil {
			_ = e.store.AssignDoctor(doc.ID, patient.ID)
			docName = doc.Name
		}

		tc := triageColor(patient.TriageLevel)
		log.Printf("%s %s→ ASSIGNED:%s %s → %sBed %s%s + %s [%s%s%s, pri=%d]",
			tagScheduler, colorGreen, colorReset,
			patient.Name, colorCyan, bed.ID, colorReset,
			docName, tc, patient.TriageLevelName, colorReset,
			patient.EffectivePri)

		concept := "priority-scheduling"
		if patient.TriageLevel == models.Critical {
			concept = "preemption"
		}

		e.store.AddEvent("patient.assigned",
			fmt.Sprintf("SCHEDULED: %s → Bed %s, %s [%s, priority %d]",
				patient.Name, bed.ID, docName, patient.TriageLevelName, patient.EffectivePri),
			concept,
			map[string]any{"patientId": patient.ID, "bedId": bed.ID, "doctorId": safeDocID(doc)},
		)
	}
}

// --- Goroutine 3: Treatment Simulator ---
// Checks for completed treatments and sends them through the discharges channel.

func (e *Engine) treatmentSimulator(ctx context.Context) {
	defer e.wg.Done()
	log.Printf("%s Goroutine started — sending discharges through channel", tagTreatment)

	for {
		if !e.scaledSleep(ctx, 1500*time.Millisecond) {
			log.Printf("%s Goroutine exiting", tagTreatment)
			return
		}

		patients := e.store.GetAllPatients()
		for _, p := range patients {
			if p.Status != models.StatusInTreatment {
				continue
			}
			if time.Since(p.CheckInTime) < treatmentDuration(p.TriageLevel) {
				continue
			}

			d := discharge{patient: p, bedID: p.AssignedBed, docID: p.AssignedDoc}

			// Send to discharges channel — processed inline here for simplicity,
			// but the channel makes it easy to add a separate consumer later.
			e.processDischarge(d)

			select {
			case e.discharges <- d:
			default:
				// non-blocking: if nobody is reading, that's fine
			}
		}
	}
}

func (e *Engine) processDischarge(d discharge) {
	p := d.patient

	if p.AssignedBed != "" {
		_, _ = e.store.ReleaseBed(p.AssignedBed)
	}

	if p.AssignedDoc != "" {
		if doc, ok := e.store.GetDoctor(p.AssignedDoc); ok {
			removeFromSlice(&doc.PatientIDs, p.ID)
			if doc.CurrentPatient == p.ID {
				doc.CurrentPatient = ""
			}
		}
	}

	p.Status = models.StatusDischarged
	p.AssignedBed = ""
	p.AssignedDoc = ""

	e.mu.Lock()
	e.totalDischarged++
	discharged := e.totalDischarged
	e.mu.Unlock()

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
