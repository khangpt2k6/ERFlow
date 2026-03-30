package engine

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
	"github.com/erflow/backend/internal/store"
)

// Engine is the heart of the simulation. It runs as background goroutines
// that continuously push patients through the ER pipeline:
//
//   ARRIVAL → TRIAGE QUEUE → BED ASSIGNMENT → TREATMENT → DISCHARGE
//
// OS parallel: This is the kernel's main loop. In a real OS, background
// daemons (kswapd, ksoftirqd, migration threads) run continuously to manage
// memory, interrupts, and load balancing. Our engine goroutines do the same
// for the ER: the scheduler assigns resources, the aging daemon prevents
// starvation, and the treatment simulator models process execution time.

type Engine struct {
	store   *store.MemStore
	mu      sync.RWMutex
	running bool
	speed   float64 // 1.0 = normal, 0.5 = slow, 3.0 = fast
	cancel  chan struct{}

	// Stats
	totalArrivals   int
	totalDischarged int
	totalPreemptions int
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
	e.cancel = make(chan struct{})
	e.mu.Unlock()

	e.store.AddEvent("engine.started",
		"Simulation engine started — patients will flow automatically through the ER pipeline",
		"priority-scheduling",
		map[string]any{"speed": e.speed},
	)

	go e.patientGenerator()
	go e.schedulerLoop()
	go e.treatmentSimulator()
	go e.agingDaemon()
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.cancel)

	e.store.AddEvent("engine.stopped",
		"Simulation engine paused",
		"priority-scheduling",
		nil,
	)
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
	e.speed = s
	e.store.AddEvent("engine.speed",
		fmt.Sprintf("Simulation speed changed to %.1fx", s),
		"priority-scheduling",
		map[string]any{"speed": s},
	)
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

// scaledSleep sleeps for the given duration divided by speed.
func (e *Engine) scaledSleep(base time.Duration) {
	e.mu.RLock()
	spd := e.speed
	e.mu.RUnlock()
	actual := time.Duration(float64(base) / spd)
	select {
	case <-time.After(actual):
	case <-e.cancel:
	}
}

// --- GOROUTINE 1: Patient Generator ---
// Continuously creates new patients at random intervals.
// OS parallel: hardware interrupts bringing new work into the system.

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

func (e *Engine) patientGenerator() {
	nameIdx := 0
	for {
		select {
		case <-e.cancel:
			return
		default:
		}

		// Random arrival interval: 2-6 seconds (scaled by speed)
		delay := time.Duration(2000+rand.Intn(4000)) * time.Millisecond
		e.scaledSleep(delay)

		select {
		case <-e.cancel:
			return
		default:
		}

		// Pick a random triage level (weighted: more non-urgent than critical)
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
		e.mu.Unlock()

		e.store.AddEvent("patient.arrival",
			fmt.Sprintf("NEW ARRIVAL: %s — %s [%s, priority %d]", p.Name, complaint, p.TriageLevelName, p.EffectivePri),
			"priority-scheduling",
			map[string]any{"patientId": p.ID, "triage": int(triage), "priority": p.EffectivePri},
		)
	}
}

// weightedTriage returns a random triage level, weighted realistically.
// Critical: 5%, Emergency: 10%, Urgent: 25%, Semi-Urgent: 35%, Non-Urgent: 25%
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

// --- GOROUTINE 2: Scheduler Loop ---
// Continuously assigns waiting patients to available beds and doctors.
// OS parallel: the CPU scheduler's main loop — pick highest priority, assign to core.

func (e *Engine) schedulerLoop() {
	for {
		select {
		case <-e.cancel:
			return
		default:
		}

		e.scaledSleep(800 * time.Millisecond)

		select {
		case <-e.cancel:
			return
		default:
		}

		// Try to assign the highest-priority waiting patient to a bed
		next := e.store.Queue.Peek()
		if next == nil {
			continue
		}

		bed := e.store.FindAvailableBed("")
		if bed == nil {
			// No beds — check if preemption is needed for critical patients
			if next.TriageLevel == models.Critical {
				results := scheduler.CheckPreemption(
					e.store.Queue,
					e.store.GetAllPatients(),
					e.store.GetBedsMap(),
					e.store.GetDoctorsMap(),
				)
				for _, pr := range results {
					e.mu.Lock()
					e.totalPreemptions++
					e.mu.Unlock()
					e.store.AddEvent("preemption",
						scheduler.FormatPreemptionMessage(pr),
						"preemption",
						pr,
					)
				}
			}
			continue
		}

		// Dequeue and assign
		patient := e.store.Queue.Dequeue()
		if patient == nil {
			continue
		}

		ok, _ := e.store.AssignBedSafe(bed.ID, patient.ID)
		if !ok {
			// Bed was taken between Peek and now — re-enqueue
			e.store.Queue.Enqueue(patient)
			continue
		}

		patient.Status = models.StatusInTreatment

		// Assign a doctor
		doc := e.store.FindAvailableDoctor()
		docName := "awaiting doctor"
		if doc != nil {
			_ = e.store.AssignDoctor(doc.ID, patient.ID)
			docName = doc.Name
		}

		concept := "priority-scheduling"
		if patient.TriageLevel == models.Critical {
			concept = "preemption"
		}

		e.store.AddEvent("patient.assigned",
			fmt.Sprintf("SCHEDULED: %s → Bed %s, %s [%s, priority %d]",
				patient.Name, bed.ID, docName, patient.TriageLevelName, patient.EffectivePri),
			concept,
			map[string]any{
				"patientId": patient.ID,
				"bedId":     bed.ID,
				"doctorId":  safeDocID(doc),
			},
		)
	}
}

// --- GOROUTINE 3: Treatment Simulator ---
// Tracks patients being treated and discharges them when done.
// OS parallel: process execution — each process runs for a time quantum, then terminates.

func (e *Engine) treatmentSimulator() {
	for {
		select {
		case <-e.cancel:
			return
		default:
		}

		e.scaledSleep(1500 * time.Millisecond)

		select {
		case <-e.cancel:
			return
		default:
		}

		// Find patients being treated and check if they're "done"
		patients := e.store.GetAllPatients()
		for _, p := range patients {
			if p.Status != models.StatusInTreatment {
				continue
			}

			// Treatment time depends on severity:
			// Critical: 15-25s, Emergency: 10-18s, Urgent: 8-14s, Semi: 6-10s, Non: 4-8s
			// (all scaled by speed)
			treatmentTime := treatmentDuration(p.TriageLevel)
			elapsed := time.Since(p.CheckInTime)

			if elapsed < treatmentTime {
				continue
			}

			// Discharge
			e.dischargePatient(p)
		}
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

func (e *Engine) dischargePatient(p *models.Patient) {
	// Release bed
	if p.AssignedBed != "" {
		_, _ = e.store.ReleaseBed(p.AssignedBed)
	}

	// Remove from doctor
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
	e.mu.Unlock()

	e.store.AddEvent("patient.discharged",
		fmt.Sprintf("DISCHARGED: %s — treatment complete [was %s]", p.Name, p.TriageLevelName),
		"resource-management",
		map[string]any{"patientId": p.ID},
	)
}

// --- GOROUTINE 4: Aging Daemon ---
// Periodically boosts priority of long-waiting patients.
// OS parallel: the aging daemon in priority schedulers.

func (e *Engine) agingDaemon() {
	for {
		select {
		case <-e.cancel:
			return
		default:
		}

		e.scaledSleep(5 * time.Second)

		select {
		case <-e.cancel:
			return
		default:
		}

		patients := e.store.GetAllPatients()
		results := scheduler.ApplyAging(e.store.Queue, patients, 0)
		for _, ar := range results {
			e.store.AddEvent("aging",
				fmt.Sprintf("AGING: %s priority %d → %d (waited %dm)",
					ar.PatientName, ar.OldPriority, ar.NewPriority, ar.WaitMinutes),
				"aging",
				ar,
			)
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
