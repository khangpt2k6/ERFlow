package engine

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
	"github.com/erflow/backend/internal/store"
)

// Color codes for terminal output
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
)

// Goroutine labels for log output
const (
	tagGenerator  = colorCyan + "[GENERATOR]" + colorReset
	tagScheduler  = colorPurple + "[SCHEDULER]" + colorReset
	tagTreatment  = colorGreen + "[TREATMENT]" + colorReset
	tagAging      = colorYellow + "[AGING]    " + colorReset
	tagPreemption = colorRed + "[PREEMPT]  " + colorReset
	tagEngine     = colorBlue + "[ENGINE]   " + colorReset
)

type Engine struct {
	store   *store.MemStore
	mu      sync.RWMutex
	running bool
	speed   float64
	cancel  chan struct{}

	totalArrivals    int
	totalDischarged  int
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

	log.Printf("%s %s=========================================%s", tagEngine, colorBold, colorReset)
	log.Printf("%s %sENGINE STARTED — Speed: %.1fx%s", tagEngine, colorBold, e.speed, colorReset)
	log.Printf("%s Launching 4 goroutines (OS kernel daemons):", tagEngine)
	log.Printf("%s   %s → Patient arrivals (hardware interrupts)", tagEngine, tagGenerator)
	log.Printf("%s   %s → Priority scheduling (CPU scheduler)", tagEngine, tagScheduler)
	log.Printf("%s   %s → Process execution (time quanta)", tagEngine, tagTreatment)
	log.Printf("%s   %s → Starvation prevention (aging daemon)", tagEngine, tagAging)
	log.Printf("%s %s=========================================%s", tagEngine, colorBold, colorReset)

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

// =====================================================================
// GOROUTINE 1: Patient Generator
// OS concept: Hardware interrupts — new work arriving in the system
// =====================================================================

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

func (e *Engine) patientGenerator() {
	log.Printf("%s Goroutine started — generating patients every 2-6s (scaled by speed)", tagGenerator)
	nameIdx := 0
	for {
		select {
		case <-e.cancel:
			log.Printf("%s Goroutine exiting", tagGenerator)
			return
		default:
		}

		delay := time.Duration(2000+rand.Intn(4000)) * time.Millisecond
		e.scaledSleep(delay)

		select {
		case <-e.cancel:
			return
		default:
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

		qLen := e.store.Queue.Len()
		tc := triageColor(triage)
		log.Printf("%s %s+ %s%s — %s [%s%s%s, pri=%d] (queue: %d, total arrivals: %d)",
			tagGenerator, colorGreen, name, colorReset,
			complaint, tc, p.TriageLevelName, colorReset,
			p.EffectivePri, qLen, arrivals)

		e.store.AddEvent("patient.arrival",
			fmt.Sprintf("NEW ARRIVAL: %s — %s [%s, priority %d]", p.Name, complaint, p.TriageLevelName, p.EffectivePri),
			"priority-scheduling",
			map[string]any{"patientId": p.ID, "triage": int(triage), "priority": p.EffectivePri},
		)
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

// =====================================================================
// GOROUTINE 2: Scheduler Loop
// OS concept: CPU scheduler — picks highest-priority process from ready queue
// =====================================================================

func (e *Engine) schedulerLoop() {
	log.Printf("%s Goroutine started — scheduling every 800ms (scaled by speed)", tagScheduler)
	for {
		select {
		case <-e.cancel:
			log.Printf("%s Goroutine exiting", tagScheduler)
			return
		default:
		}

		e.scaledSleep(800 * time.Millisecond)

		select {
		case <-e.cancel:
			return
		default:
		}

		next := e.store.Queue.Peek()
		if next == nil {
			continue
		}

		bed := e.store.FindAvailableBed("")
		if bed == nil {
			// No beds available
			if next.TriageLevel == models.Critical {
				log.Printf("%s %s⚠ No beds for CRITICAL patient %s — attempting PREEMPTION%s",
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
					preemptions := e.totalPreemptions
					e.mu.Unlock()

					log.Printf("%s %s⚡ PREEMPTION #%d: %s (pri=%d) BUMPED %s (pri=%d) from bed %s%s",
						tagPreemption, colorRed+colorBold, preemptions,
						pr.IncomingPatientName, pr.IncomingPriority,
						pr.PreemptedPatientName, pr.PreemptedPriority,
						pr.BedID, colorReset)

					e.store.AddEvent("preemption", scheduler.FormatPreemptionMessage(pr), "preemption", pr)
				}
				if len(results) == 0 {
					log.Printf("%s %s✗ Preemption failed — no lower-priority patients to bump%s",
						tagScheduler, colorRed, colorReset)
				}
			} else {
				log.Printf("%s All beds full — %s (pri=%d) stays in queue (pos: queue has %d waiting)",
					tagScheduler, next.Name, next.EffectivePri, e.store.Queue.Len())
			}
			continue
		}

		patient := e.store.Queue.Dequeue()
		if patient == nil {
			continue
		}

		ok, _ := e.store.AssignBedSafe(bed.ID, patient.ID)
		if !ok {
			e.store.Queue.Enqueue(patient)
			log.Printf("%s Bed %s was taken (race avoided by mutex) — %s re-queued",
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

// =====================================================================
// GOROUTINE 3: Treatment Simulator
// OS concept: Process execution — each process runs for a time quantum
// =====================================================================

func (e *Engine) treatmentSimulator() {
	log.Printf("%s Goroutine started — checking treatment completion every 1.5s", tagTreatment)
	for {
		select {
		case <-e.cancel:
			log.Printf("%s Goroutine exiting", tagTreatment)
			return
		default:
		}

		e.scaledSleep(1500 * time.Millisecond)

		select {
		case <-e.cancel:
			return
		default:
		}

		patients := e.store.GetAllPatients()
		for _, p := range patients {
			if p.Status != models.StatusInTreatment {
				continue
			}

			treatmentTime := treatmentDuration(p.TriageLevel)
			elapsed := time.Since(p.CheckInTime)

			if elapsed < treatmentTime {
				continue
			}

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
	bedID := p.AssignedBed
	docID := p.AssignedDoc

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
	log.Printf("%s %s✓ DISCHARGED:%s %s — Bed %s freed, %s released [%s%s%s] (treated %s, total discharged: %d)",
		tagTreatment, colorGreen, colorReset,
		p.Name, bedID, docID,
		tc, p.TriageLevelName, colorReset,
		elapsed, discharged)

	e.store.AddEvent("patient.discharged",
		fmt.Sprintf("DISCHARGED: %s — treatment complete [was %s]", p.Name, p.TriageLevelName),
		"resource-management",
		map[string]any{"patientId": p.ID},
	)
}

// =====================================================================
// GOROUTINE 4: Aging Daemon
// OS concept: Priority aging — prevents starvation of low-priority processes
// =====================================================================

func (e *Engine) agingDaemon() {
	log.Printf("%s Goroutine started — scanning for stale patients every 5s", tagAging)
	for {
		select {
		case <-e.cancel:
			log.Printf("%s Goroutine exiting", tagAging)
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
		if len(results) > 0 {
			log.Printf("%s %s↑ AGING PASS: %d patient(s) boosted:%s",
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
