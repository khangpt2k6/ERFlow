package api

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
	"github.com/erflow/backend/internal/store"
)

// =============================================================================
// SIMULATION ENDPOINTS
// =============================================================================
//
// These endpoints trigger real OS scenarios against the ER system so you can
// observe scheduling, preemption, aging, mutex behavior, and race conditions
// in action. Every action emits events tagged with the OS concept it demonstrates.
// =============================================================================

type SimulationHandler struct {
	store *store.MemStore
}

// --- POST /api/simulate/rush-hour ---
// Adds 8-10 patients of mixed triage levels rapidly.
// OS concept: process burst — many processes arriving at once stress-tests the scheduler.

func (h *SimulationHandler) RushHour(w http.ResponseWriter, r *http.Request) {
	type rushPatient struct {
		Name      string
		Triage    models.TriageLevel
		Complaint string
	}

	incoming := []rushPatient{
		{"Sarah Mitchell", models.Urgent, "Broken arm from fall"},
		{"Miguel Santos", models.NonUrgent, "Persistent cough for 3 days"},
		{"Aiko Tanaka", models.Emergency, "Severe chest pain radiating to left arm"},
		{"James O'Brien", models.SemiUrgent, "Sprained ankle, moderate swelling"},
		{"Priya Sharma", models.Critical, "Unresponsive, possible overdose"},
		{"David Kim", models.Urgent, "Deep laceration on forearm, heavy bleeding"},
		{"Fatima Al-Hassan", models.NonUrgent, "Low-grade fever and sore throat"},
		{"Carlos Rivera", models.Emergency, "Difficulty breathing, lips turning blue"},
		{"Elena Volkov", models.SemiUrgent, "Abdominal pain, nausea for 12 hours"},
		{"Marcus Johnson", models.Urgent, "Allergic reaction, facial swelling"},
	}

	// Shuffle to simulate unpredictable arrival order
	rand.Shuffle(len(incoming), func(i, j int) {
		incoming[i], incoming[j] = incoming[j], incoming[i]
	})

	h.store.AddEvent("simulation.rush-hour.start",
		"RUSH HOUR: 10 patients arriving in rapid succession — scheduler must triage them all",
		"priority-scheduling",
		map[string]int{"patientCount": len(incoming)},
	)

	var created []*models.Patient
	for _, rp := range incoming {
		id := h.store.NextPatientID()
		p := models.NewPatient(id, rp.Name, rp.Triage, rp.Complaint)
		h.store.AddPatient(p)
		h.store.Queue.Enqueue(p)
		created = append(created, p)

		h.store.AddEvent("patient.checkin",
			fmt.Sprintf("[Rush Hour] %s checked in — Triage: %s (priority %d)", p.Name, p.TriageLevelName, p.EffectivePri),
			"priority-scheduling",
			map[string]any{
				"patientId":   p.ID,
				"triageLevel": int(p.TriageLevel),
				"priority":    p.EffectivePri,
			},
		)
	}

	// Show the resulting queue order
	queued := h.store.Queue.All()
	queueOrder := make([]string, len(queued))
	for i, p := range queued {
		queueOrder[i] = fmt.Sprintf("%d. %s (%s, pri=%d)", i+1, p.Name, p.TriageLevelName, p.EffectivePri)
	}

	h.store.AddEvent("simulation.rush-hour.complete",
		fmt.Sprintf("Rush hour complete: %d patients in queue, scheduler sorted by priority", len(queued)),
		"priority-scheduling",
		map[string]any{"queueOrder": queueOrder},
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"scenario":   "rush-hour",
		"concept":    "priority-scheduling",
		"added":      len(created),
		"queueOrder": queueOrder,
		"message":    "10 patients admitted — priority queue sorted them by triage severity",
	})
}

// --- POST /api/simulate/cardiac-cascade ---
// Adds 3 Critical patients rapidly. If beds are full, triggers preemption.

func (h *SimulationHandler) CardiacCascade(w http.ResponseWriter, r *http.Request) {
	criticals := []struct {
		Name      string
		Complaint string
	}{
		{"Robert Chen", "Cardiac arrest — found unresponsive in waiting room"},
		{"Maria Gonzalez", "Massive stroke — sudden onset, left side paralysis"},
		{"Ahmed Hassan", "Anaphylactic shock — throat closing, cannot breathe"},
	}

	h.store.AddEvent("simulation.cardiac-cascade.start",
		"CARDIAC CASCADE: 3 Critical patients arriving — system must preempt lower-priority treatments",
		"preemption",
		map[string]int{"criticalCount": 3},
	)

	var created []*models.Patient
	for _, c := range criticals {
		id := h.store.NextPatientID()
		p := models.NewPatient(id, c.Name, models.Critical, c.Complaint)
		h.store.AddPatient(p)
		h.store.Queue.Enqueue(p)
		created = append(created, p)

		h.store.AddEvent("patient.checkin.critical",
			fmt.Sprintf("[Cardiac Cascade] CRITICAL: %s — %s", p.Name, c.Complaint),
			"preemption",
			map[string]any{
				"patientId": p.ID,
				"priority":  p.EffectivePri,
				"complaint": c.Complaint,
			},
		)
	}

	// Attempt preemption for each critical patient
	preemptionResults := scheduler.CheckPreemption(
		h.store.Queue,
		h.store.GetAllPatients(),
		h.store.GetBedsMap(),
		h.store.GetDoctorsMap(),
	)

	for _, pr := range preemptionResults {
		h.store.AddEvent("preemption",
			scheduler.FormatPreemptionMessage(pr),
			"preemption",
			pr,
		)
	}

	if len(preemptionResults) == 0 {
		h.store.AddEvent("simulation.cardiac-cascade.no-preemption",
			"No preemption occurred — either beds were available or no lower-priority patients to preempt",
			"preemption",
			nil,
		)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"scenario":    "cardiac-cascade",
		"concept":     "preemption",
		"added":       len(created),
		"preemptions": preemptionResults,
		"message":     fmt.Sprintf("3 Critical patients added, %d preemption(s) occurred", len(preemptionResults)),
	})
}

// --- POST /api/simulate/race-condition ---
// Fires 2 simultaneous unsafe bed assignments to the same bed using goroutines.

func (h *SimulationHandler) RaceCondition(w http.ResponseWriter, r *http.Request) {
	// Find a free bed
	freeBed := h.store.FindAvailableBed("")
	if freeBed == nil {
		http.Error(w, "no free beds available to demonstrate race condition", http.StatusConflict)
		return
	}

	// Create two patients who will race for the same bed
	id1 := h.store.NextPatientID()
	p1 := models.NewPatient(id1, "Liam Foster", models.Urgent, "Deep cut on hand — needs stitches")
	h.store.AddPatient(p1)
	h.store.Queue.Enqueue(p1)

	id2 := h.store.NextPatientID()
	p2 := models.NewPatient(id2, "Zara Okafor", models.Urgent, "Dislocated shoulder — visible deformity")
	h.store.AddPatient(p2)
	h.store.Queue.Enqueue(p2)

	h.store.AddEvent("simulation.race-condition.start",
		fmt.Sprintf("RACE CONDITION DEMO: %s and %s both trying to claim bed %s simultaneously WITHOUT mutex", p1.Name, p2.Name, freeBed.ID),
		"race-condition",
		map[string]any{
			"bedId":    freeBed.ID,
			"patient1": p1.ID,
			"patient2": p2.ID,
		},
	)

	// Fire two goroutines that both try to assign the same bed without locking
	var wg sync.WaitGroup
	type raceResult struct {
		patientID   string
		patientName string
		success     bool
		err         error
	}

	results := make([]raceResult, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		ok, err := h.store.AssignBedUnsafe(freeBed.ID, p1.ID)
		results[0] = raceResult{p1.ID, p1.Name, ok, err}
	}()
	go func() {
		defer wg.Done()
		ok, err := h.store.AssignBedUnsafe(freeBed.ID, p2.ID)
		results[1] = raceResult{p2.ID, p2.Name, ok, err}
	}()
	wg.Wait()

	// Check who actually got the bed
	bed, _ := h.store.GetBed(freeBed.ID)
	actualOwner := bed.PatientID

	// Both may report success (that's the bug!)
	bothClaimedSuccess := results[0].success && results[1].success

	for _, res := range results {
		if res.success {
			h.store.Queue.Remove(func() *models.Patient {
				p, _ := h.store.GetPatient(res.patientID)
				return p
			}())
		}
	}

	var raceDetected string
	if bothClaimedSuccess {
		raceDetected = fmt.Sprintf("RACE DETECTED: Both %s and %s were told they got bed %s, but only %s actually has it. The other patient's assignment was silently overwritten — a classic lost-update bug.",
			results[0].patientName, results[1].patientName, freeBed.ID, actualOwner)
	} else {
		raceDetected = fmt.Sprintf("Race did not manifest this time (timing-dependent). Patient %s got the bed. In production, this bug would appear intermittently — the worst kind.", actualOwner)
	}

	h.store.AddEvent("simulation.race-condition.result",
		raceDetected,
		"race-condition",
		map[string]any{
			"bedId":              freeBed.ID,
			"actualOwner":       actualOwner,
			"bothClaimedSuccess": bothClaimedSuccess,
			"result1":           map[string]any{"patientId": results[0].patientID, "success": results[0].success},
			"result2":           map[string]any{"patientId": results[1].patientID, "success": results[1].success},
		},
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"scenario":            "race-condition",
		"concept":             "race-condition",
		"bedId":               freeBed.ID,
		"actualOwner":         actualOwner,
		"bothClaimedSuccess":  bothClaimedSuccess,
		"raceDetected":        bothClaimedSuccess,
		"explanation":         raceDetected,
		"patient1Result":      results[0].success,
		"patient2Result":      results[1].success,
	})
}

// --- POST /api/simulate/aging ---
// Artificially ages all waiting patients by 2 hours to demonstrate priority boosting.

func (h *SimulationHandler) Aging(w http.ResponseWriter, r *http.Request) {
	patients := h.store.GetAllPatients()
	waitingCount := 0
	for _, p := range patients {
		if p.Status == models.StatusWaiting {
			waitingCount++
		}
	}

	if waitingCount == 0 {
		http.Error(w, "no waiting patients to age — run rush-hour first", http.StatusBadRequest)
		return
	}

	h.store.AddEvent("simulation.aging.start",
		fmt.Sprintf("AGING SIMULATION: Fast-forwarding 2 hours for %d waiting patients — low-priority patients will get priority boosts to prevent starvation", waitingCount),
		"aging",
		map[string]any{"waitingCount": waitingCount, "artificialAgeMinutes": 120},
	)

	results := scheduler.ApplyAging(h.store.Queue, patients, 2*time.Hour)

	for _, ar := range results {
		h.store.AddEvent("aging.boost",
			fmt.Sprintf("AGING: %s priority boosted from %d to %d (waited %d min) — preventing starvation", ar.PatientName, ar.OldPriority, ar.NewPriority, ar.WaitMinutes),
			"aging",
			ar,
		)
	}

	h.store.AddEvent("simulation.aging.complete",
		scheduler.FormatAgingMessage(results),
		"aging",
		map[string]any{"boostCount": len(results), "results": results},
	)

	// Show new queue order
	queued := h.store.Queue.All()
	queueOrder := make([]string, len(queued))
	for i, p := range queued {
		queueOrder[i] = fmt.Sprintf("%d. %s (%s, pri=%d)", i+1, p.Name, p.TriageLevelName, p.EffectivePri)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"scenario":        "aging",
		"concept":         "aging",
		"patientsAffected": len(results),
		"agingResults":    results,
		"newQueueOrder":   queueOrder,
		"message":         fmt.Sprintf("%d patients had their priority boosted after 2-hour simulated wait", len(results)),
	})
}

// --- POST /api/simulate/preemption ---
// Adds a Critical patient when beds are full with lower-priority patients.
// First fills beds if needed, then adds the Critical patient and triggers preemption.

func (h *SimulationHandler) Preemption(w http.ResponseWriter, r *http.Request) {
	// Step 1: Fill all beds with low-priority patients if not already full
	fillerPatients := []struct {
		Name      string
		Triage    models.TriageLevel
		Complaint string
	}{
		{"Tom Bradley", models.NonUrgent, "Minor headache"},
		{"Nina Petrov", models.SemiUrgent, "Twisted knee while jogging"},
		{"Oscar Mendez", models.NonUrgent, "Paper cut that won't stop bleeding"},
		{"Grace Liu", models.SemiUrgent, "Mild allergic rash on arms"},
		{"Henry Clark", models.NonUrgent, "Requesting flu shot"},
		{"Yuki Sato", models.SemiUrgent, "Back pain from lifting boxes"},
		{"Bella Rossi", models.NonUrgent, "Earache for 2 days"},
		{"Sam Washington", models.SemiUrgent, "Persistent hiccups for 6 hours"},
		{"Ines Dubois", models.NonUrgent, "Splinter in finger"},
		{"Ravi Patel", models.SemiUrgent, "Mild food poisoning symptoms"},
		{"Chloe Anderson", models.NonUrgent, "Sunburn on shoulders"},
		{"Wei Zhang", models.SemiUrgent, "Stiff neck from sleeping wrong"},
		{"Amara Diallo", models.NonUrgent, "Bug bite, slight swelling"},
		{"Jake Murphy", models.SemiUrgent, "Jammed finger playing basketball"},
		{"Leila Khoury", models.NonUrgent, "Cold symptoms, wants to be checked"},
		{"Dmitri Orlov", models.SemiUrgent, "Bruised rib from minor fall"},
		{"Sofia Reyes", models.NonUrgent, "Wants prescription refill"},
	}
	fillerIdx := 0

	h.store.AddEvent("simulation.preemption.start",
		"PREEMPTION DEMO: Filling beds with low-priority patients, then admitting a Critical patient",
		"preemption",
		nil,
	)

	// Assign filler patients to all available beds
	bedsAssigned := 0
	for {
		bed := h.store.FindAvailableBed("")
		if bed == nil || fillerIdx >= len(fillerPatients) {
			break
		}

		fp := fillerPatients[fillerIdx]
		fillerIdx++

		id := h.store.NextPatientID()
		p := models.NewPatient(id, fp.Name, fp.Triage, fp.Complaint)
		h.store.AddPatient(p)

		ok, _ := h.store.AssignBedSafe(bed.ID, p.ID)
		if ok {
			// Mark as in-treatment so preemption can find them
			p.Status = models.StatusInTreatment
			bedsAssigned++

			// Try to assign a doctor
			doc := h.store.FindAvailableDoctor()
			if doc != nil {
				_ = h.store.AssignDoctor(doc.ID, p.ID)
			}

			h.store.AddEvent("bed.assigned",
				fmt.Sprintf("[Preemption Setup] %s (%s) assigned to bed %s — filling beds with low-priority patients", p.Name, p.TriageLevelName, bed.ID),
				"preemption",
				map[string]any{"patientId": p.ID, "bedId": bed.ID},
			)
		}
	}

	// Step 2: Now add a Critical patient
	critID := h.store.NextPatientID()
	critPatient := models.NewPatient(critID, "Kenji Nakamura", models.Critical, "Massive internal bleeding — motorcycle accident, losing consciousness")
	h.store.AddPatient(critPatient)
	h.store.Queue.Enqueue(critPatient)

	h.store.AddEvent("patient.checkin.critical",
		fmt.Sprintf("CRITICAL ARRIVAL: %s — %s. All %d beds occupied by lower-priority patients. Preemption needed!",
			critPatient.Name, critPatient.Complaint, bedsAssigned),
		"preemption",
		map[string]any{"patientId": critPatient.ID, "bedsOccupied": bedsAssigned},
	)

	// Step 3: Run preemption
	preemptionResults := scheduler.CheckPreemption(
		h.store.Queue,
		h.store.GetAllPatients(),
		h.store.GetBedsMap(),
		h.store.GetDoctorsMap(),
	)

	for _, pr := range preemptionResults {
		h.store.AddEvent("preemption",
			scheduler.FormatPreemptionMessage(pr),
			"preemption",
			pr,
		)
	}

	summary := fmt.Sprintf("Preemption demo complete: filled %d beds, added Critical patient %s, %d preemption(s) occurred",
		bedsAssigned, critPatient.Name, len(preemptionResults))

	h.store.AddEvent("simulation.preemption.complete", summary, "preemption",
		map[string]any{
			"bedsFilled":  bedsAssigned,
			"preemptions": len(preemptionResults),
		},
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"scenario":    "preemption",
		"concept":     "preemption",
		"bedsFilled":  bedsAssigned,
		"preemptions": preemptionResults,
		"message":     summary,
	})
}

// --- GET /api/events ---
// Returns the full event log.

func (h *SimulationHandler) Events(w http.ResponseWriter, r *http.Request) {
	events := h.store.GetEvents()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"events": events,
		"count":  len(events),
	})
}

// --- POST /api/simulate/reset ---
// Clears all state and reinitializes the ER.

func (h *SimulationHandler) Reset(w http.ResponseWriter, r *http.Request) {
	h.store.Reset()

	h.store.AddEvent("system.reset",
		"System reset — all patients discharged, all beds freed, all queues cleared. Fresh start.",
		"resource-management",
		nil,
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message": "ER system reset to initial state",
	})
}
