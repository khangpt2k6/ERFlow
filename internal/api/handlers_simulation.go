package api

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/store"
)

type SimulationHandler struct {
	store *store.MemStore
}

// POST /api/simulate/rush-hour — adds 10 patients of mixed triage levels.
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

	rand.Shuffle(len(incoming), func(i, j int) {
		incoming[i], incoming[j] = incoming[j], incoming[i]
	})

	h.store.AddEvent("simulation.rush-hour.start",
		"RUSH HOUR: 10 patients arriving — scheduler must triage them all",
		"scaling",
		map[string]int{"patientCount": len(incoming)},
	)

	var created []*models.Patient
	for _, rp := range incoming {
		id := h.store.NextPatientID()
		p := models.NewPatient(id, rp.Name, rp.Triage, rp.Complaint)
		h.store.AddPatient(p)
		h.store.Queue.Enqueue(p)
		created = append(created, p)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"scenario": "rush-hour",
		"added":    len(created),
		"message":  "10 patients admitted — priority queue sorted them by triage severity",
	})
}

// POST /api/simulate/stress — floods N patients to stress the system.
func (h *SimulationHandler) Stress(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Count <= 0 {
		req.Count = 50
	}
	if req.Count > 1000 {
		req.Count = 1000
	}

	h.store.AddEvent("simulation.stress.start",
		fmt.Sprintf("STRESS TEST: Adding %d patients", req.Count),
		"scaling",
		map[string]int{"count": req.Count},
	)

	names := []string{
		"Alex Morgan", "Jamie Lee", "Pat Quinn", "Chris Stone", "Sam Rivers",
		"Jordan Banks", "Casey Drew", "Riley Fox", "Avery Cole", "Quinn Hart",
		"Taylor West", "Morgan Blake", "Dakota Ray", "Skyler James", "Drew Park",
	}
	complaints := []string{
		"Headache", "Sprain", "Fever", "Cough", "Back pain",
		"Stomach ache", "Skin rash", "Dizziness", "Sore throat", "Fatigue",
	}

	for i := 0; i < req.Count; i++ {
		triage := models.SemiUrgent
		if i%5 == 0 {
			triage = models.Urgent
		}
		if i%10 == 0 {
			triage = models.Emergency
		}
		if i%50 == 0 {
			triage = models.Critical
		}

		id := h.store.NextPatientID()
		p := models.NewPatient(id, names[i%len(names)], triage, complaints[i%len(complaints)])
		h.store.AddPatient(p)
		h.store.Queue.Enqueue(p)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"scenario": "stress",
		"added":    req.Count,
		"message":  fmt.Sprintf("%d patients added — watch the auto-scaler react", req.Count),
	})
}

func (h *SimulationHandler) Events(w http.ResponseWriter, r *http.Request) {
	events := h.store.GetEvents()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"events": events,
		"count":  len(events),
	})
}

func (h *SimulationHandler) Reset(w http.ResponseWriter, r *http.Request) {
	h.store.Reset()
	h.store.AddEvent("system.reset", "System reset — fresh start", "scaling", nil)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "ER system reset"})
}
