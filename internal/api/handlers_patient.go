package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/store"
)

type PatientHandler struct {
	store *store.MemStore
}

// CheckInRequest is what the coordinator sends to check in a new patient.
type CheckInRequest struct {
	Name        string `json:"name"`
	TriageLevel int    `json:"triageLevel"` // 1-5
	Complaint   string `json:"complaint"`
}

// CheckIn adds a new patient to the ER and inserts them into the priority queue.
//
// OS parallel: This is like a new process being created (fork/exec) and inserted
// into the scheduler's ready queue. The process doesn't run immediately — it waits
// in the queue until the scheduler picks it based on priority.
func (h *PatientHandler) CheckIn(w http.ResponseWriter, r *http.Request) {
	var req CheckInRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if req.TriageLevel < 1 || req.TriageLevel > 5 {
		http.Error(w, "triageLevel must be 1-5", http.StatusBadRequest)
		return
	}

	id := h.store.NextPatientID()
	patient := models.NewPatient(id, req.Name, models.TriageLevel(req.TriageLevel), req.Complaint)

	// Add to the store (process table) and the priority queue (ready queue)
	h.store.AddPatient(patient)
	h.store.Queue.Enqueue(patient)

	// Emit an event — every check-in is a new process entering the ready queue
	h.store.AddEvent("patient.checkin",
		fmt.Sprintf("%s checked in — Triage: %s (priority %d). Queued for scheduling.", patient.Name, patient.TriageLevelName, patient.EffectivePri),
		"priority-scheduling",
		map[string]any{
			"patientId":   patient.ID,
			"triageLevel": int(patient.TriageLevel),
			"priority":    patient.EffectivePri,
			"complaint":   patient.Complaint,
		},
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(patient)
}

// List returns all patients in the ER (all statuses).
func (h *PatientHandler) List(w http.ResponseWriter, r *http.Request) {
	patients := h.store.GetAllPatients()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(patients)
}

// Queue returns patients in the waiting queue, sorted by effective priority.
// This is the scheduler's view — who gets seen next?
func (h *PatientHandler) Queue(w http.ResponseWriter, r *http.Request) {
	queued := h.store.Queue.All()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(queued)
}

// Get returns a single patient by ID.
func (h *PatientHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	patient, ok := h.store.GetPatient(id)
	if !ok {
		http.Error(w, "patient not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(patient)
}

// Update modifies a patient (e.g., triage override).
func (h *PatientHandler) Update(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	patient, ok := h.store.GetPatient(id)
	if !ok {
		http.Error(w, "patient not found", http.StatusNotFound)
		return
	}

	var updates struct {
		TriageLevel *int    `json:"triageLevel,omitempty"`
		Status      *string `json:"status,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if updates.TriageLevel != nil {
		tl := models.TriageLevel(*updates.TriageLevel)
		patient.TriageLevel = tl
		patient.TriageLevelName = tl.String()
		patient.EffectivePri = *updates.TriageLevel * 100
		// Re-sort in the priority queue — priority changed
		h.store.Queue.Update(patient)
	}
	if updates.Status != nil {
		patient.Status = models.PatientStatus(*updates.Status)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(patient)
}

// Discharge removes a patient from the ER.
// OS parallel: process termination — free all resources, remove from all queues.
func (h *PatientHandler) Discharge(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	patient, ok := h.store.GetPatient(id)
	if !ok {
		http.Error(w, "patient not found", http.StatusNotFound)
		return
	}

	patient.Status = models.StatusDischarged
	h.store.Queue.Remove(patient)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(patient)
}

// SystemState returns the full ER snapshot — all patients, beds, doctors, queue.
func (h *PatientHandler) SystemState(w http.ResponseWriter, r *http.Request) {
	state := map[string]interface{}{
		"patients": h.store.GetAllPatients(),
		"beds":     h.store.GetAllBeds(),
		"doctors":  h.store.GetAllDoctors(),
		"queue":    h.store.Queue.All(),
		"queueLen": h.store.Queue.Len(),
		"events":   h.store.GetEvents(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}
