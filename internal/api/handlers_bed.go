package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/erflow/backend/internal/store"
)

// =============================================================================
// OS CONCEPT: MUTUAL EXCLUSION (mutex) and RACE CONDITIONS
// =============================================================================
//
// A RACE CONDITION happens when two threads access shared data concurrently
// and at least one of them writes. The result depends on the exact interleaving
// of instructions — it's non-deterministic and almost always a bug.
//
// A MUTEX (mutual exclusion lock) prevents this. Only one thread can hold the
// lock at a time. Others block until it's released. The "critical section"
// (check-if-free then mark-as-occupied) runs atomically.
//
// The safe endpoint uses the mutex. The unsafe endpoint deliberately skips it,
// introducing a TOCTOU (Time-of-Check-to-Time-of-Use) vulnerability:
//   Thread A: reads bed.Occupied == false
//   Thread B: reads bed.Occupied == false  (same bed!)
//   Thread A: sets bed.Occupied = true, assigns patient A
//   Thread B: sets bed.Occupied = true, assigns patient B  ← overwrites A!
//
// Patient A thinks they have a bed, but patient B actually has it. Classic race.
// =============================================================================

type BedHandler struct {
	store *store.MemStore
}

type AssignBedRequest struct {
	PatientID string `json:"patientId"`
}

// AssignSafe handles POST /api/beds/{id}/assign — mutex-protected bed assignment.
func (h *BedHandler) AssignSafe(w http.ResponseWriter, r *http.Request) {
	bedID := chi.URLParam(r, "id")

	var req AssignBedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.PatientID == "" {
		http.Error(w, "patientId is required", http.StatusBadRequest)
		return
	}

	patient, ok := h.store.GetPatient(req.PatientID)
	if !ok {
		http.Error(w, "patient not found", http.StatusNotFound)
		return
	}

	success, err := h.store.AssignBedSafe(bedID, req.PatientID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if !success {
		h.store.AddEvent("bed.assign.blocked", fmt.Sprintf("Bed %s was already occupied when %s tried to claim it — mutex prevented double-assignment", bedID, patient.Name), "mutex", map[string]string{
			"bedId":     bedID,
			"patientId": req.PatientID,
			"result":    "blocked",
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "bed already occupied",
			"concept": "mutex — another thread acquired the lock first",
		})
		return
	}

	// Remove from the priority queue since they are now assigned
	h.store.Queue.Remove(patient)

	// Try to assign a doctor too
	doc := h.store.FindAvailableDoctor()
	docName := "none available"
	if doc != nil {
		_ = h.store.AssignDoctor(doc.ID, req.PatientID)
		docName = doc.Name
	}

	h.store.AddEvent("bed.assigned", fmt.Sprintf("Bed %s assigned to %s (mutex held — atomic check-and-set). Doctor: %s", bedID, patient.Name, docName), "mutex", map[string]any{
		"bedId":     bedID,
		"patientId": req.PatientID,
		"doctorId":  docIDOrEmpty(doc),
		"safe":      true,
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"bedId":   bedID,
		"concept": "mutex — critical section protected by lock",
	})
}

// AssignUnsafe handles POST /api/beds/{id}/assign-unsafe — deliberately racy.
func (h *BedHandler) AssignUnsafe(w http.ResponseWriter, r *http.Request) {
	bedID := chi.URLParam(r, "id")

	var req AssignBedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.PatientID == "" {
		http.Error(w, "patientId is required", http.StatusBadRequest)
		return
	}

	patient, ok := h.store.GetPatient(req.PatientID)
	if !ok {
		http.Error(w, "patient not found", http.StatusNotFound)
		return
	}

	h.store.AddEvent("bed.assign.unsafe", fmt.Sprintf("UNSAFE assignment attempted: %s trying bed %s WITHOUT mutex — race condition possible!", patient.Name, bedID), "race-condition", map[string]any{
		"bedId":     bedID,
		"patientId": req.PatientID,
		"safe":      false,
	})

	success, err := h.store.AssignBedUnsafe(bedID, req.PatientID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if success {
		h.store.Queue.Remove(patient)
	}

	h.store.AddEvent("bed.assign.unsafe.result", fmt.Sprintf("UNSAFE assignment result for %s on bed %s: success=%v (may have overwritten another patient!)", patient.Name, bedID, success), "race-condition", map[string]any{
		"bedId":     bedID,
		"patientId": req.PatientID,
		"success":   success,
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": success,
		"bedId":   bedID,
		"concept": "race-condition — NO mutex, TOCTOU vulnerability",
		"warning": "This assignment may have overwritten another concurrent assignment!",
	})
}

// Release handles POST /api/beds/{id}/release — frees a bed.
func (h *BedHandler) Release(w http.ResponseWriter, r *http.Request) {
	bedID := chi.URLParam(r, "id")

	released, err := h.store.ReleaseBed(bedID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msg := fmt.Sprintf("Bed %s released", bedID)
	if released != nil {
		msg = fmt.Sprintf("Bed %s released — %s returned to waiting queue", bedID, released.Name)
		h.store.Queue.Enqueue(released)
	}

	h.store.AddEvent("bed.released", msg, "resource-management", map[string]any{
		"bedId":     bedID,
		"patientId": patientIDOrEmpty(released),
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"success": "true",
		"message": msg,
	})
}

func safeDocID(d *models.Doctor) string {
	if d == nil {
		return ""
	}
	return d.ID
}

func safePatientID(p *models.Patient) string {
	if p == nil {
		return ""
	}
	return p.ID
}
