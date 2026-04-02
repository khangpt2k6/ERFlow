package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/store"
)

type BedHandler struct {
	store *store.MemStore
}

type AssignBedRequest struct {
	PatientID string `json:"patientId"`
}

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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "bed already occupied"})
		return
	}

	h.store.Queue.Remove(patient)
	doc := h.store.FindAvailableDoctor()
	if doc != nil {
		_ = h.store.AssignDoctor(doc.ID, req.PatientID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"success": true, "bedId": bedID})
}

func (h *BedHandler) Release(w http.ResponseWriter, r *http.Request) {
	bedID := chi.URLParam(r, "id")

	released, err := h.store.ReleaseBed(bedID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msg := fmt.Sprintf("Bed %s released", bedID)
	if released != nil {
		msg = fmt.Sprintf("Bed %s released — %s returned to queue", bedID, released.Name)
		h.store.Queue.Enqueue(released)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": msg})
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
