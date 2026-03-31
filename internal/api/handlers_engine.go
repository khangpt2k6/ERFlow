package api

import (
	"encoding/json"
	"net/http"

	"github.com/erflow/backend/internal/engine"
	"github.com/erflow/backend/internal/scheduler"
)

type EngineHandler struct {
	engine *engine.Engine
}

func (h *EngineHandler) Start(w http.ResponseWriter, r *http.Request) {
	h.engine.Start()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.engine.Stats())
}

func (h *EngineHandler) Stop(w http.ResponseWriter, r *http.Request) {
	h.engine.Stop()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.engine.Stats())
}

func (h *EngineHandler) Speed(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Speed float64 `json:"speed"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	h.engine.SetSpeed(req.Speed)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.engine.Stats())
}

func (h *EngineHandler) Status(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.engine.Stats())
}

// SetScheduler handles POST /api/engine/scheduler — switches the scheduling algorithm.
func (h *EngineHandler) SetScheduler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Algorithm string `json:"algorithm"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	algo := scheduler.Algorithm(req.Algorithm)
	valid := false
	for _, a := range scheduler.AllAlgorithms() {
		if a == algo {
			valid = true
			break
		}
	}
	if !valid {
		http.Error(w, "invalid algorithm: "+req.Algorithm, http.StatusBadRequest)
		return
	}

	h.engine.SetScheduler(algo)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.engine.Stats())
}

// GetScheduler handles GET /api/engine/scheduler — returns current algorithm and available options.
func (h *EngineHandler) GetScheduler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"algorithm": string(h.engine.GetScheduler()),
		"available": scheduler.AllAlgorithms(),
	})
}
