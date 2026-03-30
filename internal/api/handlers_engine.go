package api

import (
	"encoding/json"
	"net/http"

	"github.com/erflow/backend/internal/engine"
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
