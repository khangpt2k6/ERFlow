package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/erflow/backend/internal/store"
)

func NewRouter(s *store.MemStore) *chi.Mux {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"http://localhost:5173"},
		AllowedMethods:   []string{"GET", "POST", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type"},
		AllowCredentials: true,
	}))

	ph := &PatientHandler{store: s}

	r.Route("/api", func(r chi.Router) {
		// Patient routes — Phase 1
		r.Route("/patients", func(r chi.Router) {
			r.Post("/", ph.CheckIn)
			r.Get("/", ph.List)
			r.Get("/queue", ph.Queue)
			r.Get("/{id}", ph.Get)
			r.Patch("/{id}", ph.Update)
			r.Delete("/{id}", ph.Discharge)
		})

		// Beds — returns current state
		r.Get("/beds", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(s.GetAllBeds())
		})

		// Doctors — returns current state
		r.Get("/doctors", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(s.GetAllDoctors())
		})

		// System state — full snapshot
		r.Get("/system/state", ph.SystemState)
	})

	return r
}
