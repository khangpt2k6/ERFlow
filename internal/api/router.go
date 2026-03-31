package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/erflow/backend/internal/engine"
	"github.com/erflow/backend/internal/store"
)

func NewRouter(s *store.MemStore, eng *engine.Engine) *chi.Mux {
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
	bh := &BedHandler{store: s}
	sh := &SimulationHandler{store: s}
	eh := &EngineHandler{engine: eng}

	r.Route("/api", func(r chi.Router) {
		// Patient routes
		r.Route("/patients", func(r chi.Router) {
			r.Post("/", ph.CheckIn)
			r.Get("/", ph.List)
			r.Get("/queue", ph.Queue)
			r.Get("/{id}", ph.Get)
			r.Patch("/{id}", ph.Update)
			r.Delete("/{id}", ph.Discharge)
		})

		// Bed routes
		r.Route("/beds", func(r chi.Router) {
			r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(s.GetAllBeds())
			})
			r.Post("/{id}/assign", bh.AssignSafe)
			r.Post("/{id}/assign-unsafe", bh.AssignUnsafe)
			r.Post("/{id}/release", bh.Release)
		})

		// Doctors
		r.Get("/doctors", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(s.GetAllDoctors())
		})

		// Events
		r.Get("/events", sh.Events)

		// Simulation scenarios
		r.Route("/simulate", func(r chi.Router) {
			r.Post("/rush-hour", sh.RushHour)
			r.Post("/cardiac-cascade", sh.CardiacCascade)
			r.Post("/race-condition", sh.RaceCondition)
			r.Post("/aging", sh.Aging)
			r.Post("/preemption", sh.Preemption)
			r.Post("/semaphore", sh.Semaphore)
			r.Post("/reset", sh.Reset)
		})

		// Engine controls (auto-simulation)
		r.Route("/engine", func(r chi.Router) {
			r.Post("/start", eh.Start)
			r.Post("/stop", eh.Stop)
			r.Post("/speed", eh.Speed)
			r.Get("/status", eh.Status)
			r.Post("/scheduler", eh.SetScheduler)
			r.Get("/scheduler", eh.GetScheduler)
		})

		// System state — includes engine status
		r.Get("/system/state", func(w http.ResponseWriter, _ *http.Request) {
			patients := s.GetAllPatients()
			// Count by status
			var waiting, treating, discharged int
			for _, p := range patients {
				switch p.Status {
				case "waiting":
					waiting++
				case "assigned", "in-treatment":
					treating++
				case "discharged":
					discharged++
				}
			}
			state := map[string]any{
				"patients":   patients,
				"beds":       s.GetAllBeds(),
				"doctors":    s.GetAllDoctors(),
				"queue":      s.Queue.All(),
				"queueLen":   s.Queue.Len(),
				"events":     s.GetEvents(),
				"engine":     eng.Stats(),
				"semaphores": s.SemaphoreStats(),
				"counts": map[string]int{
					"waiting":    waiting,
					"treating":   treating,
					"discharged": discharged,
					"total":      len(patients),
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(state)
		})
	})

	return r
}
