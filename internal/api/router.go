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
	bh := &BedHandler{store: s}
	sh := &SimulationHandler{store: s}

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

		// Bed routes — safe (mutex) and unsafe (racy) assignment
		r.Route("/beds", func(r chi.Router) {
			r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(s.GetAllBeds())
			})
			r.Post("/{id}/assign", bh.AssignSafe)
			r.Post("/{id}/assign-unsafe", bh.AssignUnsafe)
			r.Post("/{id}/release", bh.Release)
		})

		// Doctors — returns current state
		r.Get("/doctors", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(s.GetAllDoctors())
		})

		// Events — the OS audit log
		r.Get("/events", sh.Events)

		// Simulation endpoints — trigger OS concept demos
		r.Route("/simulate", func(r chi.Router) {
			r.Post("/rush-hour", sh.RushHour)
			r.Post("/cardiac-cascade", sh.CardiacCascade)
			r.Post("/race-condition", sh.RaceCondition)
			r.Post("/aging", sh.Aging)
			r.Post("/preemption", sh.Preemption)
			r.Post("/reset", sh.Reset)
		})

		// System state — full snapshot
		r.Get("/system/state", ph.SystemState)
	})

	return r
}
