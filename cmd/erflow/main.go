package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/erflow/backend/internal/api"
	"github.com/erflow/backend/internal/engine"
	"github.com/erflow/backend/internal/metrics"
	"github.com/erflow/backend/internal/store"
)

func main() {
	fmt.Println("=== ERFlow — ER Management System ===")
	fmt.Println("OS concepts powering real hospital decisions")
	fmt.Println()

	memStore := store.NewMemStore()
	fmt.Printf("Initialized: %d beds, %d doctors\n", len(memStore.GetAllBeds()), len(memStore.GetAllDoctors()))

	eng := engine.New(memStore)
	fmt.Println("Engine ready (POST /api/engine/start to begin auto-simulation)")

	// Initialize Prometheus metrics for default scheduler and bed counts
	metrics.SetActiveScheduler("priority")
	metrics.BedsTotal.WithLabelValues("general").Set(10)
	metrics.BedsTotal.WithLabelValues("icu").Set(5)
	metrics.BedsTotal.WithLabelValues("trauma").Set(2)

	router := api.NewRouter(memStore, eng)

	fmt.Println()
	fmt.Println("Server:  http://localhost:8080")
	fmt.Println("State:   http://localhost:8080/api/system/state")
	fmt.Println("Metrics: http://localhost:8080/metrics")
	fmt.Println()

	if err := http.ListenAndServe(":8080", router); err != nil {
		log.Fatal(err)
	}
}
