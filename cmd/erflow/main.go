package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/erflow/backend/internal/api"
	"github.com/erflow/backend/internal/engine"
	"github.com/erflow/backend/internal/store"
)

func main() {
	fmt.Println("=== ERFlow — ER Management System ===")
	fmt.Println("OS concepts powering real hospital decisions")
	fmt.Println()

	// Initialize the in-memory store (kernel's process table + device table)
	memStore := store.NewMemStore()
	fmt.Printf("Initialized: %d beds, %d doctors\n", len(memStore.GetAllBeds()), len(memStore.GetAllDoctors()))

	// Initialize the simulation engine (the kernel's background daemons)
	eng := engine.New(memStore)
	fmt.Println("Engine ready (POST /api/engine/start to begin auto-simulation)")

	// Set up HTTP router
	router := api.NewRouter(memStore, eng)

	fmt.Println()
	fmt.Println("Server: http://localhost:8080")
	fmt.Println("State:  http://localhost:8080/api/system/state")
	fmt.Println()

	if err := http.ListenAndServe(":8080", router); err != nil {
		log.Fatal(err)
	}
}
