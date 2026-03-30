package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/erflow/backend/internal/api"
	"github.com/erflow/backend/internal/store"
)

func main() {
	fmt.Println("🏥 ERFlow — ER Management System")
	fmt.Println("OS concepts powering real hospital decisions")
	fmt.Println()

	// Initialize the in-memory store (the kernel's process table + device table)
	memStore := store.NewMemStore()

	fmt.Printf("Initialized: %d beds, %d doctors\n",
		len(memStore.GetAllBeds()), len(memStore.GetAllDoctors()))

	// Set up the HTTP router
	router := api.NewRouter(memStore)

	fmt.Println()
	fmt.Println("Server running on http://localhost:8080")
	fmt.Println("API: http://localhost:8080/api/system/state")

	if err := http.ListenAndServe(":8080", router); err != nil {
		log.Fatal(err)
	}
}
