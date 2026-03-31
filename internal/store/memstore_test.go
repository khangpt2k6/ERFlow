package store

import (
	"sync"
	"testing"

	"github.com/erflow/backend/internal/models"
)

func TestAssignBedSafe(t *testing.T) {
	s := NewMemStore()

	id := s.NextPatientID()
	p := models.NewPatient(id, "Test", models.Urgent, "test")
	s.AddPatient(p)

	ok, err := s.AssignBedSafe("bed-g1", p.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected successful assignment")
	}

	// Second assign to same bed should fail
	id2 := s.NextPatientID()
	p2 := models.NewPatient(id2, "Test2", models.Urgent, "test")
	s.AddPatient(p2)

	ok2, err2 := s.AssignBedSafe("bed-g1", p2.ID)
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	if ok2 {
		t.Fatal("should not assign occupied bed")
	}
}

func TestAssignBedSafe_NotFound(t *testing.T) {
	s := NewMemStore()
	_, err := s.AssignBedSafe("nonexistent", "p1")
	if err == nil {
		t.Fatal("expected error for nonexistent bed")
	}
}

func TestReleaseBed(t *testing.T) {
	s := NewMemStore()

	id := s.NextPatientID()
	p := models.NewPatient(id, "Test", models.Urgent, "test")
	s.AddPatient(p)
	s.AssignBedSafe("bed-icu1", p.ID)

	released, err := s.ReleaseBed("bed-icu1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if released == nil {
		t.Fatal("expected released patient")
	}
	if released.ID != p.ID {
		t.Errorf("expected %s, got %s", p.ID, released.ID)
	}

	// Bed should be free now
	bed, _ := s.GetBed("bed-icu1")
	if bed.Occupied {
		t.Fatal("bed should be free after release")
	}
}

func TestReset(t *testing.T) {
	s := NewMemStore()

	p := models.NewPatient("p1", "Test", models.Urgent, "test")
	s.AddPatient(p)
	s.Queue.Enqueue(p)

	s.Reset()

	patients := s.GetAllPatients()
	if len(patients) != 0 {
		t.Errorf("expected 0 patients after reset, got %d", len(patients))
	}
	if s.Queue.Len() != 0 {
		t.Errorf("expected empty queue after reset, got %d", s.Queue.Len())
	}
}

func TestConcurrentOperations(t *testing.T) {
	s := NewMemStore()
	var wg sync.WaitGroup

	// Concurrent reads and writes
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			id := s.NextPatientID()
			p := models.NewPatient(id, "Test", models.TriageLevel(n%5+1), "test")
			s.AddPatient(p)
		}(i)
		go func() {
			defer wg.Done()
			_ = s.GetAllPatients()
			_ = s.GetAllBeds()
			_ = s.GetAllDoctors()
		}()
	}

	wg.Wait()
	// No panic under -race = pass
}

func TestSemaphoreStats(t *testing.T) {
	s := NewMemStore()
	stats := s.SemaphoreStats()
	if len(stats) != 3 {
		t.Fatalf("expected 3 semaphores, got %d", len(stats))
	}

	// Check initial capacities
	for _, sem := range stats {
		switch sem.Name {
		case "General":
			if sem.Capacity != 10 {
				t.Errorf("General capacity should be 10, got %d", sem.Capacity)
			}
		case "ICU":
			if sem.Capacity != 5 {
				t.Errorf("ICU capacity should be 5, got %d", sem.Capacity)
			}
		case "Trauma":
			if sem.Capacity != 2 {
				t.Errorf("Trauma capacity should be 2, got %d", sem.Capacity)
			}
		}
	}
}
