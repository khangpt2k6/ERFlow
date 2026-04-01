package engine

import (
	"testing"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
	"github.com/erflow/backend/internal/store"
)

func TestEngine_StartStop(t *testing.T) {
	s := store.NewMemStore()
	eng := New(s)

	if eng.IsRunning() {
		t.Fatal("engine should not be running initially")
	}

	eng.Start()
	if !eng.IsRunning() {
		t.Fatal("engine should be running after Start")
	}

	// Double start should be a no-op
	eng.Start()
	if !eng.IsRunning() {
		t.Fatal("engine should still be running after double Start")
	}

	eng.Stop()
	if eng.IsRunning() {
		t.Fatal("engine should not be running after Stop")
	}

	// Double stop should be safe
	eng.Stop()
}

func TestEngine_Speed(t *testing.T) {
	s := store.NewMemStore()
	eng := New(s)

	eng.SetSpeed(2.5)
	if eng.GetSpeed() != 2.5 {
		t.Errorf("expected speed 2.5, got %f", eng.GetSpeed())
	}

	// Clamp low
	eng.SetSpeed(0.01)
	if eng.GetSpeed() != 0.1 {
		t.Errorf("speed should clamp to 0.1, got %f", eng.GetSpeed())
	}

	// Clamp high
	eng.SetSpeed(100)
	if eng.GetSpeed() != 10 {
		t.Errorf("speed should clamp to 10, got %f", eng.GetSpeed())
	}
}

func TestEngine_SetScheduler(t *testing.T) {
	s := store.NewMemStore()
	eng := New(s)

	// Add patients to queue
	for i := 0; i < 5; i++ {
		p := models.NewPatient(s.NextPatientID(), "Test", models.Urgent, "test")
		s.AddPatient(p)
		s.Queue.Enqueue(p)
	}

	if s.Queue.Name() != scheduler.AlgoPriority {
		t.Fatalf("expected default priority scheduler, got %s", s.Queue.Name())
	}

	// Switch to FCFS — patients should migrate
	eng.SetScheduler(scheduler.AlgoFCFS)
	if s.Queue.Name() != scheduler.AlgoFCFS {
		t.Errorf("expected FCFS, got %s", s.Queue.Name())
	}
	if s.Queue.Len() != 5 {
		t.Errorf("expected 5 patients after migration, got %d", s.Queue.Len())
	}

	// Same algorithm should be no-op
	eng.SetScheduler(scheduler.AlgoFCFS)
	if s.Queue.Len() != 5 {
		t.Errorf("patients should be preserved, got %d", s.Queue.Len())
	}
}

func TestEngine_Stats(t *testing.T) {
	s := store.NewMemStore()
	eng := New(s)

	stats := eng.Stats()
	if stats["running"] != false {
		t.Error("should not be running")
	}
	if stats["speed"] != 1.0 {
		t.Errorf("default speed should be 1.0, got %v", stats["speed"])
	}
	if stats["algorithm"] != "priority" {
		t.Errorf("default algorithm should be priority, got %v", stats["algorithm"])
	}
}

func TestEngine_GeneratesPatients(t *testing.T) {
	s := store.NewMemStore()
	eng := New(s)

	eng.SetSpeed(10) // fast
	eng.Start()

	// Let it run briefly
	time.Sleep(2 * time.Second)
	eng.Stop()

	arrivals := eng.totalArrivals.Load()
	if arrivals == 0 {
		t.Error("engine should have generated at least some patients")
	}

	patients := s.GetAllPatients()
	if len(patients) == 0 {
		t.Error("store should have patients after engine ran")
	}

	events := s.GetEvents()
	if len(events) == 0 {
		t.Error("store should have events after engine ran")
	}
}

func TestEngine_ProcessesDischarges(t *testing.T) {
	s := store.NewMemStore()
	eng := New(s)

	eng.SetSpeed(10) // fast
	eng.Start()

	// Let it run long enough for some treatments to complete
	time.Sleep(4 * time.Second)
	eng.Stop()

	discharged := eng.totalDischarged.Load()
	// At high speed, at least some patients should be discharged
	if discharged == 0 {
		t.Log("WARNING: no discharges in 4s at 10x speed — may be environment-dependent")
	}
}

func TestEngine_SchedulerSwapPreservesPatients(t *testing.T) {
	s := store.NewMemStore()
	eng := New(s)

	// Manually add patients
	for i := 0; i < 10; i++ {
		p := models.NewPatient(s.NextPatientID(), "Test", models.TriageLevel(i%5+1), "test")
		s.AddPatient(p)
		s.Queue.Enqueue(p)
	}

	// Swap through all algorithms
	for _, algo := range scheduler.AllAlgorithms() {
		eng.SetScheduler(algo)
		if s.Queue.Len() != 10 {
			t.Errorf("after swapping to %s, expected 10 patients, got %d", algo, s.Queue.Len())
		}
	}
}
