package scheduler

import (
	"testing"
	"time"

	"github.com/erflow/backend/internal/models"
)

func TestFCFS_Order(t *testing.T) {
	q := NewFCFSQueue()

	p1 := makePatient("p1", models.NonUrgent)   // low priority
	p2 := makePatient("p2", models.Critical)     // high priority, arrives second

	q.Enqueue(p1)
	q.Enqueue(p2)

	// FCFS ignores priority — first in, first out
	got := q.Dequeue()
	if got.ID != "p1" {
		t.Errorf("FCFS should return p1 first (arrived first), got %s", got.ID)
	}
	got = q.Dequeue()
	if got.ID != "p2" {
		t.Errorf("FCFS should return p2 second, got %s", got.ID)
	}
}

func TestSJF_Order(t *testing.T) {
	q := NewSJFQueue()

	p1 := makePatient("p1", models.Critical)     // 20s estimated
	p2 := makePatient("p2", models.NonUrgent)    // 6s estimated
	p3 := makePatient("p3", models.Urgent)       // 11s estimated

	q.Enqueue(p1)
	q.Enqueue(p2)
	q.Enqueue(p3)

	// SJF should pick shortest duration first: NonUrgent (6s) < Urgent (11s) < Critical (20s)
	got := q.Dequeue()
	if got.ID != "p2" {
		t.Errorf("SJF should pick p2 (shortest), got %s (est=%v)", got.ID, got.EstimatedDuration)
	}
	got = q.Dequeue()
	if got.ID != "p3" {
		t.Errorf("SJF should pick p3 next, got %s", got.ID)
	}
	got = q.Dequeue()
	if got.ID != "p1" {
		t.Errorf("SJF should pick p1 last (longest), got %s", got.ID)
	}
}

func TestRoundRobin_Order(t *testing.T) {
	q := NewRoundRobinQueue()

	p1 := makePatient("p1", models.Critical)
	p2 := makePatient("p2", models.NonUrgent)

	q.Enqueue(p1)
	q.Enqueue(p2)

	// Round Robin is FIFO for initial dequeue
	got := q.Dequeue()
	if got.ID != "p1" {
		t.Errorf("RR should return p1 first, got %s", got.ID)
	}

	// Re-enqueue p1 (simulating quantum expiry)
	q.Enqueue(p1)

	got = q.Dequeue()
	if got.ID != "p2" {
		t.Errorf("RR should return p2 next, got %s", got.ID)
	}
	got = q.Dequeue()
	if got.ID != "p1" {
		t.Errorf("RR should return p1 again, got %s", got.ID)
	}
}

func TestMLFQ_Demotion(t *testing.T) {
	m := NewMLFQ()

	p := makePatient("p1", models.Urgent)
	p.MLFQLevel = 0 // starts at highest queue

	m.Enqueue(p)
	if m.Len() != 1 {
		t.Fatal("expected len 1")
	}

	// Dequeue from Q0
	got := m.Dequeue()
	if got.ID != "p1" {
		t.Fatal("expected p1")
	}

	// Demote to Q1
	p.MLFQLevel = 1
	m.Enqueue(p)

	// Add a fresh Q0 patient
	p2 := makePatient("p2", models.NonUrgent)
	p2.MLFQLevel = 0
	m.Enqueue(p2)

	// Q0 should be served first even though p1 was enqueued first
	got = m.Dequeue()
	if got.ID != "p2" {
		t.Errorf("MLFQ should serve Q0 (p2) before Q1 (p1), got %s", got.ID)
	}
}

func TestOptimal_Order(t *testing.T) {
	q := NewOptimalQueue()

	// Same effective priority, different remaining time: shortest remaining should go first.
	p1 := makePatient("p1", models.Urgent)
	p2 := makePatient("p2", models.Urgent)
	p3 := makePatient("p3", models.Critical) // should always come first due to higher urgency

	p1.RemainingTreatment = 12 * time.Second
	p2.RemainingTreatment = 5 * time.Second

	q.Enqueue(p1)
	q.Enqueue(p2)
	q.Enqueue(p3)

	got := q.Dequeue()
	if got.ID != "p3" {
		t.Errorf("Optimal should keep urgency first, got %s", got.ID)
	}

	got = q.Dequeue()
	if got.ID != "p2" {
		t.Errorf("Optimal should pick shorter remaining treatment next, got %s", got.ID)
	}

	got = q.Dequeue()
	if got.ID != "p1" {
		t.Errorf("expected p1 last, got %s", got.ID)
	}
}

func TestNewScheduler_AllAlgorithms(t *testing.T) {
	for _, algo := range AllAlgorithms() {
		s := NewScheduler(algo)
		if s.Name() != algo {
			t.Errorf("expected Name()=%s, got %s", algo, s.Name())
		}
		// Verify basic operations work
		p := makePatient("test", models.Urgent)
		s.Enqueue(p)
		if s.Len() != 1 {
			t.Errorf("%s: expected len 1, got %d", algo, s.Len())
		}
		got := s.Dequeue()
		if got == nil || got.ID != "test" {
			t.Errorf("%s: dequeue failed", algo)
		}
	}
}
