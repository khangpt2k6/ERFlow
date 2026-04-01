package scheduler

import (
	"testing"

	"github.com/erflow/backend/internal/models"
)

func TestMLFQ_Demote(t *testing.T) {
	m := NewMLFQ()

	p := makePatient("p1", models.Urgent)
	p.MLFQLevel = 0
	m.Enqueue(p)

	// Demote from Q0 → Q1
	m.Demote(p)
	if p.MLFQLevel != 1 {
		t.Errorf("expected MLFQLevel 1 after demotion, got %d", p.MLFQLevel)
	}

	// Demote from Q1 → Q2
	m.Demote(p)
	if p.MLFQLevel != 2 {
		t.Errorf("expected MLFQLevel 2, got %d", p.MLFQLevel)
	}

	// Demote at Q2 should be no-op
	m.Demote(p)
	if p.MLFQLevel != 2 {
		t.Errorf("should stay at Q2, got %d", p.MLFQLevel)
	}

	// Should still be in queue
	if m.Len() != 1 {
		t.Errorf("expected 1 patient, got %d", m.Len())
	}
}

func TestMLFQ_Promote(t *testing.T) {
	m := NewMLFQ()

	p := makePatient("p1", models.NonUrgent)
	p.MLFQLevel = 2
	m.Enqueue(p)

	// Promote from Q2 → Q1
	m.Promote(p)
	if p.MLFQLevel != 1 {
		t.Errorf("expected MLFQLevel 1 after promotion, got %d", p.MLFQLevel)
	}

	// Promote from Q1 → Q0
	m.Promote(p)
	if p.MLFQLevel != 0 {
		t.Errorf("expected MLFQLevel 0, got %d", p.MLFQLevel)
	}

	// Promote at Q0 should be no-op
	m.Promote(p)
	if p.MLFQLevel != 0 {
		t.Errorf("should stay at Q0, got %d", p.MLFQLevel)
	}
}

func TestMLFQ_HigherQueueServedFirst(t *testing.T) {
	m := NewMLFQ()

	// Add patients to different levels
	p0 := makePatient("p0", models.Urgent)
	p0.MLFQLevel = 0
	p1 := makePatient("p1", models.SemiUrgent)
	p1.MLFQLevel = 1
	p2 := makePatient("p2", models.NonUrgent)
	p2.MLFQLevel = 2

	// Enqueue in reverse order
	m.Enqueue(p2)
	m.Enqueue(p1)
	m.Enqueue(p0)

	// Should dequeue Q0 first, then Q1, then Q2
	got := m.Dequeue()
	if got.ID != "p0" {
		t.Errorf("expected Q0 patient first, got %s", got.ID)
	}
	got = m.Dequeue()
	if got.ID != "p1" {
		t.Errorf("expected Q1 patient second, got %s", got.ID)
	}
	got = m.Dequeue()
	if got.ID != "p2" {
		t.Errorf("expected Q2 patient third, got %s", got.ID)
	}
}

func TestMLFQ_RemoveFromAnyLevel(t *testing.T) {
	m := NewMLFQ()

	p0 := makePatient("p0", models.Critical)
	p0.MLFQLevel = 0
	p1 := makePatient("p1", models.Urgent)
	p1.MLFQLevel = 1

	m.Enqueue(p0)
	m.Enqueue(p1)

	// Remove p0 from Q0
	m.Remove(p0)
	if m.Len() != 1 {
		t.Errorf("expected 1, got %d", m.Len())
	}

	got := m.Dequeue()
	if got.ID != "p1" {
		t.Errorf("expected p1 after removing p0, got %s", got.ID)
	}
}

func TestMLFQ_Update_Promotes(t *testing.T) {
	m := NewMLFQ()

	p := makePatient("p1", models.NonUrgent)
	p.MLFQLevel = 2
	m.Enqueue(p)

	// Simulate aging — set target level lower than current
	p.MLFQLevel = 0
	m.Update(p)

	// Should now be in Q0
	got := m.Dequeue()
	if got.ID != "p1" {
		t.Fatalf("expected p1, got %v", got)
	}
	// Queue should be empty
	if m.Len() != 0 {
		t.Errorf("expected empty queue, got %d", m.Len())
	}
}

func TestMLFQ_All_ReturnsSchedulingOrder(t *testing.T) {
	m := NewMLFQ()

	p0 := makePatient("p0", models.Critical)
	p0.MLFQLevel = 0
	p1 := makePatient("p1", models.Urgent)
	p1.MLFQLevel = 1
	p2 := makePatient("p2", models.NonUrgent)
	p2.MLFQLevel = 2

	m.Enqueue(p2)
	m.Enqueue(p1)
	m.Enqueue(p0)

	all := m.All()
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}
	// Should be in Q0, Q1, Q2 order
	if all[0].ID != "p0" {
		t.Errorf("first should be Q0 patient, got %s", all[0].ID)
	}
	if all[1].ID != "p1" {
		t.Errorf("second should be Q1 patient, got %s", all[1].ID)
	}
	if all[2].ID != "p2" {
		t.Errorf("third should be Q2 patient, got %s", all[2].ID)
	}
}

func TestMLFQ_EmptyDequeue(t *testing.T) {
	m := NewMLFQ()
	got := m.Dequeue()
	if got != nil {
		t.Errorf("dequeue from empty MLFQ should return nil, got %v", got)
	}
}

func TestMLFQ_EmptyPeek(t *testing.T) {
	m := NewMLFQ()
	got := m.Peek()
	if got != nil {
		t.Errorf("peek on empty MLFQ should return nil, got %v", got)
	}
}

func TestGetMLFQQuantum(t *testing.T) {
	tests := []struct {
		level    int
		expected int64 // nanoseconds
	}{
		{0, 3_000_000_000},  // 3s
		{1, 6_000_000_000},  // 6s
		{2, 0},              // FCFS, no quantum
		{-1, 0},             // out of bounds
		{5, 0},              // out of bounds
	}

	for _, tt := range tests {
		got := GetMLFQQuantum(tt.level)
		if got.Nanoseconds() != tt.expected {
			t.Errorf("GetMLFQQuantum(%d) = %v, want %v ns", tt.level, got, tt.expected)
		}
	}
}
