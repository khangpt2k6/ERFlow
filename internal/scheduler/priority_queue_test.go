package scheduler

import (
	"sync"
	"testing"

	"github.com/erflow/backend/internal/models"
)

func makePatient(id string, triage models.TriageLevel) *models.Patient {
	return models.NewPatient(id, "Test-"+id, triage, "test complaint")
}

func TestPriorityQueue_EnqueueDequeue(t *testing.T) {
	pq := NewPatientQueue()

	p1 := makePatient("p1", models.NonUrgent)   // pri 500
	p2 := makePatient("p2", models.Critical)     // pri 100
	p3 := makePatient("p3", models.Urgent)       // pri 300

	pq.Enqueue(p1)
	pq.Enqueue(p2)
	pq.Enqueue(p3)

	if pq.Len() != 3 {
		t.Fatalf("expected len 3, got %d", pq.Len())
	}

	// Should dequeue in priority order: Critical, Urgent, NonUrgent
	got := pq.Dequeue()
	if got.ID != "p2" {
		t.Errorf("expected p2 (Critical), got %s", got.ID)
	}
	got = pq.Dequeue()
	if got.ID != "p3" {
		t.Errorf("expected p3 (Urgent), got %s", got.ID)
	}
	got = pq.Dequeue()
	if got.ID != "p1" {
		t.Errorf("expected p1 (NonUrgent), got %s", got.ID)
	}
}

func TestPriorityQueue_PeekDoesNotRemove(t *testing.T) {
	pq := NewPatientQueue()
	p := makePatient("p1", models.Critical)
	pq.Enqueue(p)

	peek := pq.Peek()
	if peek == nil || peek.ID != "p1" {
		t.Fatal("peek returned wrong patient")
	}
	if pq.Len() != 1 {
		t.Fatal("peek should not remove from queue")
	}
}

func TestPriorityQueue_Update(t *testing.T) {
	pq := NewPatientQueue()
	p1 := makePatient("p1", models.NonUrgent)   // pri 500
	p2 := makePatient("p2", models.SemiUrgent)   // pri 400
	pq.Enqueue(p1)
	pq.Enqueue(p2)

	// Boost p1 to higher priority than p2
	p1.EffectivePri = 50
	pq.Update(p1)

	got := pq.Peek()
	if got.ID != "p1" {
		t.Errorf("after update, expected p1 at front, got %s", got.ID)
	}
}

func TestPriorityQueue_Remove(t *testing.T) {
	pq := NewPatientQueue()
	p1 := makePatient("p1", models.Critical)
	p2 := makePatient("p2", models.NonUrgent)
	pq.Enqueue(p1)
	pq.Enqueue(p2)

	pq.Remove(p1)
	if pq.Len() != 1 {
		t.Fatal("expected len 1 after remove")
	}
	got := pq.Dequeue()
	if got.ID != "p2" {
		t.Errorf("expected p2, got %s", got.ID)
	}
}

func TestPriorityQueue_ConcurrentEnqueueDequeue(t *testing.T) {
	pq := NewPatientQueue()
	var wg sync.WaitGroup

	// 50 goroutines enqueue
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			p := makePatient("p"+string(rune('A'+n%26)), models.TriageLevel(n%5+1))
			p.ID = "concurrent-" + string(rune('A'+n%26)) + string(rune('0'+n/26))
			pq.Enqueue(p)
		}(i)
	}

	// 25 goroutines dequeue
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pq.Dequeue() // may return nil, that's fine
		}()
	}

	wg.Wait()
	// No panic = test passes (race detector will catch issues)
}

func BenchmarkPriorityQueue_Enqueue(b *testing.B) {
	pq := NewPatientQueue()
	for i := 0; i < b.N; i++ {
		p := makePatient("bench", models.TriageLevel(i%5+1))
		p.ID = "bench-" + string(rune(i))
		pq.Enqueue(p)
	}
}
