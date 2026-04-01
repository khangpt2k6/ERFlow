package scheduler

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/erflow/backend/internal/models"
)

// Property: No patient is lost across enqueue/dequeue cycles.
// For every scheduler, if we enqueue N patients and dequeue N patients,
// we get exactly the same N patients back (no duplicates, no missing).
func TestProperty_NoPatientLost(t *testing.T) {
	for _, algo := range AllAlgorithms() {
		t.Run(string(algo), func(t *testing.T) {
			sched := NewScheduler(algo)
			n := 100

			patients := make(map[string]*models.Patient, n)
			for i := 0; i < n; i++ {
				triage := models.TriageLevel(i%5 + 1)
				p := models.NewPatient(fmt.Sprintf("p-%d", i), fmt.Sprintf("Patient-%d", i), triage, "test")
				// Stagger check-in times so heaps have unique sort keys
				p.CheckInTime = time.Now().Add(time.Duration(i) * time.Millisecond)
				patients[p.ID] = p
				sched.Enqueue(p)
			}

			if sched.Len() != n {
				t.Fatalf("expected queue len %d, got %d", n, sched.Len())
			}

			seen := make(map[string]bool)
			for i := 0; i < n; i++ {
				p := sched.Dequeue()
				if p == nil {
					t.Fatalf("dequeue returned nil at index %d (expected %d more patients)", i, n-i)
				}
				if seen[p.ID] {
					t.Errorf("duplicate dequeue: %s", p.ID)
				}
				seen[p.ID] = true
				if _, ok := patients[p.ID]; !ok {
					t.Errorf("dequeued unknown patient: %s", p.ID)
				}
			}

			// Queue should be empty
			if sched.Len() != 0 {
				t.Errorf("queue should be empty after draining, got %d", sched.Len())
			}
			if sched.Dequeue() != nil {
				t.Error("dequeue on empty queue should return nil")
			}

			// All patients accounted for
			if len(seen) != n {
				t.Errorf("expected %d unique patients, got %d", n, len(seen))
			}
		})
	}
}

// Property: Priority scheduler always dequeues in priority order.
func TestProperty_PriorityOrder(t *testing.T) {
	pq := NewPatientQueue()
	n := 50

	for i := 0; i < n; i++ {
		triage := models.TriageLevel(rand.Intn(5) + 1)
		p := models.NewPatient(fmt.Sprintf("p-%d", i), "Test", triage, "test")
		p.CheckInTime = time.Now().Add(time.Duration(i) * time.Millisecond)
		pq.Enqueue(p)
	}

	prev := 0
	for i := 0; i < n; i++ {
		p := pq.Dequeue()
		if p == nil {
			t.Fatalf("nil at index %d", i)
		}
		if p.EffectivePri < prev {
			t.Errorf("priority order violated at %d: prev=%d, current=%d", i, prev, p.EffectivePri)
		}
		prev = p.EffectivePri
	}
}

// Property: SJF always dequeues in shortest-duration-first order.
func TestProperty_SJFOrder(t *testing.T) {
	q := NewSJFQueue()
	n := 50

	for i := 0; i < n; i++ {
		triage := models.TriageLevel(rand.Intn(5) + 1)
		p := models.NewPatient(fmt.Sprintf("p-%d", i), "Test", triage, "test")
		p.CheckInTime = time.Now().Add(time.Duration(i) * time.Millisecond)
		q.Enqueue(p)
	}

	var prevDur time.Duration
	for i := 0; i < n; i++ {
		p := q.Dequeue()
		if p == nil {
			t.Fatalf("nil at index %d", i)
		}
		if p.EstimatedDuration < prevDur {
			t.Errorf("SJF order violated at %d: prev=%v, current=%v", i, prevDur, p.EstimatedDuration)
		}
		prevDur = p.EstimatedDuration
	}
}

// Property: FCFS always dequeues in arrival order.
func TestProperty_FCFSOrder(t *testing.T) {
	q := NewFCFSQueue()
	n := 50

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		triage := models.TriageLevel(rand.Intn(5) + 1)
		p := models.NewPatient(fmt.Sprintf("p-%d", i), "Test", triage, "test")
		ids[i] = p.ID
		q.Enqueue(p)
	}

	for i := 0; i < n; i++ {
		p := q.Dequeue()
		if p == nil {
			t.Fatalf("nil at index %d", i)
		}
		if p.ID != ids[i] {
			t.Errorf("FCFS order violated at %d: expected %s, got %s", i, ids[i], p.ID)
		}
	}
}

// Property: Aging eventually makes any waiting patient the highest priority.
// This proves the system is starvation-free when aging is enabled.
func TestProperty_AgingPreventsStarvation(t *testing.T) {
	pq := NewPatientQueue()

	// One very low priority patient
	low := models.NewPatient("low", "LowPri", models.NonUrgent, "test") // pri 500
	low.Status = models.StatusWaiting
	pq.Enqueue(low)

	// Many high priority patients
	for i := 0; i < 10; i++ {
		p := models.NewPatient(fmt.Sprintf("high-%d", i), "HighPri", models.Emergency, "test")
		p.Status = models.StatusWaiting
		p.CheckInTime = time.Now() // recent arrival
		pq.Enqueue(p)
	}

	// The low priority patient is NOT at the front initially
	peek := pq.Peek()
	if peek.ID == "low" {
		t.Skip("low priority patient already at front — test setup issue")
	}

	// Apply heavy aging to the low priority patient
	ApplyAging(pq, []*models.Patient{low}, 24*time.Hour) // 24 hours of waiting

	// Now low should be at priority floor (50) — better than Emergency (200)
	if low.EffectivePri != 50 {
		t.Fatalf("expected pri=50 after heavy aging, got %d", low.EffectivePri)
	}

	peek = pq.Peek()
	if peek.ID != "low" {
		t.Errorf("after aging, low-priority patient should be first, got %s (pri=%d)",
			peek.ID, peek.EffectivePri)
	}
}

// Property: All schedulers handle Remove correctly.
func TestProperty_RemoveIntegrity(t *testing.T) {
	for _, algo := range AllAlgorithms() {
		t.Run(string(algo), func(t *testing.T) {
			sched := NewScheduler(algo)

			p1 := models.NewPatient("p1", "A", models.Critical, "test")
			p2 := models.NewPatient("p2", "B", models.NonUrgent, "test")
			p3 := models.NewPatient("p3", "C", models.Urgent, "test")

			p1.CheckInTime = time.Now()
			p2.CheckInTime = time.Now().Add(1 * time.Millisecond)
			p3.CheckInTime = time.Now().Add(2 * time.Millisecond)

			sched.Enqueue(p1)
			sched.Enqueue(p2)
			sched.Enqueue(p3)

			sched.Remove(p2)

			if sched.Len() != 2 {
				t.Errorf("expected len 2 after remove, got %d", sched.Len())
			}

			// Drain and verify p2 is not present
			seen := make(map[string]bool)
			for {
				p := sched.Dequeue()
				if p == nil {
					break
				}
				seen[p.ID] = true
			}
			if seen["p2"] {
				t.Error("removed patient p2 should not appear in dequeue")
			}
			if !seen["p1"] || !seen["p3"] {
				t.Error("remaining patients should still be dequeue-able")
			}
		})
	}
}

// Property: Peek returns the same patient that Dequeue would return.
func TestProperty_PeekMatchesDequeue(t *testing.T) {
	for _, algo := range AllAlgorithms() {
		t.Run(string(algo), func(t *testing.T) {
			sched := NewScheduler(algo)

			for i := 0; i < 10; i++ {
				triage := models.TriageLevel(i%5 + 1)
				p := models.NewPatient(fmt.Sprintf("p-%d", i), "Test", triage, "test")
				p.CheckInTime = time.Now().Add(time.Duration(i) * time.Millisecond)
				sched.Enqueue(p)
			}

			peeked := sched.Peek()
			dequeued := sched.Dequeue()

			if peeked == nil || dequeued == nil {
				t.Fatal("peek and dequeue should not return nil")
			}
			if peeked.ID != dequeued.ID {
				t.Errorf("peek (%s) != dequeue (%s)", peeked.ID, dequeued.ID)
			}
		})
	}
}

// Stress test: concurrent enqueue/dequeue/remove across all schedulers.
func TestProperty_ConcurrentSafety(t *testing.T) {
	for _, algo := range AllAlgorithms() {
		t.Run(string(algo), func(t *testing.T) {
			sched := NewScheduler(algo)
			var wg sync.WaitGroup

			// 30 enqueue goroutines
			for i := 0; i < 30; i++ {
				wg.Add(1)
				go func(n int) {
					defer wg.Done()
					triage := models.TriageLevel(n%5 + 1)
					p := models.NewPatient(fmt.Sprintf("c-%d", n), "Test", triage, "test")
					sched.Enqueue(p)
				}(i)
			}

			// 15 dequeue goroutines
			for i := 0; i < 15; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sched.Dequeue()
				}()
			}

			// 5 peek goroutines
			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sched.Peek()
				}()
			}

			// 5 Len goroutines
			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sched.Len()
				}()
			}

			// 5 All goroutines
			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sched.All()
				}()
			}

			wg.Wait()
			// No panic under -race = pass
		})
	}
}

// Benchmark: Enqueue + Dequeue cycle for all schedulers.
func BenchmarkSchedulers_EnqueueDequeue(b *testing.B) {
	for _, algo := range AllAlgorithms() {
		b.Run(string(algo), func(b *testing.B) {
			sched := NewScheduler(algo)
			patients := make([]*models.Patient, b.N)
			for i := 0; i < b.N; i++ {
				patients[i] = models.NewPatient(
					fmt.Sprintf("b-%d", i), "Bench",
					models.TriageLevel(i%5+1), "bench",
				)
				patients[i].CheckInTime = time.Now().Add(time.Duration(i) * time.Microsecond)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sched.Enqueue(patients[i])
			}
			for i := 0; i < b.N; i++ {
				sched.Dequeue()
			}
		})
	}
}

func BenchmarkPriorityQueue_Dequeue(b *testing.B) {
	pq := NewPatientQueue()
	for i := 0; i < 1000; i++ {
		p := models.NewPatient(fmt.Sprintf("b-%d", i), "Bench", models.TriageLevel(i%5+1), "bench")
		p.CheckInTime = time.Now().Add(time.Duration(i) * time.Microsecond)
		pq.Enqueue(p)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := pq.Dequeue()
		if p != nil {
			pq.Enqueue(p) // put back to keep queue non-empty
		}
	}
}

func BenchmarkSJFQueue_Dequeue(b *testing.B) {
	q := NewSJFQueue()
	for i := 0; i < 1000; i++ {
		p := models.NewPatient(fmt.Sprintf("b-%d", i), "Bench", models.TriageLevel(i%5+1), "bench")
		p.CheckInTime = time.Now().Add(time.Duration(i) * time.Microsecond)
		q.Enqueue(p)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := q.Dequeue()
		if p != nil {
			q.Enqueue(p)
		}
	}
}

func BenchmarkFCFSQueue_Dequeue(b *testing.B) {
	q := NewFCFSQueue()
	for i := 0; i < 1000; i++ {
		p := models.NewPatient(fmt.Sprintf("b-%d", i), "Bench", models.TriageLevel(i%5+1), "bench")
		q.Enqueue(p)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := q.Dequeue()
		if p != nil {
			q.Enqueue(p)
		}
	}
}

func BenchmarkMLFQ_Dequeue(b *testing.B) {
	m := NewMLFQ()
	for i := 0; i < 1000; i++ {
		p := models.NewPatient(fmt.Sprintf("b-%d", i), "Bench", models.TriageLevel(i%5+1), "bench")
		p.MLFQLevel = i % 3
		m.Enqueue(p)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := m.Dequeue()
		if p != nil {
			m.Enqueue(p)
		}
	}
}
