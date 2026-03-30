package scheduler

import (
	"container/heap"
	"sync"

	"github.com/erflow/backend/internal/models"
)

// =============================================================================
// OS CONCEPT: PRIORITY QUEUE (used by the CPU scheduler)
// =============================================================================
//
// In an OS, the scheduler maintains a "ready queue" of processes waiting for CPU time.
// But it's NOT a regular FIFO queue — it's a PRIORITY QUEUE implemented as a min-heap.
//
// A min-heap is a binary tree where the parent is always smaller than its children.
// This means the smallest element (highest priority) is always at the root — O(1) to find,
// O(log n) to insert or remove. Perfect for a scheduler that constantly needs the
// highest-priority process.
//
// Go's container/heap package gives us this. We implement 5 methods:
//   - Len()           → how many patients are waiting
//   - Less(i, j)      → is patient i higher priority than patient j?
//   - Swap(i, j)      → swap two patients in the queue
//   - Push(x)         → add a new patient (goes to bottom, then "bubbles up")
//   - Pop()           → remove highest-priority patient (root, then "sifts down")
//
// The key insight: Less() compares EffectivePriority, not just TriageLevel.
// This is how aging works later — a patient's effective priority decreases over time
// (lower number = higher priority), so they gradually "bubble up" in the heap.
// =============================================================================

// PatientQueue is a min-heap of patients, sorted by EffectivePriority.
// Lower EffectivePriority = higher urgency = gets scheduled first.
// Thread-safe via embedded mutex.
type PatientQueue struct {
	mu   sync.Mutex
	heap patientHeap
}

func NewPatientQueue() *PatientQueue {
	pq := &PatientQueue{}
	heap.Init(&pq.heap)
	return pq
}

// Enqueue adds a patient to the priority queue.
// OS equivalent: a new process enters the ready queue.
func (pq *PatientQueue) Enqueue(p *models.Patient) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(&pq.heap, p)
}

// Dequeue removes and returns the highest-priority patient.
// OS equivalent: the scheduler picks the next process to run.
func (pq *PatientQueue) Dequeue() *models.Patient {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if pq.heap.Len() == 0 {
		return nil
	}
	return heap.Pop(&pq.heap).(*models.Patient)
}

// Peek returns the highest-priority patient without removing them.
func (pq *PatientQueue) Peek() *models.Patient {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if pq.heap.Len() == 0 {
		return nil
	}
	return pq.heap[0]
}

// Update re-sorts a patient after their priority changes (e.g., from aging).
// OS equivalent: the scheduler re-evaluates a process's priority.
func (pq *PatientQueue) Update(p *models.Patient) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if p.HeapIndex >= 0 && p.HeapIndex < pq.heap.Len() {
		heap.Fix(&pq.heap, p.HeapIndex)
	}
}

// Remove removes a specific patient from the queue (e.g., when assigned to a bed).
func (pq *PatientQueue) Remove(p *models.Patient) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if p.HeapIndex >= 0 && p.HeapIndex < pq.heap.Len() {
		heap.Remove(&pq.heap, p.HeapIndex)
	}
}

// Len returns the number of patients waiting.
func (pq *PatientQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.heap.Len()
}

// All returns a snapshot of all patients in priority order (for display).
// Does NOT modify the queue.
func (pq *PatientQueue) All() []*models.Patient {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Copy and sort — we don't want to drain the heap
	result := make([]*models.Patient, len(pq.heap))
	copy(result, pq.heap)

	// Sort by effective priority (heap property only guarantees root is min)
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j].EffectivePri < result[j-1].EffectivePri; j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// patientHeap implements heap.Interface.
// This is the raw heap — PatientQueue wraps it with thread safety.
// ---------------------------------------------------------------------------

type patientHeap []*models.Patient

func (h patientHeap) Len() int { return len(h) }

// Less defines the priority order.
// Lower EffectivePri = higher priority = should be at the top of the heap.
// Tie-breaker: earlier check-in time wins (FIFO among equal priorities).
func (h patientHeap) Less(i, j int) bool {
	if h[i].EffectivePri == h[j].EffectivePri {
		return h[i].CheckInTime.Before(h[j].CheckInTime)
	}
	return h[i].EffectivePri < h[j].EffectivePri
}

func (h patientHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].HeapIndex = i
	h[j].HeapIndex = j
}

func (h *patientHeap) Push(x interface{}) {
	p := x.(*models.Patient)
	p.HeapIndex = len(*h)
	*h = append(*h, p)
}

func (h *patientHeap) Pop() interface{} {
	old := *h
	n := len(old)
	p := old[n-1]
	old[n-1] = nil // avoid memory leak
	p.HeapIndex = -1
	*h = old[:n-1]
	return p
}
