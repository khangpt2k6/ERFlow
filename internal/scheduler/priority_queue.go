package scheduler

import (
	"container/heap"
	"sync"

	"github.com/erflow/backend/internal/models"
)

// PatientQueue is a thread-safe min-heap sorted by EffectivePriority.
// Lower value = higher urgency = dequeued first.
type PatientQueue struct {
	mu   sync.Mutex
	heap patientHeap
}

func NewPatientQueue() *PatientQueue {
	pq := &PatientQueue{}
	heap.Init(&pq.heap)
	return pq
}

func (pq *PatientQueue) Name() Algorithm { return AlgoPriority }

// Enqueue adds a patient to the priority queue.
func (pq *PatientQueue) Enqueue(p *models.Patient) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(&pq.heap, p)
}

// Dequeue removes and returns the highest-priority patient.
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

// Update re-sorts a patient after their priority changes (e.g. aging).
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

// All returns a snapshot of all patients in priority order.
func (pq *PatientQueue) All() []*models.Patient {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	result := make([]*models.Patient, len(pq.heap))
	copy(result, pq.heap)
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j].EffectivePri < result[j-1].EffectivePri; j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	return result
}

// patientHeap implements heap.Interface.
type patientHeap []*models.Patient

func (h patientHeap) Len() int { return len(h) }

// Less: lower EffectivePri = higher priority. Ties broken by check-in time.
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
