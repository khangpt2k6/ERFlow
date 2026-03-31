package scheduler

import (
	"container/heap"
	"sync"

	"github.com/erflow/backend/internal/models"
)

// SJFQueue implements Shortest Job First scheduling.
// Patients with shorter estimated treatment durations are scheduled first.
// OS parallel: SJF minimizes average waiting time but can starve long jobs.
type SJFQueue struct {
	mu   sync.Mutex
	heap sjfHeap
}

func NewSJFQueue() *SJFQueue {
	q := &SJFQueue{}
	heap.Init(&q.heap)
	return q
}

func (q *SJFQueue) Name() Algorithm { return AlgoSJF }

func (q *SJFQueue) Enqueue(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	heap.Push(&q.heap, p)
}

func (q *SJFQueue) Dequeue() *models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.heap.Len() == 0 {
		return nil
	}
	return heap.Pop(&q.heap).(*models.Patient)
}

func (q *SJFQueue) Peek() *models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.heap.Len() == 0 {
		return nil
	}
	return q.heap[0]
}

func (q *SJFQueue) Update(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if p.HeapIndex >= 0 && p.HeapIndex < q.heap.Len() {
		heap.Fix(&q.heap, p.HeapIndex)
	}
}

func (q *SJFQueue) Remove(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if p.HeapIndex >= 0 && p.HeapIndex < q.heap.Len() {
		heap.Remove(&q.heap, p.HeapIndex)
	}
}

func (q *SJFQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.heap.Len()
}

func (q *SJFQueue) All() []*models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]*models.Patient, len(q.heap))
	copy(result, q.heap)
	// Sort by EstimatedDuration (shortest first), ties broken by CheckInTime
	for i := 1; i < len(result); i++ {
		for j := i; j > 0; j-- {
			if result[j].EstimatedDuration < result[j-1].EstimatedDuration ||
				(result[j].EstimatedDuration == result[j-1].EstimatedDuration &&
					result[j].CheckInTime.Before(result[j-1].CheckInTime)) {
				result[j], result[j-1] = result[j-1], result[j]
			}
		}
	}
	return result
}

// sjfHeap implements heap.Interface, sorted by EstimatedDuration (shortest first).
type sjfHeap []*models.Patient

func (h sjfHeap) Len() int { return len(h) }

func (h sjfHeap) Less(i, j int) bool {
	if h[i].EstimatedDuration == h[j].EstimatedDuration {
		return h[i].CheckInTime.Before(h[j].CheckInTime)
	}
	return h[i].EstimatedDuration < h[j].EstimatedDuration
}

func (h sjfHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].HeapIndex = i
	h[j].HeapIndex = j
}

func (h *sjfHeap) Push(x interface{}) {
	p := x.(*models.Patient)
	p.HeapIndex = len(*h)
	*h = append(*h, p)
}

func (h *sjfHeap) Pop() interface{} {
	old := *h
	n := len(old)
	p := old[n-1]
	old[n-1] = nil
	p.HeapIndex = -1
	*h = old[:n-1]
	return p
}
