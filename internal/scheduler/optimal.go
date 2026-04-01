package scheduler

import (
	"container/heap"
	"sync"
	"time"

	"github.com/erflow/backend/internal/models"
)

// OptimalQueue is an adaptive queue tuned for emergency flow visualization:
// 1) preserve urgency guarantees (EffectivePri), then
// 2) improve throughput (shorter remaining treatment first), then
// 3) preserve fairness (earlier check-in first).
//
// It is a pragmatic "best overall" policy for this simulator.
type OptimalQueue struct {
	mu   sync.Mutex
	heap optimalHeap
}

func NewOptimalQueue() *OptimalQueue {
	q := &OptimalQueue{}
	heap.Init(&q.heap)
	return q
}

func (q *OptimalQueue) Name() Algorithm { return AlgoOptimal }

func (q *OptimalQueue) Enqueue(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	heap.Push(&q.heap, p)
}

func (q *OptimalQueue) Dequeue() *models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.heap.Len() == 0 {
		return nil
	}
	return heap.Pop(&q.heap).(*models.Patient)
}

func (q *OptimalQueue) Peek() *models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.heap.Len() == 0 {
		return nil
	}
	return q.heap[0]
}

func (q *OptimalQueue) Remove(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if p.HeapIndex >= 0 && p.HeapIndex < q.heap.Len() {
		heap.Remove(&q.heap, p.HeapIndex)
	}
}

func (q *OptimalQueue) Update(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if p.HeapIndex >= 0 && p.HeapIndex < q.heap.Len() {
		heap.Fix(&q.heap, p.HeapIndex)
	}
}

func (q *OptimalQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.heap.Len()
}

func (q *OptimalQueue) All() []*models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]*models.Patient, len(q.heap))
	copy(result, q.heap)
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && optimalLess(result[j], result[j-1]); j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	return result
}

type optimalHeap []*models.Patient

func (h optimalHeap) Len() int { return len(h) }

func (h optimalHeap) Less(i, j int) bool {
	return optimalLess(h[i], h[j])
}

func (h optimalHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].HeapIndex = i
	h[j].HeapIndex = j
}

func (h *optimalHeap) Push(x interface{}) {
	p := x.(*models.Patient)
	p.HeapIndex = len(*h)
	*h = append(*h, p)
}

func (h *optimalHeap) Pop() interface{} {
	old := *h
	n := len(old)
	p := old[n-1]
	old[n-1] = nil
	p.HeapIndex = -1
	*h = old[:n-1]
	return p
}

func optimalLess(a, b *models.Patient) bool {
	if a.EffectivePri != b.EffectivePri {
		return a.EffectivePri < b.EffectivePri
	}

	ar := remainingOrEstimated(a)
	br := remainingOrEstimated(b)
	if ar != br {
		return ar < br
	}

	if !a.CheckInTime.Equal(b.CheckInTime) {
		return a.CheckInTime.Before(b.CheckInTime)
	}

	return a.ID < b.ID
}

func remainingOrEstimated(p *models.Patient) time.Duration {
	if p.RemainingTreatment > 0 {
		return p.RemainingTreatment
	}
	return p.EstimatedDuration
}
