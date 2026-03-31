package scheduler

import (
	"sort"
	"sync"

	"github.com/erflow/backend/internal/models"
)

// FCFSQueue implements First-Come-First-Served scheduling.
// Patients are served strictly in arrival order regardless of priority.
// OS parallel: the simplest scheduler — no priority, no preemption, just FIFO.
type FCFSQueue struct {
	mu       sync.Mutex
	patients []*models.Patient
}

func NewFCFSQueue() *FCFSQueue {
	return &FCFSQueue{}
}

func (q *FCFSQueue) Name() Algorithm { return AlgoFCFS }

func (q *FCFSQueue) Enqueue(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.patients = append(q.patients, p)
}

func (q *FCFSQueue) Dequeue() *models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.patients) == 0 {
		return nil
	}
	p := q.patients[0]
	q.patients = q.patients[1:]
	return p
}

func (q *FCFSQueue) Peek() *models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.patients) == 0 {
		return nil
	}
	return q.patients[0]
}

func (q *FCFSQueue) Remove(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, pt := range q.patients {
		if pt.ID == p.ID {
			q.patients = append(q.patients[:i], q.patients[i+1:]...)
			return
		}
	}
}

// Update is a no-op for FCFS — arrival order never changes.
func (q *FCFSQueue) Update(p *models.Patient) {}

func (q *FCFSQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.patients)
}

func (q *FCFSQueue) All() []*models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]*models.Patient, len(q.patients))
	copy(result, q.patients)
	// Already in FIFO order, but sort by CheckInTime to be explicit
	sort.Slice(result, func(i, j int) bool {
		return result[i].CheckInTime.Before(result[j].CheckInTime)
	})
	return result
}
