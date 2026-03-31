package scheduler

import (
	"sync"
	"time"

	"github.com/erflow/backend/internal/models"
)

// DefaultQuantum is the default time slice each patient gets before being rotated.
const DefaultQuantum = 5 * time.Second

// RoundRobinQueue implements Round Robin scheduling.
// Each patient gets a fixed time quantum of treatment, then goes to the back.
// OS parallel: time-sharing — ensures fairness by giving every process equal CPU slices.
type RoundRobinQueue struct {
	mu       sync.Mutex
	patients []*models.Patient
	Quantum  time.Duration
}

func NewRoundRobinQueue() *RoundRobinQueue {
	return &RoundRobinQueue{
		Quantum: DefaultQuantum,
	}
}

func (q *RoundRobinQueue) Name() Algorithm { return AlgoRoundRobin }

func (q *RoundRobinQueue) Enqueue(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.patients = append(q.patients, p)
}

func (q *RoundRobinQueue) Dequeue() *models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.patients) == 0 {
		return nil
	}
	p := q.patients[0]
	q.patients = q.patients[1:]
	return p
}

func (q *RoundRobinQueue) Peek() *models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.patients) == 0 {
		return nil
	}
	return q.patients[0]
}

func (q *RoundRobinQueue) Remove(p *models.Patient) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, pt := range q.patients {
		if pt.ID == p.ID {
			q.patients = append(q.patients[:i], q.patients[i+1:]...)
			return
		}
	}
}

// Update is a no-op for Round Robin — order is strictly FIFO rotation.
func (q *RoundRobinQueue) Update(p *models.Patient) {}

func (q *RoundRobinQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.patients)
}

func (q *RoundRobinQueue) All() []*models.Patient {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]*models.Patient, len(q.patients))
	copy(result, q.patients)
	return result
}

// GetQuantum returns the current quantum duration.
func (q *RoundRobinQueue) GetQuantum() time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.Quantum
}
