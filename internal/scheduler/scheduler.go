package scheduler

import "github.com/erflow/backend/internal/models"

// Algorithm identifies a scheduling strategy.
type Algorithm string

const (
	AlgoPriority Algorithm = "priority"
)

// Scheduler is the abstraction the engine programs against.
type Scheduler interface {
	Name() Algorithm
	Enqueue(p *models.Patient)
	Dequeue() *models.Patient
	Peek() *models.Patient
	Remove(p *models.Patient)
	Update(p *models.Patient)
	Len() int
	All() []*models.Patient
}

// NewScheduler creates a priority queue scheduler.
func NewScheduler(algo Algorithm) Scheduler {
	return NewPatientQueue()
}
