package scheduler

import "github.com/erflow/backend/internal/models"

// Algorithm identifies a scheduling strategy.
type Algorithm string

const (
	AlgoPriority   Algorithm = "priority"
	AlgoFCFS       Algorithm = "fcfs"
	AlgoSJF        Algorithm = "sjf"
	AlgoRoundRobin Algorithm = "round-robin"
	AlgoMLFQ       Algorithm = "mlfq"
)

// AllAlgorithms returns all available scheduling algorithms.
func AllAlgorithms() []Algorithm {
	return []Algorithm{AlgoPriority, AlgoFCFS, AlgoSJF, AlgoRoundRobin, AlgoMLFQ}
}

// Scheduler is the abstraction the engine programs against.
// Every algorithm must implement these methods.
type Scheduler interface {
	// Name returns the algorithm identifier.
	Name() Algorithm

	// Enqueue adds a patient to the ready queue.
	Enqueue(p *models.Patient)

	// Dequeue removes and returns the next patient to be scheduled.
	Dequeue() *models.Patient

	// Peek returns the next patient without removing them.
	Peek() *models.Patient

	// Remove removes a specific patient from the queue.
	Remove(p *models.Patient)

	// Update re-sorts a patient after their priority changes (e.g., aging).
	Update(p *models.Patient)

	// Len returns the number of patients in the queue.
	Len() int

	// All returns a snapshot of all patients in scheduling order.
	All() []*models.Patient
}

// NewScheduler creates a scheduler of the given algorithm type.
func NewScheduler(algo Algorithm) Scheduler {
	switch algo {
	case AlgoFCFS:
		return NewFCFSQueue()
	case AlgoSJF:
		return NewSJFQueue()
	case AlgoRoundRobin:
		return NewRoundRobinQueue()
	case AlgoMLFQ:
		return NewMLFQ()
	default:
		return NewPatientQueue()
	}
}
