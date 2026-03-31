package scheduler

import (
	"sync"
	"time"

	"github.com/erflow/backend/internal/models"
)

// MLFQ implements a Multilevel Feedback Queue scheduler.
// Three queues: Q0 (highest priority, 3s quantum), Q1 (medium, 6s quantum), Q2 (lowest, FCFS).
// New patients enter Q0. If they use their full quantum, they get demoted.
// Aging can promote patients from lower queues back up.
// OS parallel: Linux CFS inspiration — dynamic priority based on behavior, not just static level.
type MLFQ struct {
	mu     sync.Mutex
	queues [3][]*models.Patient // Q0=highest, Q1=medium, Q2=lowest (FCFS)
}

// MLFQ queue quantums — how long a patient gets in each queue before demotion.
var MLFQQuantums = [3]time.Duration{
	3 * time.Second,  // Q0: short quantum, interactive/critical
	6 * time.Second,  // Q1: medium quantum
	0,                // Q2: no quantum (FCFS, runs to completion)
}

func NewMLFQ() *MLFQ {
	return &MLFQ{}
}

func (m *MLFQ) Name() Algorithm { return AlgoMLFQ }

func (m *MLFQ) Enqueue(p *models.Patient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	level := p.MLFQLevel
	if level < 0 || level > 2 {
		level = 0
		p.MLFQLevel = 0
	}
	m.queues[level] = append(m.queues[level], p)
}

func (m *MLFQ) Dequeue() *models.Patient {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Always serve highest queue first
	for i := 0; i < 3; i++ {
		if len(m.queues[i]) > 0 {
			p := m.queues[i][0]
			m.queues[i] = m.queues[i][1:]
			return p
		}
	}
	return nil
}

func (m *MLFQ) Peek() *models.Patient {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < 3; i++ {
		if len(m.queues[i]) > 0 {
			return m.queues[i][0]
		}
	}
	return nil
}

func (m *MLFQ) Remove(p *models.Patient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < 3; i++ {
		for j, pt := range m.queues[i] {
			if pt.ID == p.ID {
				m.queues[i] = append(m.queues[i][:j], m.queues[i][j+1:]...)
				return
			}
		}
	}
}

// Update handles aging-driven re-sorting. For MLFQ, aging can promote patients
// from lower queues back to higher ones.
func (m *MLFQ) Update(p *models.Patient) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find which queue the patient is currently in
	currentLevel := -1
	currentIdx := -1
	for i := 0; i < 3; i++ {
		for j, pt := range m.queues[i] {
			if pt.ID == p.ID {
				currentLevel = i
				currentIdx = j
				break
			}
		}
		if currentLevel >= 0 {
			break
		}
	}

	if currentLevel < 0 {
		return // not in any queue
	}

	// If aging has boosted priority enough, promote to a higher queue
	targetLevel := p.MLFQLevel
	if targetLevel < currentLevel {
		// Remove from current queue
		m.queues[currentLevel] = append(m.queues[currentLevel][:currentIdx], m.queues[currentLevel][currentIdx+1:]...)
		// Add to target queue
		m.queues[targetLevel] = append(m.queues[targetLevel], p)
	}
}

func (m *MLFQ) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queues[0]) + len(m.queues[1]) + len(m.queues[2])
}

// All returns a snapshot in scheduling order: Q0 first, then Q1, then Q2.
func (m *MLFQ) All() []*models.Patient {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*models.Patient
	for i := 0; i < 3; i++ {
		result = append(result, m.queues[i]...)
	}
	return result
}

// Demote moves a patient to the next lower queue (used when quantum expires).
func (m *MLFQ) Demote(p *models.Patient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.MLFQLevel >= 2 {
		return // already at lowest
	}
	// Remove from current level
	for i, pt := range m.queues[p.MLFQLevel] {
		if pt.ID == p.ID {
			m.queues[p.MLFQLevel] = append(m.queues[p.MLFQLevel][:i], m.queues[p.MLFQLevel][i+1:]...)
			break
		}
	}
	p.MLFQLevel++
	m.queues[p.MLFQLevel] = append(m.queues[p.MLFQLevel], p)
}

// Promote moves a patient to the next higher queue (used by aging daemon).
func (m *MLFQ) Promote(p *models.Patient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.MLFQLevel <= 0 {
		return // already at highest
	}
	// Remove from current level
	for i, pt := range m.queues[p.MLFQLevel] {
		if pt.ID == p.ID {
			m.queues[p.MLFQLevel] = append(m.queues[p.MLFQLevel][:i], m.queues[p.MLFQLevel][i+1:]...)
			break
		}
	}
	p.MLFQLevel--
	m.queues[p.MLFQLevel] = append(m.queues[p.MLFQLevel], p)
}

// GetQuantum returns the quantum for a given MLFQ level.
func GetMLFQQuantum(level int) time.Duration {
	if level < 0 || level > 2 {
		return 0
	}
	return MLFQQuantums[level]
}
