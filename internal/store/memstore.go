package store

import (
	"fmt"
	"sync"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
)

// MemStore is the in-memory state of the entire ER.
//
// OS parallel: This is like the kernel's process table + device table.
// The OS kernel keeps all process info, device status, and resource state in
// kernel memory — protected by locks so multiple CPU cores don't corrupt it.
//
// We use sync.RWMutex here: multiple goroutines can READ simultaneously
// (RLock), but only ONE can WRITE at a time (Lock). This is a readers-writer
// lock — a real OS primitive. It's more efficient than a plain mutex when
// reads vastly outnumber writes (which they do here: the dashboard polls
// constantly, but check-ins are occasional).
type MemStore struct {
	mu       sync.RWMutex
	patients map[string]*models.Patient
	beds     map[string]*models.Bed
	doctors  map[string]*models.Doctor

	// The priority queue is the scheduler's ready queue.
	// Patients in "waiting" status live here, sorted by effective priority.
	Queue *scheduler.PatientQueue

	nextPatientNum int
}

func NewMemStore() *MemStore {
	s := &MemStore{
		patients: make(map[string]*models.Patient),
		beds:     make(map[string]*models.Bed),
		doctors:  make(map[string]*models.Doctor),
		Queue:    scheduler.NewPatientQueue(),
	}
	s.initializeER()
	return s
}

// initializeER sets up the default ER configuration:
// 10 general beds, 5 ICU beds, 2 trauma beds, 3 doctors.
func (s *MemStore) initializeER() {
	// General beds
	for i := 1; i <= 10; i++ {
		id := fmt.Sprintf("bed-g%d", i)
		s.beds[id] = &models.Bed{ID: id, Type: models.BedGeneral}
	}
	// ICU beds
	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("bed-icu%d", i)
		s.beds[id] = &models.Bed{ID: id, Type: models.BedICU}
	}
	// Trauma beds
	for i := 1; i <= 2; i++ {
		id := fmt.Sprintf("bed-t%d", i)
		s.beds[id] = &models.Bed{ID: id, Type: models.BedTrauma}
	}

	// Doctors
	s.doctors["doc-1"] = &models.Doctor{
		ID: "doc-1", Name: "Dr. Adams", Specialty: "General",
		MaxPatients: 5, PatientIDs: []string{},
	}
	s.doctors["doc-2"] = &models.Doctor{
		ID: "doc-2", Name: "Dr. Baker", Specialty: "Cardiac",
		MaxPatients: 4, PatientIDs: []string{},
	}
	s.doctors["doc-3"] = &models.Doctor{
		ID: "doc-3", Name: "Dr. Chen", Specialty: "Trauma",
		MaxPatients: 4, PatientIDs: []string{},
	}
}

// --- Patient Operations ---

func (s *MemStore) AddPatient(p *models.Patient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.patients[p.ID] = p
}

func (s *MemStore) GetPatient(id string) (*models.Patient, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.patients[id]
	return p, ok
}

func (s *MemStore) GetAllPatients() []*models.Patient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*models.Patient, 0, len(s.patients))
	for _, p := range s.patients {
		result = append(result, p)
	}
	return result
}

func (s *MemStore) NextPatientID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextPatientNum++
	return fmt.Sprintf("patient-%d", s.nextPatientNum)
}

// --- Bed Operations ---

func (s *MemStore) GetAllBeds() []*models.Bed {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*models.Bed, 0, len(s.beds))
	for _, b := range s.beds {
		result = append(result, b)
	}
	return result
}

func (s *MemStore) GetBed(id string) (*models.Bed, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.beds[id]
	return b, ok
}

// --- Doctor Operations ---

func (s *MemStore) GetAllDoctors() []*models.Doctor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*models.Doctor, 0, len(s.doctors))
	for _, d := range s.doctors {
		result = append(result, d)
	}
	return result
}

func (s *MemStore) GetDoctor(id string) (*models.Doctor, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.doctors[id]
	return d, ok
}
