package store

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
)

// MemStore is the in-memory state of the ER, protected by a RWMutex.
type MemStore struct {
	mu       sync.RWMutex
	patients map[string]*models.Patient
	beds     map[string]*models.Bed
	doctors  map[string]*models.Doctor

	Queue scheduler.Scheduler

	nextPatientNum int

	events       []*models.Event
	nextEventNum int

	BedMu sync.Mutex // protects bed assignment atomically

	// SSE publish callback
	OnEvent func(eventType string, data any)
}

func NewMemStore() *MemStore {
	s := &MemStore{
		patients: make(map[string]*models.Patient),
		beds:     make(map[string]*models.Bed),
		doctors:  make(map[string]*models.Doctor),
		Queue:    scheduler.NewScheduler(scheduler.AlgoPriority),
	}
	s.initializeER()
	return s
}

func (s *MemStore) initializeER() {
	// 10 general beds, 5 ICU beds, 2 trauma beds = 17 total
	for i := 1; i <= 10; i++ {
		id := fmt.Sprintf("bed-g%d", i)
		s.beds[id] = &models.Bed{ID: id, Type: models.BedGeneral}
	}
	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("bed-icu%d", i)
		s.beds[id] = &models.Bed{ID: id, Type: models.BedICU}
	}
	for i := 1; i <= 2; i++ {
		id := fmt.Sprintf("bed-t%d", i)
		s.beds[id] = &models.Bed{ID: id, Type: models.BedTrauma}
	}

	// 3 doctors (initial workers — auto-scaler adds more goroutines)
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

// --- Events ---

func (s *MemStore) AddEvent(eventType, message, concept string, details any) *models.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextEventNum++
	ev := &models.Event{
		ID:        fmt.Sprintf("evt-%d", s.nextEventNum),
		Type:      eventType,
		Message:   message,
		Concept:   concept,
		Details:   details,
		Timestamp: time.Now(),
	}
	s.events = append(s.events, ev)
	// Keep max 500 events to prevent unbounded growth
	if len(s.events) > 500 {
		s.events = s.events[len(s.events)-500:]
	}

	if s.OnEvent != nil {
		go s.OnEvent(eventType, ev)
	}
	return ev
}

func (s *MemStore) GetEvents() []*models.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*models.Event, len(s.events))
	copy(result, s.events)
	return result
}

func (s *MemStore) ClearEvents() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = nil
	s.nextEventNum = 0
}

// --- Patients ---

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
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (s *MemStore) NextPatientID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextPatientNum++
	return fmt.Sprintf("patient-%d", s.nextPatientNum)
}

// --- Beds ---

func (s *MemStore) GetAllBeds() []*models.Bed {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*models.Bed, 0, len(s.beds))
	for _, b := range s.beds {
		result = append(result, b)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (s *MemStore) GetBed(id string) (*models.Bed, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.beds[id]
	return b, ok
}

func (s *MemStore) AssignBedSafe(bedID, patientID string) (bool, error) {
	s.BedMu.Lock()
	defer s.BedMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	bed, ok := s.beds[bedID]
	if !ok {
		return false, fmt.Errorf("bed %s not found", bedID)
	}
	if bed.Occupied {
		return false, nil
	}

	bed.Occupied = true
	bed.PatientID = patientID
	if p, ok := s.patients[patientID]; ok {
		p.Status = models.StatusAssigned
		p.AssignedBed = bedID
	}
	return true, nil
}

func (s *MemStore) ReleaseBed(bedID string) (*models.Patient, error) {
	s.BedMu.Lock()
	defer s.BedMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	bed, ok := s.beds[bedID]
	if !ok {
		return nil, fmt.Errorf("bed %s not found", bedID)
	}
	if !bed.Occupied {
		return nil, nil
	}

	var released *models.Patient
	if bed.PatientID != "" {
		if p, ok := s.patients[bed.PatientID]; ok {
			released = p
		}
	}
	bed.Occupied = false
	bed.PatientID = ""
	return released, nil
}

func (s *MemStore) FindAvailableBed(bedType models.BedType) *models.Bed {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, b := range s.beds {
		if !b.Occupied {
			if bedType == "" || b.Type == bedType {
				return b
			}
		}
	}
	return nil
}

// --- Doctors ---

func (s *MemStore) GetAllDoctors() []*models.Doctor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*models.Doctor, 0, len(s.doctors))
	for _, d := range s.doctors {
		result = append(result, d)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (s *MemStore) GetDoctor(id string) (*models.Doctor, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.doctors[id]
	return d, ok
}

func (s *MemStore) FindAvailableDoctor() *models.Doctor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.doctors {
		if len(d.PatientIDs) < d.MaxPatients {
			return d
		}
	}
	return nil
}

func (s *MemStore) AssignDoctor(docID, patientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	doc, ok := s.doctors[docID]
	if !ok {
		return fmt.Errorf("doctor %s not found", docID)
	}
	if len(doc.PatientIDs) >= doc.MaxPatients {
		return fmt.Errorf("doctor %s at capacity", docID)
	}

	doc.PatientIDs = append(doc.PatientIDs, patientID)
	doc.CurrentPatient = patientID
	if p, ok := s.patients[patientID]; ok {
		p.AssignedDoc = docID
	}
	return nil
}

func (s *MemStore) RemovePatientFromDoctor(docID, patientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, ok := s.doctors[docID]
	if !ok {
		return
	}
	for i, pid := range doc.PatientIDs {
		if pid == patientID {
			doc.PatientIDs = append(doc.PatientIDs[:i], doc.PatientIDs[i+1:]...)
			break
		}
	}
	if doc.CurrentPatient == patientID {
		doc.CurrentPatient = ""
	}
}

// --- Reset ---

func (s *MemStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.patients = make(map[string]*models.Patient)
	s.beds = make(map[string]*models.Bed)
	s.doctors = make(map[string]*models.Doctor)
	s.Queue = scheduler.NewScheduler(scheduler.AlgoPriority)
	s.nextPatientNum = 0
	s.events = nil
	s.nextEventNum = 0

	s.initializeER()
}
