package store

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
)

// MemStore is the in-memory state of the entire ER, protected by a RWMutex.
type MemStore struct {
	mu       sync.RWMutex
	patients map[string]*models.Patient
	beds     map[string]*models.Bed
	doctors  map[string]*models.Doctor

	Queue scheduler.Scheduler

	// Semaphores — counting semaphores for bed types (buffered channels)
	GeneralSem *scheduler.BedSemaphore
	ICUSem     *scheduler.BedSemaphore
	TraumaSem  *scheduler.BedSemaphore

	// Resources for deadlock detection
	Resources *scheduler.ResourceManager

	nextPatientNum int

	events       []*models.Event
	nextEventNum int

	BedMu sync.Mutex // protects bed assignment (safe path acquires, unsafe path skips)
}

func NewMemStore() *MemStore {
	s := &MemStore{
		patients:   make(map[string]*models.Patient),
		beds:       make(map[string]*models.Bed),
		doctors:    make(map[string]*models.Doctor),
		Queue:      scheduler.NewScheduler(scheduler.AlgoPriority),
		GeneralSem: scheduler.NewBedSemaphore("General", 10),
		ICUSem:     scheduler.NewBedSemaphore("ICU", 5),
		TraumaSem:  scheduler.NewBedSemaphore("Trauma", 2),
		Resources:  scheduler.NewResourceManager(),
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

// AddEvent appends an event to the log.
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
	return ev
}

// GetEvents returns a copy of all events.
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

// AssignBedSafe atomically checks and assigns a bed (mutex-protected).
// Also acquires the corresponding semaphore permit.
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
		return false, nil // Bed already taken — no race, just unavailable
	}

	bed.Occupied = true
	bed.PatientID = patientID

	if p, ok := s.patients[patientID]; ok {
		p.Status = models.StatusAssigned
		p.AssignedBed = bedID
	}

	// Acquire semaphore permit (non-blocking since we already verified the bed is free)
	s.semForBed(bed.Type).TryAcquire()

	return true, nil
}

// semForBed returns the semaphore corresponding to a bed type.
func (s *MemStore) semForBed(bedType models.BedType) *scheduler.BedSemaphore {
	switch bedType {
	case models.BedICU:
		return s.ICUSem
	case models.BedTrauma:
		return s.TraumaSem
	default:
		return s.GeneralSem
	}
}

// SemaphoreStats returns stats for all three bed semaphores.
func (s *MemStore) SemaphoreStats() []scheduler.SemaphoreStats {
	return []scheduler.SemaphoreStats{
		s.GeneralSem.Stats(),
		s.ICUSem.Stats(),
		s.TraumaSem.Stats(),
	}
}

// AssignBedUnsafe skips the mutex to demonstrate a TOCTOU race condition.
func (s *MemStore) AssignBedUnsafe(bedID, patientID string) (bool, error) {
	s.mu.RLock()
	bed, ok := s.beds[bedID]
	if !ok {
		s.mu.RUnlock()
		return false, fmt.Errorf("bed %s not found", bedID)
	}
	occupied := bed.Occupied
	s.mu.RUnlock()

	if occupied {
		return false, nil
	}

	time.Sleep(100 * time.Millisecond) // race window
	s.mu.Lock()
	bed.Occupied = true
	bed.PatientID = patientID
	if p, ok := s.patients[patientID]; ok {
		p.Status = models.StatusAssigned
		p.AssignedBed = bedID
	}
	s.mu.Unlock()

	return true, nil
}

// ReleaseBed frees a bed and resets the patient's status.
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
		return nil, fmt.Errorf("bed %s is already empty", bedID)
	}

	var released *models.Patient
	if bed.PatientID != "" {
		if p, ok := s.patients[bed.PatientID]; ok {
			p.Status = models.StatusWaiting
			p.AssignedBed = ""
			p.AssignedDoc = ""
			released = p
		}
	}

	bedType := bed.Type
	bed.Occupied = false
	bed.PatientID = ""

	// Release semaphore permit — signals any blocked goroutine
	s.semForBed(bedType).Release()

	return released, nil
}

// FindAvailableBed returns an unoccupied bed matching the type (or any if empty).
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

// FindAvailableDoctor returns a doctor with capacity.
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
		return fmt.Errorf("doctor %s is at capacity", docID)
	}

	doc.PatientIDs = append(doc.PatientIDs, patientID)
	doc.CurrentPatient = patientID

	if p, ok := s.patients[patientID]; ok {
		p.AssignedDoc = docID
	}

	return nil
}

// RemovePatientFromDoctor removes a patient from a doctor's list (write-lock protected).
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

func (s *MemStore) GetBedsMap() map[string]*models.Bed {
	return s.beds
}

func (s *MemStore) GetDoctorsMap() map[string]*models.Doctor {
	return s.doctors
}

// Reset clears all state and reinitializes the ER.
func (s *MemStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.patients = make(map[string]*models.Patient)
	s.beds = make(map[string]*models.Bed)
	s.doctors = make(map[string]*models.Doctor)
	// Preserve the current algorithm across resets
	algo := s.Queue.Name()
	s.Queue = scheduler.NewScheduler(algo)
	s.GeneralSem = scheduler.NewBedSemaphore("General", 10)
	s.ICUSem = scheduler.NewBedSemaphore("ICU", 5)
	s.TraumaSem = scheduler.NewBedSemaphore("Trauma", 2)
	s.Resources = scheduler.NewResourceManager()
	s.nextPatientNum = 0
	s.events = nil
	s.nextEventNum = 0

	s.initializeER()
}
