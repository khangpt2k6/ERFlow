package store

import (
	"fmt"
	"sync"
	"time"

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

	// Event log — the kernel's audit trail.
	events       []*models.Event
	nextEventNum int

	// BedMu is the mutex that protects bed assignment.
	// OS parallel: this is like a spinlock or mutex protecting a shared device.
	// The "safe" assignment path acquires this lock; the "unsafe" path skips it
	// to demonstrate what happens without mutual exclusion.
	BedMu sync.Mutex
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

// --- Event Operations ---

// AddEvent appends an event to the log. Thread-safe.
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

// GetEvents returns a copy of all events. Thread-safe.
func (s *MemStore) GetEvents() []*models.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*models.Event, len(s.events))
	copy(result, s.events)
	return result
}

// ClearEvents removes all events.
func (s *MemStore) ClearEvents() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = nil
	s.nextEventNum = 0
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

// GetAllPatientsUnsafe returns all patients WITHOUT acquiring the lock.
// Only call this when you already hold the lock externally.
func (s *MemStore) GetAllPatientsUnsafe() []*models.Patient {
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

// AssignBedSafe atomically checks if a bed is available and assigns it.
//
// OS parallel: This is the CORRECT way to access a shared resource — acquire
// the mutex BEFORE the check-then-set operation. This makes the entire
// "check if free → mark as occupied" operation ATOMIC, preventing TOCTOU
// (Time-of-Check-to-Time-of-Use) race conditions.
//
// Returns (success, error).
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

	return true, nil
}

// AssignBedUnsafe intentionally does NOT lock. It reads, sleeps (simulating
// processing delay), then writes — creating a TOCTOU race window.
//
// OS parallel: This is what happens when you forget to use a mutex. Two threads
// both read "bed is free", both decide to assign it, and both write — last one
// wins, and the first patient's assignment silently disappears. This is a
// classic data race / lost update bug.
func (s *MemStore) AssignBedUnsafe(bedID, patientID string) (bool, error) {
	// Step 1: READ — is the bed free? (no lock!)
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

	// Step 2: DELAY — simulates processing time. During this window,
	// another goroutine can also read "bed is free" and proceed.
	time.Sleep(100 * time.Millisecond)

	// Step 3: WRITE — assign the bed (no lock!)
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

// ReleaseBed frees a bed and returns its patient to waiting status.
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

	bed.Occupied = false
	bed.PatientID = ""
	return released, nil
}

// FindAvailableBed returns the first unoccupied bed of the given type,
// or any unoccupied bed if bedType is empty. Returns nil if none available.
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

// FindAvailableDoctor returns a doctor with capacity for another patient.
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

// AssignDoctor assigns a patient to a doctor.
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

// --- Bed/Doctor Maps (for scheduler access) ---

// GetBedsMap returns the internal beds map. Caller must hold appropriate locks
// or use this only in contexts where the store lock is already held.
func (s *MemStore) GetBedsMap() map[string]*models.Bed {
	return s.beds
}

// GetDoctorsMap returns the internal doctors map.
func (s *MemStore) GetDoctorsMap() map[string]*models.Doctor {
	return s.doctors
}

// --- Reset ---

// Reset clears all state and reinitializes the ER to defaults.
func (s *MemStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.patients = make(map[string]*models.Patient)
	s.beds = make(map[string]*models.Bed)
	s.doctors = make(map[string]*models.Doctor)
	s.Queue = scheduler.NewPatientQueue()
	s.nextPatientNum = 0
	s.events = nil
	s.nextEventNum = 0

	s.initializeER()
}
