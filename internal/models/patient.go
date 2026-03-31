package models

import "time"

// TriageLevel maps to real hospital triage systems (ESI - Emergency Severity Index).
// In OS terms: this is the base priority of a process.
// Lower number = higher priority (just like Linux nice values where -20 is highest).
type TriageLevel int

const (
	Critical   TriageLevel = 1 // Cardiac arrest, massive trauma — needs immediate intervention
	Emergency  TriageLevel = 2 // Heart attack, stroke — life-threatening but slightly more stable
	Urgent     TriageLevel = 3 // Broken bones, deep lacerations — serious but not immediately fatal
	SemiUrgent TriageLevel = 4 // Sprains, moderate pain — can wait a bit
	NonUrgent  TriageLevel = 5 // Minor cuts, colds — lowest priority
)

func (t TriageLevel) String() string {
	switch t {
	case Critical:
		return "Critical"
	case Emergency:
		return "Emergency"
	case Urgent:
		return "Urgent"
	case SemiUrgent:
		return "Semi-Urgent"
	case NonUrgent:
		return "Non-Urgent"
	default:
		return "Unknown"
	}
}

type PatientStatus string

const (
	StatusWaiting      PatientStatus = "waiting"
	StatusAssigned     PatientStatus = "assigned"
	StatusInTreatment  PatientStatus = "in-treatment"
	StatusDischarged   PatientStatus = "discharged"
)

type Patient struct {
	ID              string        `json:"id"`
	Name            string        `json:"name"`
	TriageLevel     TriageLevel   `json:"triageLevel"`
	TriageLevelName string        `json:"triageLevelName"`
	EffectivePri    int           `json:"effectivePriority"` // Computed: base priority + aging bonus (lower = higher priority)
	Complaint       string        `json:"complaint"`
	CheckInTime     time.Time     `json:"checkInTime"`
	Status          PatientStatus `json:"status"`
	AssignedBed     string        `json:"assignedBed,omitempty"`
	AssignedDoc     string        `json:"assignedDoctor,omitempty"`
	Preempted       bool          `json:"preempted"`

	// Scheduling fields for multiple algorithms
	EstimatedDuration  time.Duration `json:"estimatedDuration"`  // set at creation — used by SJF
	RemainingTreatment time.Duration `json:"remainingTreatment"` // tracks leftover work for Round Robin
	TreatmentStarted   time.Time     `json:"-"`                  // when current treatment quantum began
	MLFQLevel          int           `json:"mlfqLevel"`          // 0-2 for Multilevel Feedback Queue

	// Index in the heap — needed by container/heap to update priority in-place.
	// In OS terms: this is like the process's position in the ready queue.
	HeapIndex int `json:"-"`
}

// NewPatient creates a patient with initial priority set from triage level.
// EffectivePriority starts as TriageLevel * 100, giving room for aging adjustments.
// A Critical patient starts at 100, NonUrgent at 500.
// EstimatedTreatmentDuration returns the expected treatment time for a triage level.
// Used by SJF scheduler and for setting initial RemainingTreatment.
func EstimatedTreatmentDuration(triage TriageLevel) time.Duration {
	switch triage {
	case Critical:
		return 20 * time.Second
	case Emergency:
		return 14 * time.Second
	case Urgent:
		return 11 * time.Second
	case SemiUrgent:
		return 8 * time.Second
	default:
		return 6 * time.Second
	}
}

func NewPatient(id, name string, triage TriageLevel, complaint string) *Patient {
	est := EstimatedTreatmentDuration(triage)
	return &Patient{
		ID:                 id,
		Name:               name,
		TriageLevel:        triage,
		TriageLevelName:    triage.String(),
		EffectivePri:       int(triage) * 100,
		Complaint:          complaint,
		CheckInTime:        time.Now(),
		Status:             StatusWaiting,
		EstimatedDuration:  est,
		RemainingTreatment: est,
		HeapIndex:          -1,
	}
}
