package models

import "time"

// TriageLevel maps to real hospital triage systems (ESI - Emergency Severity Index).
// Lower number = higher priority.
type TriageLevel int

const (
	Critical   TriageLevel = 1
	Emergency  TriageLevel = 2
	Urgent     TriageLevel = 3
	SemiUrgent TriageLevel = 4
	NonUrgent  TriageLevel = 5
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
	StatusWaiting     PatientStatus = "waiting"
	StatusAssigned    PatientStatus = "assigned"
	StatusInTreatment PatientStatus = "in-treatment"
	StatusDischarged  PatientStatus = "discharged"
	StatusAdmitted    PatientStatus = "admitted"
	StatusTransferred PatientStatus = "transferred"
)

type Patient struct {
	ID              string        `json:"id"`
	Name            string        `json:"name"`
	TriageLevel     TriageLevel   `json:"triageLevel"`
	TriageLevelName string        `json:"triageLevelName"`
	EffectivePri    int           `json:"effectivePriority"`
	Complaint       string        `json:"complaint"`
	CheckInTime     time.Time     `json:"checkInTime"`
	Status          PatientStatus `json:"status"`
	AssignedBed     string        `json:"assignedBed,omitempty"`
	AssignedDoc     string        `json:"assignedDoctor,omitempty"`
	Preempted       bool          `json:"preempted"`

	EstimatedDuration  time.Duration `json:"estimatedDuration"`
	RemainingTreatment time.Duration `json:"remainingTreatment"`
	TreatmentStarted   time.Time     `json:"-"`

	// Index in the heap — needed by container/heap.
	HeapIndex int `json:"-"`
}

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
