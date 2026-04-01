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
	StatusArrived       PatientStatus = "arrived"        // just entered ER, not triaged yet
	StatusTriage        PatientStatus = "triage"         // being assessed at triage desk
	StatusWaiting       PatientStatus = "waiting"        // triaged, in queue
	StatusAssigned      PatientStatus = "assigned"       // bed assigned
	StatusInTreatment   PatientStatus = "in-treatment"   // in bed, doctor visiting
	StatusAwaitingLab   PatientStatus = "awaiting-lab"   // waiting for lab/imaging results
	StatusDischarged    PatientStatus = "discharged"     // sent home
	StatusAdmitted      PatientStatus = "admitted"       // admitted to hospital ward
	StatusTransferred   PatientStatus = "transferred"    // transferred to another facility
)

// Disposition determines what happens after treatment.
type Disposition string

const (
	DispoDischarge Disposition = "discharge" // go home
	DispoAdmit     Disposition = "admit"     // admit to hospital ward
	DispoTransfer  Disposition = "transfer"  // transfer to another facility
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

	// Nurse assignment (OS: I/O device handling async tasks)
	AssignedNurse string `json:"assignedNurse,omitempty"`

	// Lab/imaging orders (OS: process waiting on I/O completion)
	LabOrdered  bool      `json:"labOrdered"`            // doctor ordered lab work
	LabType     string    `json:"labType,omitempty"`     // "blood", "ct-scan", "x-ray"
	LabReady    bool      `json:"labReady"`              // results are back
	LabOrderedAt time.Time `json:"-"`

	// Multiple doctor visits (OS: multi-phase process execution)
	DoctorVisits    int `json:"doctorVisits"`    // how many times doctor has visited
	MaxDoctorVisits int `json:"maxDoctorVisits"` // total visits needed (initial exam, check results, final)

	// Disposition (OS: process termination type)
	Disposition Disposition `json:"disposition,omitempty"` // discharge, admit, or transfer

	// Index in the heap — needed by container/heap to update priority in-place.
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
	// ESI 1-2 skip triage (arrive by ambulance, severity is obvious)
	initialStatus := StatusArrived
	if triage <= Emergency {
		initialStatus = StatusWaiting // already triaged by EMS
	}

	// Determine how many doctor visits needed based on severity
	visits := 2 // default: initial exam + final check
	if triage <= Emergency {
		visits = 3 // critical: initial + check results + final assessment
	} else if triage >= SemiUrgent {
		visits = 1 // minor: single visit enough
	}

	// Determine disposition based on severity
	dispo := DispoDischarge
	if triage == Critical {
		dispo = DispoAdmit // critical → admit to hospital
	} else if triage == Emergency && len(id) > 0 {
		// 50% admit, 50% discharge
		if id[len(id)-1]%2 == 0 {
			dispo = DispoAdmit
		}
	}

	return &Patient{
		ID:                 id,
		Name:               name,
		TriageLevel:        triage,
		TriageLevelName:    triage.String(),
		EffectivePri:       int(triage) * 100,
		Complaint:          complaint,
		CheckInTime:        time.Now(),
		Status:             initialStatus,
		EstimatedDuration:  est,
		RemainingTreatment: est,
		MaxDoctorVisits:    visits,
		Disposition:        dispo,
		HeapIndex:          -1,
	}
}
