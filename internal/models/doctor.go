package models

type Doctor struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Specialty      string   `json:"specialty"`
	PatientIDs     []string `json:"patientIds"`
	MaxPatients    int      `json:"maxPatients"`
	CurrentPatient string   `json:"currentPatient,omitempty"`

	// Worker pool fields
	Busy         bool `json:"busy"`
	TotalTreated int  `json:"totalTreated"`

	// Context switch tracking
	ContextSwitches int    `json:"contextSwitches"`
	LastPatientID   string `json:"lastPatientId"`

	// Resource management for deadlock detection
	HeldResources []string `json:"heldResources"` // resource types currently held
	WaitingFor    string   `json:"waitingFor"`     // resource type waiting to acquire
}
