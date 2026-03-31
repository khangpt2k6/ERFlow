package models

type Doctor struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Specialty      string   `json:"specialty"`
	PatientIDs     []string `json:"patientIds"`
	MaxPatients    int      `json:"maxPatients"`
	CurrentPatient string   `json:"currentPatient,omitempty"`

	// Resource management for deadlock detection
	HeldResources []string `json:"heldResources"` // resource types currently held
	WaitingFor    string   `json:"waitingFor"`     // resource type waiting to acquire
}
