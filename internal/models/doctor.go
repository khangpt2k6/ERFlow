package models

type Doctor struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Specialty      string   `json:"specialty"`
	PatientIDs     []string `json:"patientIds"`
	MaxPatients    int      `json:"maxPatients"`
	CurrentPatient string   `json:"currentPatient,omitempty"`

	Busy         bool `json:"busy"`
	TotalTreated int  `json:"totalTreated"`
}
