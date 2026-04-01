package models

// Nurse handles async tasks: initial assessment, medication, vitals monitoring.
// OS parallel: I/O device — handles background work while CPU (doctor) does main processing.
type Nurse struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	PatientIDs     []string `json:"patientIds"`
	MaxPatients    int      `json:"maxPatients"`
	CurrentPatient string   `json:"currentPatient,omitempty"`
	Busy           bool     `json:"busy"`
	TotalAssisted  int      `json:"totalAssisted"`
}
