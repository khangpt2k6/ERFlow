package models

type BedType string

const (
	BedGeneral BedType = "general"
	BedICU     BedType = "icu"
	BedTrauma  BedType = "trauma"
)

type Bed struct {
	ID        string  `json:"id"`
	Type      BedType `json:"type"`
	Occupied  bool    `json:"occupied"`
	PatientID string  `json:"patientId,omitempty"`
}
