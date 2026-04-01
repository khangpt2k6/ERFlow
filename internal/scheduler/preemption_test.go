package scheduler

import (
	"testing"

	"github.com/erflow/backend/internal/models"
)

func TestCheckPreemption_CriticalBumpsLowest(t *testing.T) {
	pq := NewPatientQueue()
	beds := map[string]*models.Bed{
		"bed-1": {ID: "bed-1", Type: models.BedGeneral, Occupied: true, PatientID: "p1"},
	}
	doctors := map[string]*models.Doctor{
		"doc-1": {ID: "doc-1", Name: "Dr. A", PatientIDs: []string{"p1"}, CurrentPatient: "p1"},
	}

	// p1 is in treatment with low priority
	p1 := makePatient("p1", models.NonUrgent) // pri 500
	p1.Status = models.StatusInTreatment
	p1.AssignedBed = "bed-1"
	p1.AssignedDoc = "doc-1"

	// p2 is critical and waiting
	p2 := makePatient("p2", models.Critical) // pri 100
	p2.Status = models.StatusWaiting
	pq.Enqueue(p2)

	results := CheckPreemption(pq, []*models.Patient{p1, p2}, beds, doctors)

	if len(results) != 1 {
		t.Fatalf("expected 1 preemption, got %d", len(results))
	}
	r := results[0]
	if r.PreemptedPatientID != "p1" {
		t.Errorf("expected p1 to be preempted, got %s", r.PreemptedPatientID)
	}
	if r.IncomingPatientID != "p2" {
		t.Errorf("expected p2 to take the bed, got %s", r.IncomingPatientID)
	}
	if r.BedID != "bed-1" {
		t.Errorf("expected bed-1, got %s", r.BedID)
	}

	// Verify state changes
	if p1.Status != models.StatusWaiting {
		t.Errorf("preempted patient should be waiting, got %s", p1.Status)
	}
	if !p1.Preempted {
		t.Error("preempted patient should have Preempted flag set")
	}
	if p2.Status != models.StatusInTreatment {
		t.Errorf("incoming patient should be in-treatment, got %s", p2.Status)
	}
	if p2.AssignedBed != "bed-1" {
		t.Errorf("incoming patient should have bed-1, got %s", p2.AssignedBed)
	}
}

func TestCheckPreemption_NoCriticalWaiting(t *testing.T) {
	pq := NewPatientQueue()

	// Only non-critical patients, none waiting as critical
	p1 := makePatient("p1", models.NonUrgent)
	p1.Status = models.StatusInTreatment

	results := CheckPreemption(pq, []*models.Patient{p1}, nil, nil)
	if len(results) != 0 {
		t.Errorf("expected no preemptions, got %d", len(results))
	}
}

func TestCheckPreemption_WontPreemptHigherPriority(t *testing.T) {
	pq := NewPatientQueue()
	beds := map[string]*models.Bed{
		"bed-1": {ID: "bed-1", Type: models.BedGeneral, Occupied: true, PatientID: "p1"},
	}
	doctors := map[string]*models.Doctor{}

	// p1 is also critical and in treatment
	p1 := makePatient("p1", models.Critical) // pri 100
	p1.Status = models.StatusInTreatment
	p1.AssignedBed = "bed-1"

	// p2 is critical and waiting — same or lower priority than p1
	p2 := makePatient("p2", models.Critical) // pri 100
	p2.Status = models.StatusWaiting
	pq.Enqueue(p2)

	results := CheckPreemption(pq, []*models.Patient{p1, p2}, beds, doctors)
	if len(results) != 0 {
		t.Errorf("should not preempt equal-priority patient, got %d preemptions", len(results))
	}
}

func TestCheckPreemption_MultipleCritical(t *testing.T) {
	pq := NewPatientQueue()
	beds := map[string]*models.Bed{
		"bed-1": {ID: "bed-1", Type: models.BedGeneral, Occupied: true, PatientID: "p1"},
		"bed-2": {ID: "bed-2", Type: models.BedGeneral, Occupied: true, PatientID: "p2"},
	}
	doctors := map[string]*models.Doctor{
		"doc-1": {ID: "doc-1", PatientIDs: []string{"p1"}, CurrentPatient: "p1"},
		"doc-2": {ID: "doc-2", PatientIDs: []string{"p2"}, CurrentPatient: "p2"},
	}

	p1 := makePatient("p1", models.NonUrgent) // pri 500
	p1.Status = models.StatusInTreatment
	p1.AssignedBed = "bed-1"
	p1.AssignedDoc = "doc-1"

	p2 := makePatient("p2", models.SemiUrgent) // pri 400
	p2.Status = models.StatusInTreatment
	p2.AssignedBed = "bed-2"
	p2.AssignedDoc = "doc-2"

	// Two critical patients waiting
	c1 := makePatient("c1", models.Critical)
	c1.Status = models.StatusWaiting
	c2 := makePatient("c2", models.Critical)
	c2.Status = models.StatusWaiting
	pq.Enqueue(c1)
	pq.Enqueue(c2)

	results := CheckPreemption(pq, []*models.Patient{p1, p2, c1, c2}, beds, doctors)

	// Should preempt the two lowest-priority patients
	if len(results) < 1 {
		t.Errorf("expected at least 1 preemption for multiple critical patients, got %d", len(results))
	}
}

func TestFindLowestPriorityTreated(t *testing.T) {
	patients := []*models.Patient{
		{ID: "p1", Status: models.StatusInTreatment, EffectivePri: 200},
		{ID: "p2", Status: models.StatusInTreatment, EffectivePri: 500},
		{ID: "p3", Status: models.StatusWaiting, EffectivePri: 600}, // not in treatment
	}

	worst := findLowestPriorityTreated(patients)
	if worst == nil {
		t.Fatal("expected to find a patient")
	}
	if worst.ID != "p2" {
		t.Errorf("expected p2 (highest pri number = lowest urgency), got %s", worst.ID)
	}
}

func TestFormatPreemptionMessage(t *testing.T) {
	r := PreemptionResult{
		IncomingPatientName:  "Alice",
		IncomingPriority:     100,
		PreemptedPatientName: "Bob",
		PreemptedPriority:    500,
		BedID:                "bed-1",
	}
	msg := FormatPreemptionMessage(r)
	if msg == "" {
		t.Error("message should not be empty")
	}
}
