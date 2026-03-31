package scheduler

import (
	"fmt"

	"github.com/erflow/backend/internal/models"
)

// PreemptionResult describes one preemption swap for event logging.
type PreemptionResult struct {
	PreemptedPatientID   string `json:"preemptedPatientId"`
	PreemptedPatientName string `json:"preemptedPatientName"`
	PreemptedPriority    int    `json:"preemptedPriority"`
	IncomingPatientID    string `json:"incomingPatientId"`
	IncomingPatientName  string `json:"incomingPatientName"`
	IncomingPriority     int    `json:"incomingPriority"`
	BedID                string `json:"bedId"`
	DoctorID             string `json:"doctorId,omitempty"`
}

// CheckPreemption swaps Critical waiting patients into beds occupied by lower-priority ones.
func CheckPreemption(
	pq Scheduler,
	allPatients []*models.Patient,
	beds map[string]*models.Bed,
	doctors map[string]*models.Doctor,
) []PreemptionResult {
	var results []PreemptionResult

	var criticalWaiting []*models.Patient
	for _, p := range allPatients {
		if p.Status == models.StatusWaiting && p.TriageLevel == models.Critical {
			criticalWaiting = append(criticalWaiting, p)
		}
	}

	if len(criticalWaiting) == 0 {
		return nil
	}

	for _, critical := range criticalWaiting {
		victim := findLowestPriorityTreated(allPatients)
		if victim == nil {
			break
		}

		if victim.EffectivePri <= critical.EffectivePri {
			continue
		}

		bedID := victim.AssignedBed
		docID := victim.AssignedDoc

		victim.Status = models.StatusWaiting
		victim.Preempted = true
		victimBed := victim.AssignedBed
		victimDoc := victim.AssignedDoc
		victim.AssignedBed = ""
		victim.AssignedDoc = ""

		pq.Enqueue(victim)
		if bed, ok := beds[victimBed]; ok {
			bed.Occupied = false
			bed.PatientID = ""
		}

		if victimDoc != "" {
			if doc, ok := doctors[victimDoc]; ok {
				doc.CurrentPatient = ""
				removePatientFromDoctor(doc, victim.ID)
			}
		}

		pq.Remove(critical)
		critical.Status = models.StatusInTreatment
		critical.AssignedBed = bedID
		critical.AssignedDoc = docID

		if bed, ok := beds[bedID]; ok {
			bed.Occupied = true
			bed.PatientID = critical.ID
		}

		if docID != "" {
			if doc, ok := doctors[docID]; ok {
				doc.CurrentPatient = critical.ID
				doc.PatientIDs = append(doc.PatientIDs, critical.ID)
			}
		}

		results = append(results, PreemptionResult{
			PreemptedPatientID:   victim.ID,
			PreemptedPatientName: victim.Name,
			PreemptedPriority:    victim.EffectivePri,
			IncomingPatientID:    critical.ID,
			IncomingPatientName:  critical.Name,
			IncomingPriority:     critical.EffectivePri,
			BedID:                bedID,
			DoctorID:             docID,
		})
	}

	return results
}

// findLowestPriorityTreated returns the least-urgent patient currently in treatment.
func findLowestPriorityTreated(patients []*models.Patient) *models.Patient {
	var worst *models.Patient
	for _, p := range patients {
		if p.Status == models.StatusInTreatment {
			if worst == nil || p.EffectivePri > worst.EffectivePri {
				worst = p
			}
		}
	}
	return worst
}

func removePatientFromDoctor(doc *models.Doctor, patientID string) {
	for i, pid := range doc.PatientIDs {
		if pid == patientID {
			doc.PatientIDs = append(doc.PatientIDs[:i], doc.PatientIDs[i+1:]...)
			return
		}
	}
}

func FormatPreemptionMessage(r PreemptionResult) string {
	return fmt.Sprintf(
		"PREEMPTION: %s (Critical, priority %d) preempted %s (priority %d) from bed %s",
		r.IncomingPatientName, r.IncomingPriority,
		r.PreemptedPatientName, r.PreemptedPriority,
		r.BedID,
	)
}
