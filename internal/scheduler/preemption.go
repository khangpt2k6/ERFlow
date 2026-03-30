package scheduler

import (
	"fmt"

	"github.com/erflow/backend/internal/models"
)

// =============================================================================
// OS CONCEPT: PREEMPTIVE SCHEDULING
// =============================================================================
//
// In a non-preemptive (cooperative) scheduler, a running process keeps the CPU
// until it voluntarily yields. This is polite but dangerous — a CPU-bound
// process can hog the CPU indefinitely.
//
// In a PREEMPTIVE scheduler (used by all modern OSes), the kernel can FORCIBLY
// remove a running process from the CPU and give it to a higher-priority one.
// This happens on:
//   - Timer interrupt (time slice expired)
//   - Higher-priority process becomes ready (e.g., I/O completion)
//
// In our ER: when a Critical patient arrives and all beds are occupied by
// lower-priority patients, we PREEMPT the lowest-priority patient currently
// being treated — they go back to waiting, and the Critical patient gets
// their bed and doctor immediately.
//
// This mirrors exactly what Linux does: if a SCHED_FIFO real-time process
// becomes runnable, it immediately preempts any SCHED_NORMAL process.
// =============================================================================

// PreemptionResult describes one preemption action for event logging.
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

// CheckPreemption looks for Critical patients waiting in the queue and
// lower-priority patients currently being treated. If a preemption is possible,
// it performs the swap and returns the result.
//
// Parameters:
//   - pq: the priority queue (to dequeue the critical patient)
//   - allPatients: all patients in the store
//   - beds: all beds in the store
//   - doctors: all doctors in the store
//
// Returns a slice of PreemptionResults (may be empty if no preemption needed).
func CheckPreemption(
	pq *PatientQueue,
	allPatients []*models.Patient,
	beds map[string]*models.Bed,
	doctors map[string]*models.Doctor,
) []PreemptionResult {
	var results []PreemptionResult

	// Find Critical patients waiting in the queue
	var criticalWaiting []*models.Patient
	for _, p := range allPatients {
		if p.Status == models.StatusWaiting && p.TriageLevel == models.Critical {
			criticalWaiting = append(criticalWaiting, p)
		}
	}

	if len(criticalWaiting) == 0 {
		return nil
	}

	// Find the lowest-priority patient currently being treated
	for _, critical := range criticalWaiting {
		victim := findLowestPriorityTreated(allPatients)
		if victim == nil {
			break
		}

		// Only preempt if the victim is genuinely lower priority
		if victim.EffectivePri <= critical.EffectivePri {
			continue
		}

		// Perform the swap
		bedID := victim.AssignedBed
		docID := victim.AssignedDoc

		// Evict the victim: back to waiting
		victim.Status = models.StatusWaiting
		victim.Preempted = true
		victimBed := victim.AssignedBed
		victimDoc := victim.AssignedDoc
		victim.AssignedBed = ""
		victim.AssignedDoc = ""

		// Put victim back in the queue
		pq.Enqueue(victim)

		// Free the bed
		if bed, ok := beds[victimBed]; ok {
			bed.Occupied = false
			bed.PatientID = ""
		}

		// Free the doctor slot
		if victimDoc != "" {
			if doc, ok := doctors[victimDoc]; ok {
				doc.CurrentPatient = ""
				removePatientFromDoctor(doc, victim.ID)
			}
		}

		// Assign critical patient
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

// findLowestPriorityTreated finds the patient with the highest EffectivePri
// (lowest urgency) who is currently in treatment.
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

// removePatientFromDoctor removes a patient ID from a doctor's patient list.
func removePatientFromDoctor(doc *models.Doctor, patientID string) {
	for i, pid := range doc.PatientIDs {
		if pid == patientID {
			doc.PatientIDs = append(doc.PatientIDs[:i], doc.PatientIDs[i+1:]...)
			return
		}
	}
}

// FormatPreemptionMessage creates a human-readable summary.
func FormatPreemptionMessage(r PreemptionResult) string {
	return fmt.Sprintf(
		"PREEMPTION: %s (Critical, priority %d) preempted %s (priority %d) from bed %s",
		r.IncomingPatientName, r.IncomingPriority,
		r.PreemptedPatientName, r.PreemptedPriority,
		r.BedID,
	)
}
