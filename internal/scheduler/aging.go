package scheduler

import (
	"fmt"
	"time"

	"github.com/erflow/backend/internal/models"
)

// AgingResult holds the outcome of one aging pass for event logging.
type AgingResult struct {
	PatientID   string `json:"patientId"`
	PatientName string `json:"patientName"`
	OldPriority int    `json:"oldPriority"`
	NewPriority int    `json:"newPriority"`
	WaitMinutes int    `json:"waitMinutes"`
}

// ApplyAging boosts priority of waiting patients based on wait time.
// Every 30 minutes of waiting reduces EffectivePri by 50 (floor: 50).
// Pass artificialAge > 0 to simulate time passing without changing CheckInTime.
func ApplyAging(pq *PatientQueue, patients []*models.Patient, artificialAge time.Duration) []AgingResult {
	var results []AgingResult

	for _, p := range patients {
		if p.Status != models.StatusWaiting {
			continue
		}

		totalWait := time.Since(p.CheckInTime) + artificialAge
		waitMinutes := int(totalWait.Minutes())

		agingBonus := (waitMinutes / 30) * 50
		basePri := int(p.TriageLevel) * 100
		newPri := basePri - agingBonus

		if newPri < 50 {
			newPri = 50
		}

		if newPri != p.EffectivePri {
			oldPri := p.EffectivePri
			p.EffectivePri = newPri
			pq.Update(p)

			results = append(results, AgingResult{
				PatientID:   p.ID,
				PatientName: p.Name,
				OldPriority: oldPri,
				NewPriority: newPri,
				WaitMinutes: waitMinutes,
			})
		}
	}

	return results
}

func FormatAgingMessage(results []AgingResult) string {
	if len(results) == 0 {
		return "Aging pass complete: no priority changes needed"
	}
	return fmt.Sprintf("Aging pass complete: %d patient(s) had their priority boosted due to wait time", len(results))
}
