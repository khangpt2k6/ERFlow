package scheduler

import (
	"fmt"
	"time"

	"github.com/erflow/backend/internal/models"
)

// =============================================================================
// OS CONCEPT: AGING (starvation prevention)
// =============================================================================
//
// Problem: In a pure priority scheduler, low-priority processes can STARVE —
// they wait forever because higher-priority processes keep arriving.
// A NonUrgent patient could wait all day if Critical patients keep showing up.
//
// Solution: AGING. The OS periodically scans the ready queue and boosts the
// priority of processes that have been waiting too long. In Linux, the CFS
// (Completely Fair Scheduler) tracks "virtual runtime" — processes that haven't
// run get increasingly favorable scheduling. In our ER, we reduce EffectivePri
// by 50 for every 30 minutes of wait time (lower number = higher priority).
//
// This means a NonUrgent patient (starting at 500) who waits 2 hours gets
// 4 * 50 = 200 points of boost, dropping to EffectivePri 300 — same as an
// Urgent patient. They'll be seen before a fresh NonUrgent arrival.
// =============================================================================

// AgingResult holds the outcome of one aging pass for event logging.
type AgingResult struct {
	PatientID   string `json:"patientId"`
	PatientName string `json:"patientName"`
	OldPriority int    `json:"oldPriority"`
	NewPriority int    `json:"newPriority"`
	WaitMinutes int    `json:"waitMinutes"`
}

// ApplyAging scans all waiting patients and increases their priority based on
// how long they've been waiting. For every 30 minutes of wait time, EffectivePri
// is reduced by 50 (higher priority). The minimum EffectivePri is 50 (never
// surpasses a fresh Critical patient's base of 100 by too much).
//
// If artificialAge is non-zero, it is added to each patient's actual wait time.
// This lets simulation endpoints fast-forward time without modifying CheckInTime.
//
// Returns a slice of AgingResults describing what changed, for event logging.
func ApplyAging(pq *PatientQueue, patients []*models.Patient, artificialAge time.Duration) []AgingResult {
	var results []AgingResult

	for _, p := range patients {
		if p.Status != models.StatusWaiting {
			continue
		}

		totalWait := time.Since(p.CheckInTime) + artificialAge
		waitMinutes := int(totalWait.Minutes())

		// Calculate new effective priority: base - (aging bonus)
		// Each 30-minute block gives 50 points of priority boost.
		agingBonus := (waitMinutes / 30) * 50
		basePri := int(p.TriageLevel) * 100
		newPri := basePri - agingBonus

		// Floor: don't let it drop below 50
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

// FormatAgingMessage creates a human-readable summary of aging results.
func FormatAgingMessage(results []AgingResult) string {
	if len(results) == 0 {
		return "Aging pass complete: no priority changes needed"
	}
	return fmt.Sprintf("Aging pass complete: %d patient(s) had their priority boosted due to wait time", len(results))
}
