package scheduler

import (
	"testing"
	"time"

	"github.com/erflow/backend/internal/models"
)

func TestApplyAging_BoostsPriority(t *testing.T) {
	pq := NewPatientQueue()

	// Patient waiting for a long time (simulate 60 minutes via artificialAge)
	p := makePatient("p1", models.NonUrgent) // base pri = 500
	p.Status = models.StatusWaiting
	pq.Enqueue(p)

	results := ApplyAging(pq, []*models.Patient{p}, 60*time.Minute)

	if len(results) != 1 {
		t.Fatalf("expected 1 aging result, got %d", len(results))
	}
	if results[0].NewPriority >= results[0].OldPriority {
		t.Errorf("priority should decrease (improve): old=%d, new=%d", results[0].OldPriority, results[0].NewPriority)
	}
	// 60 min = 2 intervals of 30 min, each subtracts 50 → 500 - 100 = 400
	if p.EffectivePri != 400 {
		t.Errorf("expected priority 400, got %d", p.EffectivePri)
	}
}

func TestApplyAging_FloorAt50(t *testing.T) {
	pq := NewPatientQueue()

	p := makePatient("p1", models.NonUrgent) // base pri = 500
	p.Status = models.StatusWaiting
	pq.Enqueue(p)

	// 10 hours = 600 min = 20 intervals * 50 = 1000 reduction → floor at 50
	results := ApplyAging(pq, []*models.Patient{p}, 10*time.Hour)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if p.EffectivePri != 50 {
		t.Errorf("priority should floor at 50, got %d", p.EffectivePri)
	}
}

func TestApplyAging_SkipsNonWaiting(t *testing.T) {
	pq := NewPatientQueue()

	p := makePatient("p1", models.NonUrgent)
	p.Status = models.StatusInTreatment // not waiting
	pq.Enqueue(p)

	results := ApplyAging(pq, []*models.Patient{p}, 60*time.Minute)

	if len(results) != 0 {
		t.Errorf("should skip non-waiting patients, got %d results", len(results))
	}
}

func TestApplyAging_NoChangeIfTooEarly(t *testing.T) {
	pq := NewPatientQueue()

	p := makePatient("p1", models.NonUrgent)
	p.Status = models.StatusWaiting
	pq.Enqueue(p)

	// Less than 30 minutes → no aging interval completed
	results := ApplyAging(pq, []*models.Patient{p}, 20*time.Minute)

	if len(results) != 0 {
		t.Errorf("should have no changes for <30 min wait, got %d results", len(results))
	}
}

func TestApplyAging_CriticalPatientGetsSmallBoost(t *testing.T) {
	pq := NewPatientQueue()

	p := makePatient("p1", models.Critical) // base pri = 100
	p.Status = models.StatusWaiting
	pq.Enqueue(p)

	// 30 min → 1 interval * 50 = 50 reduction → 100 - 50 = 50
	results := ApplyAging(pq, []*models.Patient{p}, 30*time.Minute)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if p.EffectivePri != 50 {
		t.Errorf("Critical after 30m should be 50, got %d", p.EffectivePri)
	}
}

func TestApplyAging_ReordersQueue(t *testing.T) {
	pq := NewPatientQueue()

	// p1: NonUrgent (500), p2: Urgent (300)
	p1 := makePatient("p1", models.NonUrgent)
	p1.Status = models.StatusWaiting
	p2 := makePatient("p2", models.Urgent)
	p2.Status = models.StatusWaiting

	pq.Enqueue(p1)
	pq.Enqueue(p2)

	// Before aging: p2 (300) should be first
	peek := pq.Peek()
	if peek.ID != "p2" {
		t.Fatalf("before aging, p2 should be first, got %s", peek.ID)
	}

	// Age p1 by 5 hours: 300 min = 10 intervals * 50 = 500 reduction → 500 - 500 = 50 (floor)
	// Only age p1 (pass slice with just p1)
	ApplyAging(pq, []*models.Patient{p1}, 5*time.Hour)

	// Now p1 (50) should outrank p2 (300)
	peek = pq.Peek()
	if peek.ID != "p1" {
		t.Errorf("after heavy aging, p1 should be first (pri=%d), but got %s (pri=%d)",
			p1.EffectivePri, peek.ID, peek.EffectivePri)
	}
}

func TestFormatAgingMessage_NoResults(t *testing.T) {
	msg := FormatAgingMessage(nil)
	if msg != "Aging pass complete: no priority changes needed" {
		t.Errorf("unexpected message: %s", msg)
	}
}

func TestFormatAgingMessage_WithResults(t *testing.T) {
	results := []AgingResult{{PatientID: "p1"}, {PatientID: "p2"}}
	msg := FormatAgingMessage(results)
	expected := "Aging pass complete: 2 patient(s) had their priority boosted due to wait time"
	if msg != expected {
		t.Errorf("expected %q, got %q", expected, msg)
	}
}
