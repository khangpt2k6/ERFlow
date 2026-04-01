package engine

import (
	"testing"
)

func TestThrashingMonitor_NotThrashingInitially(t *testing.T) {
	tm := NewThrashingMonitor(2.0)
	if tm.IsThrashing() {
		t.Error("should not be thrashing initially")
	}
	if tm.OverheadMultiplier() != 1.0 {
		t.Errorf("overhead should be 1.0 initially, got %f", tm.OverheadMultiplier())
	}
}

func TestThrashingMonitor_DetectsThrashing(t *testing.T) {
	tm := NewThrashingMonitor(2.0)

	// ratio = 30/10 = 3.0 > threshold 2.0 → thrashing
	changed := tm.Check(30, 10, 0)
	if !changed {
		t.Error("state should change to thrashing")
	}
	if !tm.IsThrashing() {
		t.Error("should be thrashing when ratio > threshold")
	}
	if tm.OverheadMultiplier() != 2.5 {
		t.Errorf("overhead should be 2.5 when thrashing, got %f", tm.OverheadMultiplier())
	}
}

func TestThrashingMonitor_ResolvesWhenLoadDrops(t *testing.T) {
	tm := NewThrashingMonitor(2.0)

	// Enter thrashing
	tm.Check(30, 10, 0)
	if !tm.IsThrashing() {
		t.Fatal("should be thrashing")
	}

	// Drop below threshold: ratio = 15/10 = 1.5 < 2.0
	changed := tm.Check(15, 10, 5)
	if !changed {
		t.Error("state should change back to not-thrashing")
	}
	if tm.IsThrashing() {
		t.Error("should not be thrashing after load drops")
	}
	if tm.OverheadMultiplier() != 1.0 {
		t.Errorf("overhead should be 1.0 after resolving, got %f", tm.OverheadMultiplier())
	}
}

func TestThrashingMonitor_NoChangeReturnsFalse(t *testing.T) {
	tm := NewThrashingMonitor(2.0)

	// Both below threshold
	changed := tm.Check(10, 10, 0)
	if changed {
		t.Error("no state change expected")
	}
	changed = tm.Check(12, 10, 0)
	if changed {
		t.Error("still below threshold, no state change")
	}
}

func TestThrashingMonitor_ZeroBeds(t *testing.T) {
	tm := NewThrashingMonitor(2.0)

	// Should not panic or thrash with 0 beds
	changed := tm.Check(10, 0, 0)
	if changed {
		t.Error("should return false for 0 beds")
	}
}

func TestThrashingMonitor_Stats(t *testing.T) {
	tm := NewThrashingMonitor(2.0)

	tm.Check(30, 10, 0) // trigger thrashing

	stats := tm.Stats()
	if !stats.Thrashing {
		t.Error("stats should show thrashing")
	}
	if stats.Threshold != 2.0 {
		t.Errorf("threshold should be 2.0, got %f", stats.Threshold)
	}
	if stats.OverheadFactor != 2.5 {
		t.Errorf("overhead factor should be 2.5, got %f", stats.OverheadFactor)
	}
	if len(stats.Samples) != 1 {
		t.Errorf("expected 1 sample, got %d", len(stats.Samples))
	}
}

func TestThrashingMonitor_SampleCap(t *testing.T) {
	tm := NewThrashingMonitor(2.0)

	// Add more than maxSamples
	for i := 0; i < 100; i++ {
		tm.Check(30, 10, int64(i))
	}

	stats := tm.Stats()
	if len(stats.Samples) > 60 {
		t.Errorf("samples should be capped at 60, got %d", len(stats.Samples))
	}
}

func TestThrashingMonitor_DischargeRate(t *testing.T) {
	tm := NewThrashingMonitor(2.0)

	// First check: baseline
	tm.Check(10, 10, 0)

	// Second check with some discharges
	tm.Check(10, 10, 10)

	stats := tm.Stats()
	if len(stats.Samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(stats.Samples))
	}
	// The second sample should have a discharge rate > 0
	// (exact value depends on timing, just check it's non-negative)
	if stats.Samples[1].DischargesPerMin < 0 {
		t.Error("discharge rate should be non-negative")
	}
}
