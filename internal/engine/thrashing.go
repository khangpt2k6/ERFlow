package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

// ThroughputSample records a point-in-time measurement for the throughput chart.
type ThroughputSample struct {
	Timestamp        time.Time `json:"timestamp"`
	DischargesPerMin float64   `json:"dischargesPerMin"`
	PatientCount     int       `json:"patientCount"`
	BedCount         int       `json:"bedCount"`
	Ratio            float64   `json:"ratio"`
	Thrashing        bool      `json:"thrashing"`
}

// ThrashingStats is returned to the frontend.
type ThrashingStats struct {
	Thrashing       bool               `json:"thrashing"`
	Ratio           float64            `json:"ratio"`
	OverheadFactor  float64            `json:"overheadFactor"`
	Threshold       float64            `json:"threshold"`
	Samples         []ThroughputSample `json:"samples"`
}

// ThrashingMonitor tracks whether the ER is thrashing — spending more time
// managing patients than treating them when demand far exceeds capacity.
// OS parallel: when working set > physical memory, the OS spends all its time
// paging and throughput collapses.
type ThrashingMonitor struct {
	mu             sync.RWMutex
	threshold      float64 // patient-to-bed ratio that triggers thrashing
	overheadFactor float64 // treatment time multiplier when thrashing (e.g. 2.5x)
	samples        []ThroughputSample
	maxSamples     int
	thrashing      atomic.Bool

	// For computing discharge rate
	lastCheckTime     time.Time
	lastDischargeCount int64
}

// NewThrashingMonitor creates a monitor with the given threshold.
func NewThrashingMonitor(threshold float64) *ThrashingMonitor {
	return &ThrashingMonitor{
		threshold:      threshold,
		overheadFactor: 2.5,
		maxSamples:     60, // keep last 60 samples (~5 min at 5s intervals)
		lastCheckTime:  time.Now(),
	}
}

// Check evaluates current load and determines if thrashing is occurring.
func (tm *ThrashingMonitor) Check(activePatients, totalBeds int, currentDischarges int64) bool {
	if totalBeds == 0 {
		return false
	}

	ratio := float64(activePatients) / float64(totalBeds)
	wasThrashing := tm.thrashing.Load()
	nowThrashing := ratio > tm.threshold

	tm.thrashing.Store(nowThrashing)

	// Record sample
	tm.mu.Lock()
	elapsed := time.Since(tm.lastCheckTime).Minutes()
	dischargeDelta := currentDischarges - tm.lastDischargeCount
	dpm := 0.0
	if elapsed > 0 {
		dpm = float64(dischargeDelta) / elapsed
	}

	sample := ThroughputSample{
		Timestamp:        time.Now(),
		DischargesPerMin: dpm,
		PatientCount:     activePatients,
		BedCount:         totalBeds,
		Ratio:            ratio,
		Thrashing:        nowThrashing,
	}
	tm.samples = append(tm.samples, sample)
	if len(tm.samples) > tm.maxSamples {
		tm.samples = tm.samples[1:]
	}
	tm.lastCheckTime = time.Now()
	tm.lastDischargeCount = currentDischarges
	tm.mu.Unlock()

	// Return true if state changed
	return nowThrashing != wasThrashing
}

// IsThrashing returns current state.
func (tm *ThrashingMonitor) IsThrashing() bool {
	return tm.thrashing.Load()
}

// OverheadMultiplier returns the treatment time multiplier (1.0 normally, 2.5 when thrashing).
func (tm *ThrashingMonitor) OverheadMultiplier() float64 {
	if tm.thrashing.Load() {
		return tm.overheadFactor
	}
	return 1.0
}

// Stats returns current state for the frontend.
func (tm *ThrashingMonitor) Stats() ThrashingStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	samples := make([]ThroughputSample, len(tm.samples))
	copy(samples, tm.samples)

	ratio := 0.0
	if len(samples) > 0 {
		ratio = samples[len(samples)-1].Ratio
	}

	return ThrashingStats{
		Thrashing:      tm.thrashing.Load(),
		Ratio:          ratio,
		OverheadFactor: tm.overheadFactor,
		Threshold:      tm.threshold,
		Samples:        samples,
	}
}
