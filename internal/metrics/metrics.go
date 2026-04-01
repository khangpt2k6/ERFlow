package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ERFlow Prometheus metrics.
// These give you real numbers to put on a resume:
// "MLFQ reduces p99 wait time by 40% vs FCFS under 3x overload."

var (
	// --- Counters ---

	// PatientsTotal counts all patient arrivals, labeled by triage level.
	PatientsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "erflow",
		Name:      "patients_total",
		Help:      "Total patients checked in, by triage level.",
	}, []string{"triage"})

	// DischargesTotal counts completed treatments, labeled by triage level.
	DischargesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "erflow",
		Name:      "discharges_total",
		Help:      "Total patients discharged, by triage level.",
	}, []string{"triage"})

	// PreemptionsTotal counts how many times a critical patient bumped another.
	PreemptionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "erflow",
		Name:      "preemptions_total",
		Help:      "Total preemptions (critical patient bumped a lower-priority one).",
	})

	// ContextSwitchesTotal counts doctor context switches.
	ContextSwitchesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "erflow",
		Name:      "context_switches_total",
		Help:      "Total doctor context switches.",
	})

	// DeadlocksDetected counts deadlock detection events.
	DeadlocksDetected = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "erflow",
		Name:      "deadlocks_detected_total",
		Help:      "Total deadlocks detected in resource wait-for graph.",
	})

	// DeadlocksResolved counts deadlock resolution events.
	DeadlocksResolved = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "erflow",
		Name:      "deadlocks_resolved_total",
		Help:      "Total deadlocks resolved via victim selection.",
	})

	// AgingBoosts counts priority boosts from the aging daemon.
	AgingBoosts = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "erflow",
		Name:      "aging_boosts_total",
		Help:      "Total aging-driven priority boosts.",
	})

	// --- Gauges ---

	// QueueLength shows current patients waiting in the scheduler queue.
	QueueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "erflow",
		Name:      "queue_length",
		Help:      "Current number of patients in the scheduler queue.",
	}, []string{"algorithm"})

	// ActivePatients shows patients currently in the system (not discharged).
	ActivePatients = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "erflow",
		Name:      "active_patients",
		Help:      "Patients currently in the system (waiting + in-treatment).",
	})

	// BedsOccupied shows current bed utilization.
	BedsOccupied = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "erflow",
		Name:      "beds_occupied",
		Help:      "Number of occupied beds, by type.",
	}, []string{"type"})

	// BedsTotal shows total bed capacity.
	BedsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "erflow",
		Name:      "beds_total",
		Help:      "Total beds available, by type.",
	}, []string{"type"})

	// ThrashingActive is 1 when the system is thrashing, 0 otherwise.
	ThrashingActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "erflow",
		Name:      "thrashing_active",
		Help:      "1 if system is thrashing (patient/bed ratio > threshold), 0 otherwise.",
	})

	// PatientBedRatio tracks the current patient-to-bed ratio.
	PatientBedRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "erflow",
		Name:      "patient_bed_ratio",
		Help:      "Current ratio of active patients to total beds.",
	})

	// WorkerPoolActive shows how many doctor workers are currently treating.
	WorkerPoolActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "erflow",
		Name:      "worker_pool_active",
		Help:      "Number of doctor workers currently treating patients.",
	})

	// --- Histograms ---

	// WaitDuration tracks how long patients wait before treatment starts.
	// Labeled by triage and algorithm so you can compare schedulers.
	WaitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "erflow",
		Name:      "wait_duration_seconds",
		Help:      "Time from check-in to treatment start, by triage level and algorithm.",
		Buckets:   []float64{1, 2, 5, 10, 20, 30, 60, 120, 300},
	}, []string{"triage", "algorithm"})

	// TreatmentDuration tracks how long actual treatment takes.
	TreatmentDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "erflow",
		Name:      "treatment_duration_seconds",
		Help:      "Treatment duration from assignment to discharge, by triage level.",
		Buckets:   []float64{2, 5, 10, 15, 20, 30, 45, 60, 90},
	}, []string{"triage"})

	// ThroughputRate tracks discharges per minute.
	ThroughputRate = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "erflow",
		Name:      "throughput_discharges_per_minute",
		Help:      "Current discharge rate (discharges per minute).",
	})

	// SchedulerAlgorithm exposes which algorithm is active (as a label).
	SchedulerAlgorithm = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "erflow",
		Name:      "scheduler_active",
		Help:      "Currently active scheduling algorithm (value=1 for active).",
	}, []string{"algorithm"})
)

// SetActiveScheduler sets the active scheduler metric label.
func SetActiveScheduler(algo string) {
	// Reset all to 0
	for _, a := range []string{"priority", "fcfs", "sjf", "round-robin", "mlfq", "optimal"} {
		SchedulerAlgorithm.WithLabelValues(a).Set(0)
	}
	SchedulerAlgorithm.WithLabelValues(algo).Set(1)
}
