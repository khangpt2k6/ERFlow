package engine

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/store"
)

// LoadGenerator produces patients at a configurable rate (RPS).
// Unlike the old patientGenerator that creates 1 patient every 2-4s,
// this can push 100-10,000+ patients/sec to stress the system.
type LoadGenerator struct {
	store    *store.MemStore
	arrivals chan arrival

	rps       atomic.Int64 // target requests per second (0 = paused)
	generated atomic.Int64
	rejected  atomic.Int64 // rejected by backpressure (channel full)
}

func NewLoadGenerator(st *store.MemStore, arrivals chan arrival) *LoadGenerator {
	return &LoadGenerator{
		store:    st,
		arrivals: arrivals,
	}
}

// SetRPS updates the target generation rate. 0 = paused.
func (lg *LoadGenerator) SetRPS(rps int64) {
	lg.rps.Store(rps)
}

// GetRPS returns the current target RPS.
func (lg *LoadGenerator) GetRPS() int64 {
	return lg.rps.Load()
}

// Run is the load generator goroutine. Uses a ticker-based batch approach
// for high RPS accuracy.
func (lg *LoadGenerator) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// Tick 100 times per second; generate rps/100 patients per tick.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	nameIdx := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rps := lg.rps.Load()
			if rps <= 0 {
				continue
			}

			// Batch size per tick: rps / 100 ticks per second
			batch := int(rps / 100)
			if batch < 1 && rps > 0 {
				// For low RPS (1-99), probabilistic: generate 1 with probability rps/100
				if rand.Int63n(100) < rps {
					batch = 1
				} else {
					continue
				}
			}

			for i := 0; i < batch; i++ {
				p := lg.generatePatient(&nameIdx)
				lg.generated.Add(1)

				select {
				case lg.arrivals <- arrival{patient: p, complaint: p.Complaint}:
				default:
					lg.rejected.Add(1) // backpressure — channel full
				}
			}
		}
	}
}

func (lg *LoadGenerator) generatePatient(nameIdx *int) *models.Patient {
	triage := weightedTriage()
	name := patientNames[*nameIdx%len(patientNames)]
	*nameIdx++
	complaintList := complaints[triage]
	complaint := complaintList[rand.Intn(len(complaintList))]

	id := lg.store.NextPatientID()
	p := models.NewPatient(id, name, triage, complaint)
	lg.store.AddPatient(p)

	return p
}

// Stats returns generator metrics.
func (lg *LoadGenerator) Stats() LoadGenStats {
	return LoadGenStats{
		TargetRPS: lg.rps.Load(),
		Generated: lg.generated.Load(),
		Rejected:  lg.rejected.Load(),
	}
}

// Reset clears counters.
func (lg *LoadGenerator) Reset() {
	lg.generated.Store(0)
	lg.rejected.Store(0)
}

type LoadGenStats struct {
	TargetRPS int64 `json:"targetRps"`
	Generated int64 `json:"generated"`
	Rejected  int64 `json:"rejected"`
}

// ThroughputRecorder tracks actual throughput over time windows.
type ThroughputRecorder struct {
	mu         sync.Mutex
	samples    []ThroughputSample
	maxSamples int
	lastCount  int64
	lastTime   time.Time
}

type ThroughputSample struct {
	Timestamp  time.Time `json:"timestamp"`
	TasksPerSec float64  `json:"tasksPerSec"`
	QueueDepth int       `json:"queueDepth"`
	Workers    int       `json:"workers"`
	Shedding   bool      `json:"shedding"`
}

func NewThroughputRecorder(maxSamples int) *ThroughputRecorder {
	return &ThroughputRecorder{
		maxSamples: maxSamples,
		lastTime:   time.Now(),
	}
}

// Record takes a snapshot of current throughput.
func (tr *ThroughputRecorder) Record(completedTotal int64, queueDepth, workers int, shedding bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	elapsed := time.Since(tr.lastTime).Seconds()
	tps := 0.0
	if elapsed > 0 {
		tps = float64(completedTotal-tr.lastCount) / elapsed
	}

	tr.samples = append(tr.samples, ThroughputSample{
		Timestamp:   time.Now(),
		TasksPerSec: tps,
		QueueDepth:  queueDepth,
		Workers:     workers,
		Shedding:    shedding,
	})
	if len(tr.samples) > tr.maxSamples {
		tr.samples = tr.samples[1:]
	}

	tr.lastCount = completedTotal
	tr.lastTime = time.Now()
}

// Samples returns a copy of all recorded samples.
func (tr *ThroughputRecorder) Samples() []ThroughputSample {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	out := make([]ThroughputSample, len(tr.samples))
	copy(out, tr.samples)
	return out
}

// CurrentTPS returns the most recent tasks/sec measurement.
func (tr *ThroughputRecorder) CurrentTPS() float64 {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.samples) == 0 {
		return 0
	}
	return tr.samples[len(tr.samples)-1].TasksPerSec
}

func (tr *ThroughputRecorder) Reset() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.samples = nil
	tr.lastCount = 0
	tr.lastTime = time.Now()
}
