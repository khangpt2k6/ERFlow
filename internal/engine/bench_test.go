package engine

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erflow/backend/internal/models"
	"github.com/erflow/backend/internal/scheduler"
	"github.com/erflow/backend/internal/store"
)

// =========================================================================
// ERFlow Concurrency Benchmark Suite
// Measures: throughput, goroutine count, channel ops/sec, mutex contention
// =========================================================================

// BenchmarkWorkerPoolThroughput measures how many jobs/sec the worker pool can process.
func BenchmarkWorkerPoolThroughput(b *testing.B) {
	st := store.NewMemStore()
	results := make(chan discharge, 100)
	var ctxSwitches atomic.Int64
	thrashing := NewThrashingMonitor(2.0)

	// Use instant sleep to measure pure scheduling overhead
	instantSleep := func(ctx context.Context, d time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}

	for _, workers := range []int{1, 3, 5, 10, 20} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			pool := NewWorkerPool(workers, results, st, instantSleep, &ctxSwitches, thrashing)
			ctx, cancel := context.WithCancel(context.Background())
			pool.Start(ctx)

			// Drain results
			go func() {
				for range results {
				}
			}()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				job := TreatmentJob{
					Patient: &models.Patient{
						ID:                 fmt.Sprintf("p-%d", i),
						Name:               "Bench Patient",
						Status:             models.StatusInTreatment,
						EstimatedDuration:  1 * time.Millisecond,
						RemainingTreatment: 1 * time.Millisecond,
					},
					BedID:    "bed-g1",
					DoctorID: "doc-1",
				}
				pool.Submit(job)
			}
			b.StopTimer()

			cancel()
			pool.Stop()
		})
	}
}

// BenchmarkChannelPipeline measures the raw channel throughput of the arrival->discharge pipeline.
func BenchmarkChannelPipeline(b *testing.B) {
	for _, bufSize := range []int{1, 10, 20, 50, 100} {
		b.Run(fmt.Sprintf("buf=%d", bufSize), func(b *testing.B) {
			ch := make(chan arrival, bufSize)
			var count atomic.Int64

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case _, ok := <-ch:
						if !ok {
							return
						}
						count.Add(1)
					}
				}
			}()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ch <- arrival{
					patient:   &models.Patient{ID: fmt.Sprintf("p-%d", i)},
					complaint: "bench",
				}
			}
			b.StopTimer()

			cancel()
			close(ch)
			wg.Wait()
		})
	}
}

// BenchmarkAtomicCounters measures atomic operations throughput (lock-free stats).
func BenchmarkAtomicCounters(b *testing.B) {
	var counter atomic.Int64

	b.Run("single-goroutine", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			counter.Add(1)
			counter.Load()
		}
	})

	for _, goroutines := range []int{4, 8, 16, 32} {
		b.Run(fmt.Sprintf("goroutines=%d", goroutines), func(b *testing.B) {
			var wg sync.WaitGroup
			perG := b.N / goroutines
			b.ResetTimer()
			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perG; i++ {
						counter.Add(1)
						counter.Load()
					}
				}()
			}
			wg.Wait()
		})
	}
}

// BenchmarkRWMutex simulates MemStore read/write contention.
func BenchmarkRWMutex(b *testing.B) {
	var mu sync.RWMutex
	data := make(map[string]int)
	data["key"] = 42

	b.Run("read-heavy-90/10", func(b *testing.B) {
		var wg sync.WaitGroup
		perG := b.N / 10
		b.ResetTimer()

		// 9 readers
		for r := 0; r < 9; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perG; i++ {
					mu.RLock()
					_ = data["key"]
					mu.RUnlock()
				}
			}()
		}
		// 1 writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				mu.Lock()
				data["key"] = i
				mu.Unlock()
			}
		}()
		wg.Wait()
	})

	b.Run("write-heavy-50/50", func(b *testing.B) {
		var wg sync.WaitGroup
		perG := b.N / 10
		b.ResetTimer()

		for g := 0; g < 10; g++ {
			wg.Add(1)
			isWriter := g%2 == 0
			go func() {
				defer wg.Done()
				for i := 0; i < perG; i++ {
					if isWriter {
						mu.Lock()
						data["key"] = i
						mu.Unlock()
					} else {
						mu.RLock()
						_ = data["key"]
						mu.RUnlock()
					}
				}
			}()
		}
		wg.Wait()
	})
}

// BenchmarkSemaphore measures semaphore acquire/release throughput.
func BenchmarkSemaphore(b *testing.B) {
	for _, capacity := range []int{2, 5, 10, 17} {
		b.Run(fmt.Sprintf("cap=%d", capacity), func(b *testing.B) {
			sem := scheduler.NewBedSemaphore("bench", capacity)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sem.Acquire(ctx)
				sem.Release()
			}
		})
	}
}

// BenchmarkSchedulerEnqueueDequeue measures scheduler throughput for different algorithms.
func BenchmarkSchedulerEnqueueDequeue(b *testing.B) {
	algos := []scheduler.Algorithm{
		scheduler.AlgoPriority,
		scheduler.AlgoRoundRobin,
		scheduler.AlgoMLFQ,
		scheduler.AlgoOptimal,
	}

	for _, algo := range algos {
		b.Run(string(algo), func(b *testing.B) {
			sched := scheduler.NewScheduler(algo)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p := &models.Patient{
					ID:       fmt.Sprintf("p-%d", i),
					Name:     "Bench",
					EffectivePri: i % 5,
					Status:   models.StatusWaiting,
				}
				sched.Enqueue(p)
				sched.Dequeue()
			}
		})
	}
}

// TestConcurrencyReport is the main report — runs the system and prints stats.
func TestConcurrencyReport(t *testing.T) {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║           ERFlow Concurrency Performance Report             ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// --- 1. System Info ---
	fmt.Println("┌─ System Info ──────────────────────────────────────────────┐")
	fmt.Printf("│  CPU Cores (logical):  %d\n", runtime.NumCPU())
	fmt.Printf("│  GOMAXPROCS:           %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("│  Go Version:           %s\n", runtime.Version())
	fmt.Println("└────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// --- 2. Goroutine Census ---
	st := store.NewMemStore()
	eng := New(st)

	goroutinesBefore := runtime.NumGoroutine()
	eng.Start()
	time.Sleep(200 * time.Millisecond) // let all goroutines spin up
	goroutinesAfter := runtime.NumGoroutine()
	engineGoroutines := goroutinesAfter - goroutinesBefore

	fmt.Println("┌─ Goroutine Census ────────────────────────────────────────┐")
	fmt.Printf("│  Before engine start:  %d goroutines\n", goroutinesBefore)
	fmt.Printf("│  After engine start:   %d goroutines\n", goroutinesAfter)
	fmt.Printf("│  Engine spawned:       %d goroutines\n", engineGoroutines)
	fmt.Println("│")
	fmt.Println("│  Breakdown:")
	fmt.Println("│    • 1x patientGenerator    (producer)")
	fmt.Println("│    • 1x schedulerLoop       (dispatcher)")
	fmt.Println("│    • 1x treatmentSimulator   (progress tracker)")
	fmt.Println("│    • 1x agingDaemon          (priority boost)")
	fmt.Println("│    • 1x throughputTracker    (thrashing monitor)")
	fmt.Println("│    • 1x dischargeHandler     (consumer)")
	fmt.Println("│    • 1x deadlockDetector     (cycle finder)")
	fmt.Printf("│    • %dx doctor workers       (thread pool)\n", len(st.GetAllDoctors()))
	fmt.Println("└────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// --- 3. Channel Pipeline Stress Test ---
	fmt.Println("┌─ Channel Pipeline Stress Test ────────────────────────────┐")

	// Test raw channel throughput
	ch := make(chan arrival, 20)
	var received atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				received.Add(1)
			}
		}
	}()

	const msgCount = 100_000
	start := time.Now()
	for i := 0; i < msgCount; i++ {
		ch <- arrival{
			patient:   &models.Patient{ID: fmt.Sprintf("stress-%d", i)},
			complaint: "bench",
		}
	}
	elapsed := time.Since(start)
	cancel()
	close(ch)

	opsPerSec := float64(msgCount) / elapsed.Seconds()
	fmt.Printf("│  Messages sent:        %d\n", msgCount)
	fmt.Printf("│  Time:                 %v\n", elapsed.Round(time.Microsecond))
	fmt.Printf("│  Throughput:           %.0f msgs/sec\n", opsPerSec)
	fmt.Printf("│  Latency per msg:      %v\n", elapsed/msgCount)
	fmt.Println("└────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// --- 4. Worker Pool Throughput ---
	fmt.Println("┌─ Worker Pool Throughput (instant sleep) ──────────────────┐")

	for _, numWorkers := range []int{1, 3, 5, 10} {
		testStore := store.NewMemStore()
		results := make(chan discharge, 1000)
		var ctxSwitches atomic.Int64
		thrashing := NewThrashingMonitor(2.0)

		instantSleep := func(ctx context.Context, d time.Duration) bool {
			return true
		}

		pool := NewWorkerPool(numWorkers, results, testStore, instantSleep, &ctxSwitches, thrashing)
		poolCtx, poolCancel := context.WithCancel(context.Background())
		pool.Start(poolCtx)

		// Drain results
		var drained atomic.Int64
		go func() {
			for range results {
				drained.Add(1)
			}
		}()

		const jobCount = 10_000
		submitted := 0
		start := time.Now()
		for i := 0; i < jobCount; i++ {
			job := TreatmentJob{
				Patient: &models.Patient{
					ID:                 fmt.Sprintf("wp-%d", i),
					Name:               "Bench",
					Status:             models.StatusInTreatment,
					EstimatedDuration:  0,
					RemainingTreatment: 0,
				},
				BedID:    "bed-g1",
				DoctorID: "doc-1",
			}
			if pool.Submit(job) {
				submitted++
			}
		}
		// Wait for completion
		time.Sleep(100 * time.Millisecond)
		elapsed := time.Since(start)
		completed := drained.Load()

		fmt.Printf("│  Workers: %-3d  Submitted: %-6d  Completed: %-6d  %.0f jobs/sec\n",
			numWorkers, submitted, completed, float64(completed)/elapsed.Seconds())

		poolCancel()
		pool.Stop()
		close(results)
	}
	fmt.Println("└────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// --- 5. Atomic Operations Throughput ---
	fmt.Println("┌─ Atomic Counter Throughput ────────────────────────────────┐")
	{
		var counter atomic.Int64
		const ops = 1_000_000
		start := time.Now()
		for i := 0; i < ops; i++ {
			counter.Add(1)
		}
		elapsed := time.Since(start)
		fmt.Printf("│  Single goroutine:     %d ops in %v = %.0f ops/sec\n",
			ops, elapsed.Round(time.Microsecond), float64(ops)/elapsed.Seconds())

		counter.Store(0)
		start = time.Now()
		var wg sync.WaitGroup
		numG := runtime.NumCPU()
		perG := ops / numG
		for g := 0; g < numG; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perG; i++ {
					counter.Add(1)
				}
			}()
		}
		wg.Wait()
		elapsed = time.Since(start)
		fmt.Printf("│  %d goroutines:       %d ops in %v = %.0f ops/sec\n",
			numG, ops, elapsed.Round(time.Microsecond), float64(ops)/elapsed.Seconds())
	}
	fmt.Println("└────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// --- 6. Semaphore Throughput ---
	fmt.Println("┌─ Semaphore Acquire/Release Throughput ────────────────────┐")
	{
		for _, cap := range []int{2, 5, 10, 17} {
			sem := scheduler.NewBedSemaphore("bench", cap)
			const ops = 100_000
			start := time.Now()
			for i := 0; i < ops; i++ {
				sem.Acquire(context.Background())
				sem.Release()
			}
			elapsed := time.Since(start)
			fmt.Printf("│  Capacity %-3d:  %d ops in %v = %.0f ops/sec\n",
				cap, ops, elapsed.Round(time.Microsecond), float64(ops)/elapsed.Seconds())
		}
	}
	fmt.Println("└────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// --- 7. Scheduler Throughput ---
	fmt.Println("┌─ Scheduler Enqueue/Dequeue Throughput ────────────────────┐")
	{
		algos := []scheduler.Algorithm{
			scheduler.AlgoPriority,
			scheduler.AlgoRoundRobin,
			scheduler.AlgoMLFQ,
			scheduler.AlgoOptimal,
		}
		for _, algo := range algos {
			sched := scheduler.NewScheduler(algo)
			const ops = 50_000
			start := time.Now()
			for i := 0; i < ops; i++ {
				p := &models.Patient{
					ID:       fmt.Sprintf("s-%d", i),
					Name:     "Bench",
					EffectivePri: i % 5,
					Status:   models.StatusWaiting,
				}
				sched.Enqueue(p)
				sched.Dequeue()
			}
			elapsed := time.Since(start)
			fmt.Printf("│  %-15s  %d ops in %v = %.0f ops/sec\n",
				algo, ops, elapsed.Round(time.Microsecond), float64(ops)/elapsed.Seconds())
		}
	}
	fmt.Println("└────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// --- 8. Concurrent RWMutex Stress ---
	fmt.Println("┌─ RWMutex Contention (simulating MemStore) ───────────────┐")
	{
		var mu sync.RWMutex
		data := map[string]int{"patients": 0}
		const opsPerWorker = 50_000
		readers := 8
		writers := 2

		var wg sync.WaitGroup
		start := time.Now()
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < opsPerWorker; i++ {
					mu.RLock()
					_ = data["patients"]
					mu.RUnlock()
				}
			}()
		}
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < opsPerWorker; i++ {
					mu.Lock()
					data["patients"]++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		totalOps := opsPerWorker * (readers + writers)
		fmt.Printf("│  %d readers + %d writers, %d ops each\n", readers, writers, opsPerWorker)
		fmt.Printf("│  Total: %d ops in %v = %.0f ops/sec\n",
			totalOps, elapsed.Round(time.Microsecond), float64(totalOps)/elapsed.Seconds())
	}
	fmt.Println("└────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// --- 9. Full System Integration Test ---
	fmt.Println("┌─ Full System Integration (5s run) ────────────────────────┐")
	{
		intStore := store.NewMemStore()
		intStore.OnEvent = func(string, any) {} // no-op SSE
		intEng := New(intStore)
		intEng.SetSpeed(10.0) // 10x speed for faster benchmark

		intEng.Start()
		time.Sleep(5 * time.Second)
		intEng.Stop()

		stats := intEng.Stats()
		poolStats := intEng.pool.Stats()

		arrivals := stats["totalArrivals"].(int64)
		discharged := stats["totalDischarged"].(int64)
		preemptions := stats["totalPreemptions"].(int64)
		ctxSw := stats["totalContextSwitches"].(int64)

		fmt.Printf("│  Duration:             5 seconds (10x speed)\n")
		fmt.Printf("│  Patients arrived:     %d\n", arrivals)
		fmt.Printf("│  Patients discharged:  %d\n", discharged)
		fmt.Printf("│  Preemptions:          %d\n", preemptions)
		fmt.Printf("│  Context switches:     %d\n", ctxSw)
		fmt.Printf("│  Worker pool completed:%d\n", poolStats.TotalCompleted)

		if arrivals > 0 {
			throughput := float64(discharged) / 5.0
			fmt.Printf("│  Throughput:           %.1f discharges/sec\n", throughput)
		}
	}
	fmt.Println("└────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// --- Stop engine ---
	eng.Stop()

	// --- Summary ---
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                   Concurrency Summary                       ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Goroutines (engine):    ~%d concurrent                     ║\n", engineGoroutines)
	fmt.Printf("║  OS Threads:             %d logical CPUs available          ║\n", runtime.NumCPU())
	fmt.Println("║  Channels:              5 buffered + N client SSE           ║")
	fmt.Println("║  Mutexes:               4 RWMutex + 7 Mutex                ║")
	fmt.Println("║  Atomics:               9 lock-free counters               ║")
	fmt.Println("║  Semaphores:            3 (General=10, ICU=5, Trauma=2)    ║")
	fmt.Println("║  WaitGroups:            2 (engine + worker pool)           ║")
	fmt.Println("║                                                             ║")
	fmt.Println("║  Concurrency Patterns Used:                                 ║")
	fmt.Println("║    ✓ CSP (Communicating Sequential Processes)               ║")
	fmt.Println("║    ✓ Worker Pool (bounded goroutine concurrency)            ║")
	fmt.Println("║    ✓ Pipeline (generator → scheduler → worker → discharge) ║")
	fmt.Println("║    ✓ Fan-out (multiple doctor workers from one job queue)   ║")
	fmt.Println("║    ✓ Counting Semaphores (bed capacity limits)             ║")
	fmt.Println("║    ✓ RWMutex (reader-heavy state protection)               ║")
	fmt.Println("║    ✓ Atomic counters (lock-free statistics)                ║")
	fmt.Println("║    ✓ Context-based cancellation (graceful shutdown)         ║")
	fmt.Println("║    ✓ Deadlock detection (wait-for graph cycle finder)      ║")
	fmt.Println("║    ✓ SSE (Server-Sent Events for real-time push)           ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
}
