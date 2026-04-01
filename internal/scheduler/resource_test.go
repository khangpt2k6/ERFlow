package scheduler

import (
	"sync"
	"testing"
)

func TestResourceManager_SelfAcquire(t *testing.T) {
	rm := NewResourceManager()

	// First acquire
	if !rm.TryAcquire(ResLab, "doc-1") {
		t.Fatal("should acquire free resource")
	}
	// Same doctor re-acquiring same resource should succeed (idempotent)
	if !rm.TryAcquire(ResLab, "doc-1") {
		t.Fatal("same doctor should be able to re-acquire own resource")
	}
}

func TestResourceManager_ForceRelease(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")
	rm.RequestAndWait(ResLab, "doc-2") // doc-2 waiting

	rm.ForceRelease(ResLab, "doc-1")

	// doc-1 should no longer hold it
	held := rm.HeldBy("doc-1")
	if len(held) != 0 {
		t.Errorf("doc-1 should hold nothing after ForceRelease, got %v", held)
	}

	// Resource should be free (ForceRelease doesn't grant to waiters — that's Release's job)
	if !rm.TryAcquire(ResLab, "doc-3") {
		t.Fatal("resource should be free after ForceRelease")
	}
}

func TestResourceManager_ForceRelease_RemovesFromWaiters(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")
	rm.RequestAndWait(ResLab, "doc-2")

	// Force release doc-2 (who is a waiter, not holder)
	rm.ForceRelease(ResLab, "doc-2")

	// doc-2 should no longer be waiting
	waitingFor := rm.WaitingFor("doc-2")
	if waitingFor != "" {
		t.Errorf("doc-2 should not be waiting after ForceRelease, got %s", waitingFor)
	}
}

func TestResourceManager_ReleaseAll(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")
	rm.TryAcquire(ResOR, "doc-1")

	rm.ReleaseAll("doc-1")

	held := rm.HeldBy("doc-1")
	if len(held) != 0 {
		t.Errorf("doc-1 should hold nothing after ReleaseAll, got %v", held)
	}
}

func TestResourceManager_ReleaseAll_GrantsToWaiters(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")
	rm.RequestAndWait(ResLab, "doc-2") // doc-2 is first waiter

	rm.ReleaseAll("doc-1")

	// doc-2 should now hold the lab
	held := rm.HeldBy("doc-2")
	found := false
	for _, r := range held {
		if r == ResLab {
			found = true
		}
	}
	if !found {
		t.Error("doc-2 should have been granted the lab after ReleaseAll")
	}
}

func TestResourceManager_HeldBy(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")
	rm.TryAcquire(ResImaging, "doc-1")

	held := rm.HeldBy("doc-1")
	if len(held) != 2 {
		t.Errorf("expected 2 resources held, got %d", len(held))
	}

	held2 := rm.HeldBy("doc-2")
	if len(held2) != 0 {
		t.Errorf("doc-2 should hold nothing, got %d", len(held2))
	}
}

func TestResourceManager_WaitingFor(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResOR, "doc-1")
	rm.RequestAndWait(ResOR, "doc-2")

	wf := rm.WaitingFor("doc-2")
	if wf != ResOR {
		t.Errorf("doc-2 should be waiting for OR, got %s", wf)
	}

	wf2 := rm.WaitingFor("doc-1")
	if wf2 != "" {
		t.Errorf("doc-1 should not be waiting, got %s", wf2)
	}
}

func TestResourceManager_Release_GrantsToFirstWaiter(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")
	rm.RequestAndWait(ResLab, "doc-2")
	rm.RequestAndWait(ResLab, "doc-3")

	rm.Release(ResLab, "doc-1")

	// doc-2 should now hold lab (first waiter)
	held := rm.HeldBy("doc-2")
	found := false
	for _, r := range held {
		if r == ResLab {
			found = true
		}
	}
	if !found {
		t.Error("doc-2 (first waiter) should hold lab after release")
	}

	// doc-3 should still be waiting
	wf := rm.WaitingFor("doc-3")
	if wf != ResLab {
		t.Errorf("doc-3 should still be waiting for lab, got %s", wf)
	}
}

func TestResourceManager_RequestAndWait_NoDuplicates(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")

	// Request multiple times
	rm.RequestAndWait(ResLab, "doc-2")
	rm.RequestAndWait(ResLab, "doc-2")
	rm.RequestAndWait(ResLab, "doc-2")

	// Should only appear once in waiters
	stats := rm.Stats()
	for _, s := range stats {
		if s.Type == ResLab {
			count := 0
			for _, w := range s.WaitedBy {
				if w == "doc-2" {
					count++
				}
			}
			if count != 1 {
				t.Errorf("doc-2 should appear once in waiters, got %d", count)
			}
		}
	}
}

func TestResourceManager_Stats(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")
	rm.RequestAndWait(ResLab, "doc-2")

	stats := rm.Stats()
	if len(stats) != 3 {
		t.Fatalf("expected 3 resource stats, got %d", len(stats))
	}

	for _, s := range stats {
		if s.Type == ResLab {
			if s.HeldBy != "doc-1" {
				t.Errorf("lab should be held by doc-1, got %s", s.HeldBy)
			}
			if len(s.WaitedBy) != 1 || s.WaitedBy[0] != "doc-2" {
				t.Errorf("lab should have doc-2 waiting, got %v", s.WaitedBy)
			}
		}
	}
}

func TestResourceManager_ConcurrentAccess(t *testing.T) {
	rm := NewResourceManager()
	var wg sync.WaitGroup

	// Many goroutines competing for resources
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			doc := "doc-" + string(rune('A'+n%26))
			res := AllResourceTypes()[n%3]

			if rm.TryAcquire(res, doc) {
				_ = rm.HeldBy(doc)
				_ = rm.Stats()
				rm.Release(res, doc)
			} else {
				rm.RequestAndWait(res, doc)
				_ = rm.WaitingFor(doc)
			}
		}(i)
	}

	wg.Wait()
	// No panic under -race = pass
}
