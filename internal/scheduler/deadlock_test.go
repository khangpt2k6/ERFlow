package scheduler

import (
	"testing"
)

func TestWaitForGraph_DetectCycle(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge("doc-1", "doc-2") // doc-1 waits for doc-2
	g.AddEdge("doc-2", "doc-1") // doc-2 waits for doc-1 — cycle!

	detected, cycle := g.DetectCycle()
	if !detected {
		t.Fatal("should detect cycle")
	}
	if len(cycle) < 2 {
		t.Fatalf("cycle should have at least 2 nodes, got %v", cycle)
	}
}

func TestWaitForGraph_NoCycle(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge("doc-1", "doc-2") // doc-1 waits for doc-2
	g.AddEdge("doc-2", "doc-3") // doc-2 waits for doc-3 — no cycle (DAG)

	detected, _ := g.DetectCycle()
	if detected {
		t.Fatal("should not detect cycle in a DAG")
	}
}

func TestResourceManager_AcquireRelease(t *testing.T) {
	rm := NewResourceManager()

	if !rm.TryAcquire(ResLab, "doc-1") {
		t.Fatal("should acquire free resource")
	}
	if rm.TryAcquire(ResLab, "doc-2") {
		t.Fatal("should not acquire held resource")
	}

	rm.Release(ResLab, "doc-1")
	if !rm.TryAcquire(ResLab, "doc-2") {
		t.Fatal("should acquire after release")
	}
}

func TestResourceManager_WaitAndDetect(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")
	rm.TryAcquire(ResOR, "doc-2")
	rm.RequestAndWait(ResOR, "doc-1")   // doc-1 holds lab, wants OR
	rm.RequestAndWait(ResLab, "doc-2")  // doc-2 holds OR, wants lab

	g := NewWaitForGraph()
	g.BuildFromResources(rm)

	detected, cycle := g.DetectCycle()
	if !detected {
		t.Fatal("should detect deadlock")
	}
	if len(cycle) < 2 {
		t.Fatalf("cycle too short: %v", cycle)
	}
}

func TestResolveDeadlock(t *testing.T) {
	rm := NewResourceManager()

	rm.TryAcquire(ResLab, "doc-1")
	rm.TryAcquire(ResOR, "doc-2")
	rm.RequestAndWait(ResOR, "doc-1")
	rm.RequestAndWait(ResLab, "doc-2")

	g := NewWaitForGraph()
	g.BuildFromResources(rm)
	detected, cycle := g.DetectCycle()
	if !detected {
		t.Fatal("expected deadlock")
	}

	victim, action := ResolveDeadlock(cycle, rm)
	if victim == "" {
		t.Fatal("should pick a victim")
	}
	if action == "" {
		t.Fatal("should describe action")
	}

	// After resolution, the cycle should be broken
	g2 := NewWaitForGraph()
	g2.BuildFromResources(rm)
	detected2, _ := g2.DetectCycle()
	if detected2 {
		t.Fatal("deadlock should be resolved")
	}
}
