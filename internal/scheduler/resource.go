package scheduler

import "sync"

// ResourceType represents shared hospital resources that doctors compete for.
type ResourceType string

const (
	ResLab     ResourceType = "lab"
	ResOR      ResourceType = "or"
	ResImaging ResourceType = "imaging"
)

// AllResourceTypes returns all available resource types.
func AllResourceTypes() []ResourceType {
	return []ResourceType{ResLab, ResOR, ResImaging}
}

// Resource is a shared hospital resource protected by mutual exclusion.
// Only one doctor can hold it at a time — others must wait.
type Resource struct {
	Type     ResourceType `json:"type"`
	HeldBy   string       `json:"heldBy"`   // doctor ID, empty if free
	WaitedBy []string     `json:"waitedBy"` // doctor IDs waiting to acquire
}

// ResourceStats is returned to the frontend for visualization.
type ResourceStats struct {
	Type     ResourceType `json:"type"`
	HeldBy   string       `json:"heldBy"`
	WaitedBy []string     `json:"waitedBy"`
}

// ResourceManager manages shared hospital resources with mutual exclusion.
// OS parallel: like a resource allocation table in an OS — tracks which process
// holds which resource and which processes are waiting.
type ResourceManager struct {
	mu        sync.Mutex
	resources map[ResourceType]*Resource
}

// NewResourceManager creates a manager with Lab, OR, and Imaging resources.
func NewResourceManager() *ResourceManager {
	rm := &ResourceManager{
		resources: make(map[ResourceType]*Resource),
	}
	for _, rt := range AllResourceTypes() {
		rm.resources[rt] = &Resource{Type: rt}
	}
	return rm
}

// TryAcquire attempts a non-blocking acquire. Returns true if the resource was obtained.
func (rm *ResourceManager) TryAcquire(resType ResourceType, doctorID string) bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	res, ok := rm.resources[resType]
	if !ok {
		return false
	}
	if res.HeldBy == "" || res.HeldBy == doctorID {
		res.HeldBy = doctorID
		// If this doctor was waiting for the resource, clear stale wait edge.
		for i := 0; i < len(res.WaitedBy); i++ {
			if res.WaitedBy[i] == doctorID {
				res.WaitedBy = append(res.WaitedBy[:i], res.WaitedBy[i+1:]...)
				break
			}
		}
		return true
	}
	return false
}

// RequestAndWait marks a doctor as waiting for a resource (does not actually block).
// The engine's deadlock detector will check for cycles in the wait-for graph.
func (rm *ResourceManager) RequestAndWait(resType ResourceType, doctorID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	res, ok := rm.resources[resType]
	if !ok {
		return
	}
	// Add to waited-by list if not already present
	for _, id := range res.WaitedBy {
		if id == doctorID {
			return
		}
	}
	res.WaitedBy = append(res.WaitedBy, doctorID)
}

// Release returns a resource. If anyone is waiting, the first waiter gets it.
func (rm *ResourceManager) Release(resType ResourceType, doctorID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	res, ok := rm.resources[resType]
	if !ok || res.HeldBy != doctorID {
		return
	}
	res.HeldBy = ""

	// Grant to the first waiter
	if len(res.WaitedBy) > 0 {
		res.HeldBy = res.WaitedBy[0]
		res.WaitedBy = res.WaitedBy[1:]
	}
}

// ForceRelease forces a doctor to release a resource (used in deadlock resolution).
func (rm *ResourceManager) ForceRelease(resType ResourceType, doctorID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	res, ok := rm.resources[resType]
	if !ok {
		return
	}
	if res.HeldBy == doctorID {
		res.HeldBy = ""
	}
	// Also remove from waiters
	for i, id := range res.WaitedBy {
		if id == doctorID {
			res.WaitedBy = append(res.WaitedBy[:i], res.WaitedBy[i+1:]...)
			break
		}
	}
}

// ReleaseAll releases all resources held by a doctor.
func (rm *ResourceManager) ReleaseAll(doctorID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for _, res := range rm.resources {
		if res.HeldBy == doctorID {
			res.HeldBy = ""
			if len(res.WaitedBy) > 0 {
				res.HeldBy = res.WaitedBy[0]
				res.WaitedBy = res.WaitedBy[1:]
			}
		}
		// Remove from waiters
		for i, id := range res.WaitedBy {
			if id == doctorID {
				res.WaitedBy = append(res.WaitedBy[:i], res.WaitedBy[i+1:]...)
				break
			}
		}
	}
}

// Stats returns the state of all resources.
func (rm *ResourceManager) Stats() []ResourceStats {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	var stats []ResourceStats
	for _, res := range rm.resources {
		waited := make([]string, len(res.WaitedBy))
		copy(waited, res.WaitedBy)
		stats = append(stats, ResourceStats{
			Type:     res.Type,
			HeldBy:   res.HeldBy,
			WaitedBy: waited,
		})
	}
	return stats
}

// HeldBy returns which resources a doctor currently holds.
func (rm *ResourceManager) HeldBy(doctorID string) []ResourceType {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	var held []ResourceType
	for _, res := range rm.resources {
		if res.HeldBy == doctorID {
			held = append(held, res.Type)
		}
	}
	return held
}

// WaitingFor returns the resource a doctor is waiting for (empty if not waiting).
func (rm *ResourceManager) WaitingFor(doctorID string) ResourceType {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for _, res := range rm.resources {
		for _, id := range res.WaitedBy {
			if id == doctorID {
				return res.Type
			}
		}
	}
	return ""
}
