package scheduler

import "fmt"

// DeadlockInfo holds the result of a deadlock detection pass.
type DeadlockInfo struct {
	Detected bool     `json:"detected"`
	Cycle    []string `json:"cycle"`    // doctor IDs forming the cycle
	Resolved bool     `json:"resolved"`
	Victim   string   `json:"victim"`   // doctor ID that was forced to release
}

// WaitForGraph represents resource dependencies between doctors.
// An edge from A to B means "A is waiting for a resource held by B."
// A cycle in this graph = deadlock.
// OS parallel: this is exactly how OS deadlock detectors work — build the
// wait-for graph from the resource allocation table, then run DFS for cycles.
type WaitForGraph struct {
	edges map[string][]string // doctorID -> []doctorIDs it waits for
}

// NewWaitForGraph creates an empty graph.
func NewWaitForGraph() *WaitForGraph {
	return &WaitForGraph{edges: make(map[string][]string)}
}

// AddEdge adds a directed edge: `from` waits for `to`.
func (g *WaitForGraph) AddEdge(from, to string) {
	g.edges[from] = append(g.edges[from], to)
}

// BuildFromResources constructs the wait-for graph from the resource manager.
// For each resource: if doctor A is waiting and doctor B holds it, add edge A→B.
func (g *WaitForGraph) BuildFromResources(rm *ResourceManager) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	g.edges = make(map[string][]string)
	for _, res := range rm.resources {
		if res.HeldBy == "" {
			continue
		}
		for _, waiter := range res.WaitedBy {
			g.edges[waiter] = append(g.edges[waiter], res.HeldBy)
		}
	}
}

// DetectCycle uses iterative DFS with coloring to find a cycle.
// White = unvisited, Gray = in progress, Black = done.
// If we encounter a gray node, we found a cycle.
func (g *WaitForGraph) DetectCycle() (bool, []string) {
	const (
		white = 0
		gray  = 1
		black = 2
	)

	color := make(map[string]int)
	parent := make(map[string]string)

	// Collect all nodes
	nodes := make(map[string]bool)
	for from, tos := range g.edges {
		nodes[from] = true
		for _, to := range tos {
			nodes[to] = true
		}
	}

	for node := range nodes {
		if color[node] != white {
			continue
		}

		// DFS using a stack
		stack := []string{node}
		for len(stack) > 0 {
			curr := stack[len(stack)-1]

			if color[curr] == white {
				color[curr] = gray
				for _, next := range g.edges[curr] {
					if color[next] == gray {
						// Found a cycle — extract it
						cycle := extractCycle(parent, curr, next)
						return true, cycle
					}
					if color[next] == white {
						parent[next] = curr
						stack = append(stack, next)
					}
				}
			} else {
				// Backtrack
				stack = stack[:len(stack)-1]
				color[curr] = black
			}
		}
	}

	return false, nil
}

// extractCycle traces back from `from` to `to` using the parent map.
func extractCycle(parent map[string]string, from, to string) []string {
	cycle := []string{to, from}
	curr := from
	for curr != to {
		p, ok := parent[curr]
		if !ok {
			break
		}
		cycle = append(cycle, p)
		curr = p
		if len(cycle) > 20 { // safety limit
			break
		}
	}
	return cycle
}

// Edges returns the graph edges for visualization.
func (g *WaitForGraph) Edges() map[string][]string {
	result := make(map[string][]string)
	for k, v := range g.edges {
		copied := make([]string, len(v))
		copy(copied, v)
		result[k] = copied
	}
	return result
}

// ResolveDeadlock picks a victim from the cycle (the doctor with the most held
// resources) and forces them to release all resources.
func ResolveDeadlock(cycle []string, rm *ResourceManager) (string, string) {
	if len(cycle) == 0 {
		return "", ""
	}

	// Pick the last doctor in the cycle as victim (simplest strategy)
	victim := cycle[len(cycle)-1]
	if len(cycle) > 1 {
		victim = cycle[1] // second in cycle — the one who could break it
	}

	held := rm.HeldBy(victim)
	rm.ReleaseAll(victim)

	var releasedStr string
	for _, r := range held {
		if releasedStr != "" {
			releasedStr += ", "
		}
		releasedStr += string(r)
	}

	return victim, fmt.Sprintf("released %s", releasedStr)
}
