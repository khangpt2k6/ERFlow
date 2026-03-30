package models

import "time"

// Event represents a logged action in the ER system, tagged with the OS concept it demonstrates.
//
// OS parallel: This is like the kernel's audit log or dmesg ring buffer.
// Every significant kernel action — process creation, context switch, page fault,
// lock acquisition — gets logged so you can trace exactly what happened and why.
type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`              // "patient.checkin", "bed.assigned", "preemption", "race.prevented", etc.
	Message   string    `json:"message"`           // Human-readable description
	Concept   string    `json:"concept"`           // "priority-scheduling", "mutex", "preemption", "aging", "deadlock", etc.
	Details   any       `json:"details,omitempty"` // Arbitrary structured data for the frontend
	Timestamp time.Time `json:"timestamp"`
}
