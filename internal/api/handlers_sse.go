package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/erflow/backend/internal/engine"
	"github.com/erflow/backend/internal/store"
)

// SSEBroker manages connected SSE clients and broadcasts events.
// OS parallel: interrupt-driven I/O — the server pushes data when available
// instead of the client polling. Each client is a channel (like a device IRQ line).
type SSEBroker struct {
	mu         sync.RWMutex
	clients    map[chan []byte]struct{}
	register   chan chan []byte
	unregister chan chan []byte
}

// NewSSEBroker creates a new broker.
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		clients:    make(map[chan []byte]struct{}),
		register:   make(chan chan []byte, 10),
		unregister: make(chan chan []byte, 10),
	}
}

// Run is the broker's main loop — manages client registration and event fan-out.
func (b *SSEBroker) Run() {
	for {
		select {
		case ch := <-b.register:
			b.mu.Lock()
			b.clients[ch] = struct{}{}
			b.mu.Unlock()
			log.Printf("[SSE] Client connected (%d total)", len(b.clients))

		case ch := <-b.unregister:
			b.mu.Lock()
			delete(b.clients, ch)
			close(ch)
			b.mu.Unlock()
			log.Printf("[SSE] Client disconnected (%d total)", len(b.clients))
		}
	}
}

// Subscribe creates a new client channel.
func (b *SSEBroker) Subscribe() chan []byte {
	ch := make(chan []byte, 64)
	b.register <- ch
	return ch
}

// Unsubscribe removes a client channel.
func (b *SSEBroker) Unsubscribe(ch chan []byte) {
	b.unregister <- ch
}

// Publish sends data to all connected clients (non-blocking per client).
func (b *SSEBroker) Publish(eventType string, data any) {
	payload, err := json.Marshal(map[string]any{
		"type": eventType,
		"data": data,
		"time": time.Now().Format(time.RFC3339),
	})
	if err != nil {
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.clients {
		select {
		case ch <- payload:
		default:
			// Client is slow — skip to avoid blocking
		}
	}
}

// ClientCount returns number of connected clients.
func (b *SSEBroker) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// SSEHandler handles the SSE stream endpoint.
type SSEHandler struct {
	store  *store.MemStore
	engine *engine.Engine
	Broker *SSEBroker
}

// NewSSEHandler creates the handler and starts the broker.
func NewSSEHandler(s *store.MemStore, eng *engine.Engine) *SSEHandler {
	broker := NewSSEBroker()
	go broker.Run()
	return &SSEHandler{store: s, engine: eng, Broker: broker}
}

// Stream handles GET /api/events/stream — the SSE endpoint.
// Uses http.Flusher to push events as they happen.
func (h *SSEHandler) Stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "http://localhost:5173")

	ch := h.Broker.Subscribe()
	defer h.Broker.Unsubscribe(ch)

	// Send initial state snapshot
	initial, _ := json.Marshal(map[string]any{
		"type": "snapshot",
		"data": map[string]any{
			"engine": h.engine.Stats(),
		},
		"time": time.Now().Format(time.RFC3339),
	})
	fmt.Fprintf(w, "data: %s\n\n", initial)
	flusher.Flush()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
