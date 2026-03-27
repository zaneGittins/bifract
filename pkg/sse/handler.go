package sse

import (
	"fmt"
	"net/http"
	"time"
)

const heartbeatInterval = 30 * time.Second

// ServeSSE registers a client for the given room and streams events until the
// connection closes. This method blocks.
func (h *Hub) ServeSSE(w http.ResponseWriter, r *http.Request, room string, info ClientInfo) *Client {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return nil
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	client := h.Register(room, info)

	// Send the connected event so the frontend knows its client ID.
	connEvent := FormatSSE(Event{
		Type: Connected,
		Data: map[string]string{"client_id": client.ID},
	})
	w.Write(connEvent)
	flusher.Flush()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			h.Unregister(client)
			return client
		case msg, ok := <-client.Send:
			if !ok {
				// Channel closed (hub shutting down).
				return client
			}
			w.Write(msg)
			flusher.Flush()
		case <-ticker.C:
			// Heartbeat comment to keep connection alive through proxies.
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}
