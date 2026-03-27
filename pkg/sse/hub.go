package sse

import (
	"sync"

	"github.com/google/uuid"
)

// ClientInfo carries user metadata for presence events.
type ClientInfo struct {
	Username        string `json:"username"`
	DisplayName     string `json:"display_name"`
	GravatarColor   string `json:"gravatar_color"`
	GravatarInitial string `json:"gravatar_initial"`
}

// Client represents a single SSE connection.
type Client struct {
	ID   string
	Info ClientInfo
	Room string
	Send chan []byte
}

// Hub manages room-based pub/sub for SSE connections.
type Hub struct {
	mu    sync.RWMutex
	rooms map[string]map[string]*Client // room -> clientID -> Client
}

// NewHub creates a new SSE hub.
func NewHub() *Hub {
	return &Hub{
		rooms: make(map[string]map[string]*Client),
	}
}

// Register adds a client to a room and returns the client.
func (h *Hub) Register(room string, info ClientInfo) *Client {
	c := &Client{
		ID:   uuid.New().String(),
		Info: info,
		Room: room,
		Send: make(chan []byte, 32),
	}

	h.mu.Lock()
	if h.rooms[room] == nil {
		h.rooms[room] = make(map[string]*Client)
	}
	h.rooms[room][c.ID] = c
	h.mu.Unlock()

	return c
}

// Unregister removes a client from its room and closes its send channel.
func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	room := h.rooms[c.Room]
	if room == nil {
		return
	}
	if _, ok := room[c.ID]; ok {
		delete(room, c.ID)
		close(c.Send)
	}
	if len(room) == 0 {
		delete(h.rooms, c.Room)
	}
}

// Broadcast sends an event to all clients in a room, optionally excluding one
// client (typically the sender, for self-echo filtering).
func (h *Hub) Broadcast(room string, event Event, excludeClientID string) {
	data := FormatSSE(event)
	if data == nil {
		return
	}

	h.mu.RLock()
	clients := h.rooms[room]
	if clients == nil {
		h.mu.RUnlock()
		return
	}
	// Snapshot client list under read lock.
	targets := make([]*Client, 0, len(clients))
	for _, c := range clients {
		if c.ID != excludeClientID {
			targets = append(targets, c)
		}
	}
	h.mu.RUnlock()

	for _, c := range targets {
		select {
		case c.Send <- data:
		default:
			// Drop event for slow client rather than blocking.
		}
	}
}

// RoomClients returns the client info for all clients in a room.
func (h *Hub) RoomClients(room string) []ClientInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	clients := h.rooms[room]
	if clients == nil {
		return nil
	}

	// Deduplicate by username (same user may have multiple tabs).
	seen := make(map[string]bool, len(clients))
	result := make([]ClientInfo, 0, len(clients))
	for _, c := range clients {
		if !seen[c.Info.Username] {
			seen[c.Info.Username] = true
			result = append(result, c.Info)
		}
	}
	return result
}

// Close shuts down the hub, closing all client channels.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for roomKey, clients := range h.rooms {
		for _, c := range clients {
			close(c.Send)
		}
		delete(h.rooms, roomKey)
	}
}
