package sse

import "encoding/json"

// Event types for notebooks.
const (
	SectionAdded          = "section_added"
	SectionRemoved        = "section_removed"
	SectionUpdated        = "section_updated"
	SectionResultsUpdated = "section_results_updated"
	SectionsReordered     = "sections_reordered"
)

// Event types for dashboards.
const (
	WidgetAdded          = "widget_added"
	WidgetRemoved        = "widget_removed"
	WidgetUpdated        = "widget_updated"
	WidgetResultsUpdated = "widget_results_updated"
	WidgetLayoutUpdated  = "widget_layout_updated"
)

// Event types for presence and connection.
const (
	PresenceJoined = "presence_joined"
	PresenceLeft   = "presence_left"
	Connected      = "connected"
)

// Event represents a server-sent event with a type and payload.
type Event struct {
	Type   string      `json:"type"`
	Data   interface{} `json:"data"`
	Sender string      `json:"sender,omitempty"`
}

// FormatSSE serializes an Event into the SSE wire format: "data: {json}\n\n".
func FormatSSE(e Event) []byte {
	payload, err := json.Marshal(e)
	if err != nil {
		return nil
	}
	// "data: " + json + "\n\n"
	buf := make([]byte, 0, 6+len(payload)+2)
	buf = append(buf, "data: "...)
	buf = append(buf, payload...)
	buf = append(buf, '\n', '\n')
	return buf
}
