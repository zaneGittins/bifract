package query

import (
	"context"
	"testing"

	"bifract/pkg/storage"
)

// TestRequestTimezone covers the precedence a query's bucket zone follows: an
// explicit valid zone (a notebook pinning its own) beats the caller's display
// zone, and an unusable one falls back rather than failing the search.
func TestRequestTimezone(t *testing.T) {
	viewer := context.WithValue(context.Background(), "user",
		&storage.User{DisplayTimezone: "America/Denver"})
	anon := context.Background()

	tests := []struct {
		name      string
		requested string
		ctx       context.Context
		want      string
	}{
		{"pinned zone wins", "Europe/Berlin", viewer, "Europe/Berlin"},
		{"pinned zone without a viewer", "Europe/Berlin", anon, "Europe/Berlin"},
		{"no pin falls back to the viewer", "", viewer, "America/Denver"},
		{"no pin and no viewer", "", anon, ""},
		{"whitespace is not a pin", "   ", viewer, "America/Denver"},
		{"unknown zone falls back", "Mars/Olympus", viewer, "America/Denver"},
		{"injection attempt falls back", "x' OR 1=1 --", viewer, "America/Denver"},
		{"Local is refused", "Local", viewer, "America/Denver"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requestTimezone(tt.ctx, tt.requested); got != tt.want {
				t.Errorf("requestTimezone(%q) = %q, want %q", tt.requested, got, tt.want)
			}
		})
	}
}
