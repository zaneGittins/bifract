package notebooks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestGetScopeResolvesPrismFirst covers the regression where a prism session
// resolved the default fractal as well, and the fractal then won every scoped
// lookup: notebooks created in a prism were stored correctly but listed from
// the default fractal, so they never appeared.
func TestGetScopeResolvesPrismFirst(t *testing.T) {
	tests := []struct {
		name        string
		fractal     string
		prism       string
		wantFractal string
		wantPrism   string
	}{
		{"fractal session", "f-1", "", "f-1", ""},
		{"prism session", "", "p-1", "", "p-1"},
		{"prism wins over a stale fractal", "f-1", "p-1", "", "p-1"},
		{"nothing selected", "", "", "", ""},
	}

	// No fractal manager, so the default-fractal fallback is out of the picture
	// and the context alone decides.
	h := &NotebookHandler{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/notebooks", nil)
			ctx := context.WithValue(r.Context(), "selected_fractal", tt.fractal)
			ctx = context.WithValue(ctx, "selected_prism", tt.prism)

			fractalID, prismID, err := h.getScope(r.WithContext(ctx))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if fractalID != tt.wantFractal || prismID != tt.wantPrism {
				t.Errorf("getScope() = (%q, %q), want (%q, %q)", fractalID, prismID, tt.wantFractal, tt.wantPrism)
			}
		})
	}
}
