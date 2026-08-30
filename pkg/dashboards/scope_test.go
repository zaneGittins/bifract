package dashboards

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"bifract/pkg/fractals"
)

// TestGetScopeResolvesPrismFirst mirrors the notebook case: a prism session must
// not also resolve the default fractal, or scoped reads and writes land on the
// wrong scope.
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

	h := &DashboardHandler{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/dashboards", nil)
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

// A client that declared no scope must not be resolved to the default fractal.
// The fractal manager is real here, so only the declaration prevents it.
func TestGetScopeRefusesDefaultForDeclaredNoScope(t *testing.T) {
	h := &DashboardHandler{fractalManager: fractals.NewManager(nil, nil)}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/dashboards", nil)
	ctx := fractals.WithNoScope(r.Context())
	ctx = context.WithValue(ctx, "selected_fractal", "")
	ctx = context.WithValue(ctx, "selected_prism", "")

	fractalID, prismID, err := h.getScope(r.WithContext(ctx))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fractalID != "" || prismID != "" {
		t.Errorf("getScope() = (%q, %q), want empty scope", fractalID, prismID)
	}
}
