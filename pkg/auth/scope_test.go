package auth

import "testing"

func TestParseScopeHeader(t *testing.T) {
	tests := []struct {
		name        string
		header      string
		wantFractal string
		wantPrism   string
		wantErr     bool
	}{
		{name: "fractal", header: "fractal:abc-123", wantFractal: "abc-123"},
		{name: "prism", header: "prism:abc-123", wantPrism: "abc-123"},
		{name: "surrounding space", header: "  fractal:abc-123  ", wantFractal: "abc-123"},
		{name: "explicit none", header: ScopeNone},
		{name: "none with space", header: "  none  "},
		{name: "none is not a kind", header: "none:abc-123", wantErr: true},
		{name: "unknown kind", header: "tenant:abc-123", wantErr: true},
		{name: "no separator", header: "abc-123", wantErr: true},
		{name: "empty id", header: "fractal:", wantErr: true},
		{name: "empty header", header: "", wantErr: true},
		{name: "id too long", header: "fractal:" + string(make([]byte, 37)), wantErr: true},
		{name: "quote in id", header: "fractal:a'b", wantErr: true},
		{name: "sql metachar in id", header: "fractal:a b OR 1=1", wantErr: true},
		{name: "path traversal in id", header: "fractal:../admin", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fractal, prism, err := parseScopeHeader(tc.header)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseScopeHeader(%q) = (%q, %q, nil), want error", tc.header, fractal, prism)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseScopeHeader(%q) returned unexpected error: %v", tc.header, err)
			}
			if fractal != tc.wantFractal || prism != tc.wantPrism {
				t.Fatalf("parseScopeHeader(%q) = (%q, %q), want (%q, %q)",
					tc.header, fractal, prism, tc.wantFractal, tc.wantPrism)
			}
			if fractal != "" && prism != "" {
				t.Fatalf("parseScopeHeader(%q) set both scopes", tc.header)
			}
		})
	}
}

// A client holding a scope it can no longer use must still be able to
// authenticate, list what it can reach, and select a new scope. Without these
// exemptions a stale tab is locked out with no way to recover.
func TestScopeHeaderExempt(t *testing.T) {
	exempt := []string{
		"/api/v1/auth/logout",
		"/api/v1/auth/user",
		"/api/v1/fractals",
		"/api/v1/prisms",
		"/api/v1/fractals/abc-123/select",
		"/api/v1/prisms/abc-123/select",
	}
	for _, path := range exempt {
		if !scopeHeaderExempt(path) {
			t.Errorf("scopeHeaderExempt(%q) = false, want true", path)
		}
	}

	enforced := []string{
		"/api/v1/query",
		"/api/v1/notebooks",
		"/api/v1/dashboards",
		"/api/v1/alerts",
		"/api/v1/fractals/abc-123",
		"/api/v1/fractals/abc-123/retention",
	}
	for _, path := range enforced {
		if scopeHeaderExempt(path) {
			t.Errorf("scopeHeaderExempt(%q) = true, want false", path)
		}
	}
}
