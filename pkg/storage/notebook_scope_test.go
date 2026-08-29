package storage

import "testing"

// TestNotebookScopePredicate covers the prism case that used to bind an empty
// string to a UUID column, which Postgres rejects outright rather than treating
// as "no match".
func TestNotebookScopePredicate(t *testing.T) {
	tests := []struct {
		name      string
		alias     string
		fractalID string
		prismID   string
		wantCol   string
		wantVal   string
		wantErr   bool
	}{
		{"fractal", "", "f-1", "", "fractal_id", "f-1", false},
		{"prism", "", "", "p-1", "prism_id", "p-1", false},
		{"aliased fractal", "n", "f-1", "", "n.fractal_id", "f-1", false},
		{"aliased prism", "n", "", "p-1", "n.prism_id", "p-1", false},
		{"neither", "n", "", "", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, val, err := notebookScopePredicate(tt.alias, tt.fractalID, tt.prismID)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error for an unscoped lookup, got %q = %v", col, val)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if col != tt.wantCol {
				t.Errorf("column = %q, want %q", col, tt.wantCol)
			}
			if val != tt.wantVal {
				t.Errorf("value = %v, want %q", val, tt.wantVal)
			}
		})
	}
}

// TestNotebookLikeEscaper checks a search term's wildcards are neutralised, so a
// notebook name containing % or _ matches literally.
func TestNotebookLikeEscaper(t *testing.T) {
	tests := []struct{ in, want string }{
		{"incident", "incident"},
		{"100%", `100\%`},
		{"a_b", `a\_b`},
		{`back\slash`, `back\\slash`},
	}
	for _, tt := range tests {
		if got := notebookLikeEscaper.Replace(tt.in); got != tt.want {
			t.Errorf("Replace(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
