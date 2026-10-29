package query

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/rbac"
)

func observedRollup(e *testEnv) {
	e.ch.On("FROM logs_field_paths", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "path", Type: "String"}, {Column: "n", Type: "UInt64"}},
		Rows: [][]any{{"", uint64(10)}, {"host", uint64(9)}, {"user", uint64(2)}},
	})
}

func getObserved(t *testing.T, h *QueryHandler, target string, vals ctxVals) (*httptest.ResponseRecorder, ObservedFieldsResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	r := testRequest(http.MethodGet, "", vals)
	r.URL.RawQuery = strings.TrimPrefix(target, "?")
	h.HandleObservedFields(rec, r)
	var resp ObservedFieldsResponse
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode %q: %v", rec.Body.String(), err)
		}
	}
	return rec, resp
}

// The list covers exactly the scope a search would: the selected fractal, or every
// member of the selected prism, plus the unscoped rows when the default fractal is
// among them.
func TestObservedFieldsFollowTheSearchScope(t *testing.T) {
	cases := []struct {
		name string
		vals ctxVals
		want string
	}{
		{"fractal", sessionOn("selected_fractal", "f1"), "fractal_id IN ('f1')"},
		{"default fractal", sessionOn("selected_fractal", "fdef"), "fractal_id IN ('fdef', '')"},
		{"prism", sessionOn("selected_prism", "p1"), "fractal_id IN ('f1', 'f2')"},
		{"prism holding the default", sessionOn("selected_prism", "p2"), "fractal_id IN ('f1', 'fdef', '')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := scopeEnv(t)
			observedRollup(e)
			rec, resp := getObserved(t, e.h, "", tc.vals)
			if rec.Code != http.StatusOK || !resp.Success {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if resp.Rows != 10 || len(resp.Fields) != 2 || resp.Fields[0].Name != "host" {
				t.Errorf("response = %+v", resp)
			}
			q := e.ch.QueriedMatching("FROM logs_field_paths")
			if len(q) != 1 || !strings.Contains(q[0], tc.want) {
				t.Errorf("want %s in:\n%s", tc.want, q)
			}
		})
	}
}

// A prism with no members has nothing to list, and reads nothing.
func TestObservedFieldsEmptyPrism(t *testing.T) {
	e := scopeEnv(t)
	rec, resp := getObserved(t, e.h, "", sessionOn("selected_prism", "pempty"))
	if rec.Code != http.StatusOK || len(resp.Fields) != 0 || resp.Rows != 0 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if n := len(e.ch.QueriedMatching("FROM logs_field_paths")); n != 0 {
		t.Errorf("read the rollup %d times for an empty scope", n)
	}
}

// Field names are scoped data: no role on the prism, no list.
func TestObservedFieldsRefusesWithoutAccess(t *testing.T) {
	e := scopeEnv(t)
	vals := ctxVals{"user": testViewer, "auth_type": "session", "selected_prism": "p1"}
	if rec, _ := getObserved(t, e.h, "", vals); rec.Code != http.StatusForbidden {
		t.Errorf("status %d, want 403", rec.Code)
	}
	if n := len(e.ch.QueriedMatching("FROM logs_field_paths")); n != 0 {
		t.Errorf("read the rollup %d times without access", n)
	}
}

// Autocomplete asks on every scope change and page load, so a scope is read once
// per TTL, and another scope is read on its own.
func TestObservedFieldsAreCachedPerScope(t *testing.T) {
	e := scopeEnv(t)
	observedRollup(e)
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	e.h.observedFields.now = func() time.Time { return now }

	f1 := sessionOn("selected_fractal", "f1")
	getObserved(t, e.h, "", f1)
	getObserved(t, e.h, "", f1)
	if n := len(e.ch.QueriedMatching("FROM logs_field_paths")); n != 1 {
		t.Errorf("read %d times within the TTL, want 1", n)
	}
	getObserved(t, e.h, "", sessionOn("selected_fractal", "f2"))
	if n := len(e.ch.QueriedMatching("FROM logs_field_paths")); n != 2 {
		t.Errorf("another scope must be read on its own, got %d reads", n)
	}
	now = now.Add(observedFieldsTTL + time.Second)
	getObserved(t, e.h, "", f1)
	if n := len(e.ch.QueriedMatching("FROM logs_field_paths")); n != 3 {
		t.Errorf("an expired entry must be read again, got %d reads", n)
	}
}

// An explicit fractal_id overrides the session's prism, as it does for a search.
func TestObservedFieldsForARequestedFractal(t *testing.T) {
	e := scopeEnv(t)
	observedRollup(e)
	vals := ctxVals{"user": testAdmin, "auth_type": "session", "selected_prism": "p1"}
	if rec, _ := getObserved(t, e.h, "?fractal_id=f2", vals); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if q := e.ch.QueriedMatching("FROM logs_field_paths"); len(q) != 1 || !strings.Contains(q[0], "fractal_id IN ('f2')") {
		t.Errorf("want only f2 in:\n%s", q)
	}
}

// A viewer of one fractal naming another it holds no role on is refused before
// anything is read, as a search naming it would be.
func TestObservedFieldsRefusesAnotherFractal(t *testing.T) {
	e := scopeEnv(t)
	observedRollup(e)
	e.h.SetRBACResolver(rbac.NewResolver(e.pgc))
	vals := ctxVals{"user": testViewer, "auth_type": "session", "selected_fractal": "f1", "fractal_role": "viewer"}
	if rec, _ := getObserved(t, e.h, "?fractal_id=f2", vals); rec.Code != http.StatusForbidden {
		t.Errorf("status %d, want 403", rec.Code)
	}
	if n := len(e.ch.QueriedMatching("FROM logs_field_paths")); n != 0 {
		t.Errorf("read the rollup %d times for a fractal the caller cannot read", n)
	}
	if rec, _ := getObserved(t, e.h, "", vals); rec.Code != http.StatusOK {
		t.Errorf("own fractal: status %d, want 200", rec.Code)
	}
}
