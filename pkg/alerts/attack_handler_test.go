package alerts

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"bifract/pkg/attack"
)

// The matrix only changes with the binary, so a client holding the current ETag gets
// a 304 instead of ~700 techniques again.
func TestAttackMatrixIsCachedByVersion(t *testing.T) {
	h := newTestHandler()

	rec := httptest.NewRecorder()
	h.HandleAttackMatrix(rec, nobody.request(http.MethodGet, "", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("no role: %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.HandleAttackMatrix(rec, viewer.request(http.MethodGet, "", nil))
	etag := rec.Header().Get("ETag")
	if rec.Code != http.StatusOK || etag == "" || rec.Header().Get("Cache-Control") == "" {
		t.Fatalf("status %d etag %q", rec.Code, etag)
	}

	r := viewer.request(http.MethodGet, "", nil)
	r.Header.Set("If-None-Match", etag)
	rec = httptest.NewRecorder()
	h.HandleAttackMatrix(rec, r)
	if rec.Code != http.StatusNotModified || rec.Body.Len() != 0 {
		t.Errorf("a current ETag must answer 304 with no body, got %d (%d bytes)", rec.Code, rec.Body.Len())
	}
}

func TestAttackEndpointsNeedARole(t *testing.T) {
	for name, endpoint := range map[string]func(h *Handler) http.HandlerFunc{
		"coverage":  func(h *Handler) http.HandlerFunc { return h.HandleAttackCoverage },
		"technique": func(h *Handler) http.HandlerFunc { return h.HandleAttackTechniqueRules },
		"gap":       func(h *Handler) http.HandlerFunc { return h.HandleAttackTechniqueGap },
		"layer":     func(h *Handler) http.HandlerFunc { return h.HandleAttackLayer },
	} {
		rec := httptest.NewRecorder()
		endpoint(newTestHandler())(rec, nobody.request(http.MethodGet, "", nil))
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s: %d", name, rec.Code)
		}
	}
}

func TestAttackFilter(t *testing.T) {
	matrix, err := attack.Get()
	if err != nil {
		t.Fatal(err)
	}
	if len(matrix.Platforms) == 0 {
		t.Fatal("the embedded matrix has no platforms")
	}
	filterFor := func(query string) (attack.Filter, error) {
		r := httptest.NewRequest(http.MethodGet, "/x?"+query, nil)
		return attackFilter(matrix, r)
	}

	f, err := filterFor("enabled_only=true&severity=informational&feed_id=feed-1&platform=" + url.QueryEscape(matrix.Platforms[0]))
	if err != nil {
		t.Fatal(err)
	}
	if !f.EnabledOnly || f.Severity != "informational" || f.FeedID != "feed-1" || f.Platform != matrix.Platforms[0] {
		t.Errorf("filter = %+v", f)
	}

	// The enum is the source of truth: "info" is not a value anything writes.
	if _, err := filterFor("severity=info"); err == nil {
		t.Error("an unknown severity must be refused")
	}
	if _, err := filterFor("platform=Amiga"); err == nil {
		t.Error("an unknown platform must be refused")
	}
	if f, err := filterFor(""); err != nil || f.EnabledOnly || f.Severity != "" {
		t.Errorf("no parameters, no filter: %+v %v", f, err)
	}
}

func TestNavigatorLayerHelpers(t *testing.T) {
	for in, want := range map[string]string{"19.1": "19", "19": "19", "": "", ".5": ".5"} {
		if got := majorVersion(in); got != want {
			t.Errorf("majorVersion(%q) = %q, want %q", in, got, want)
		}
	}
	if got := techniqueURL("T1059.001"); got != "https://attack.mitre.org/techniques/T1059/001/" {
		t.Errorf("sub-technique URL = %q", got)
	}
	if got := techniqueURL("T1059"); got != "https://attack.mitre.org/techniques/T1059/" {
		t.Errorf("technique URL = %q", got)
	}

	// The gradient's top is the busiest technique, and never zero, which would make
	// Navigator divide by nothing.
	if got := maxScore(&attack.Coverage{Techniques: map[string]*attack.TechniqueCoverage{}}); got != 1 {
		t.Errorf("empty coverage: %d", got)
	}
	cov := &attack.Coverage{Techniques: map[string]*attack.TechniqueCoverage{
		"T1": {Total: 3}, "T2": {Total: 9}, "T3": {Total: 0},
	}}
	if got := maxScore(cov); got != 9 {
		t.Errorf("max score = %d", got)
	}
}
