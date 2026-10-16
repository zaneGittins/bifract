package query

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The status page is polled; a fresh cached answer is served without asking
// ClickHouse again. (The handler has no client here, so a refetch would panic.)
func TestHandleStatusServesFreshCache(t *testing.T) {
	h := NewStatusHandler(nil, nil)
	h.cached = &StatusResponse{Success: true, ClickHouse: ClickHouseStatus{Connected: true, TotalLogs: 42}}
	h.cachedAt = time.Now()

	rec := httptest.NewRecorder()
	h.HandleStatus(rec, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	var resp StatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || resp.ClickHouse.TotalLogs != 42 {
		t.Errorf("got %d %+v, want the cached status", rec.Code, resp)
	}
}

func TestStatusHandlersRejectWrongMethod(t *testing.T) {
	h := NewStatusHandler(nil, nil)
	rec := httptest.NewRecorder()
	h.HandleStatus(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status POST: %d", rec.Code)
	}
	// Clearing logs is destructive: only DELETE may reach it.
	for _, m := range []string{http.MethodGet, http.MethodPost} {
		rec = httptest.NewRecorder()
		h.HandleClearLogs(rec, httptest.NewRequest(m, "/?fractal_id=f1", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("clear %s: %d", m, rec.Code)
		}
	}
}
