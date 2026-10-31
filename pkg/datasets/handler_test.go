package datasets

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"bifract/internal/fakedb"
	"bifract/pkg/exchange"
	"bifract/pkg/fractals"
	"bifract/pkg/sse"
	"bifract/pkg/storage/storagetest"
)

type fakeIndex struct {
	idx *exchange.Index
	err error
}

func (f *fakeIndex) Index(context.Context) (*exchange.Index, error) { return f.idx, f.err }

type fakeFractals struct {
	createErr error
	created   []string
	deleted   []string
	start     time.Time
}

func (f *fakeFractals) CreateFractalForDataset(_ context.Context, name, description, _ string, start, _ time.Time) (*fractals.Fractal, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.created = append(f.created, name+"|"+description)
	f.start = start
	return &fractals.Fractal{ID: testFractal, Name: name}, nil
}

func (f *fakeFractals) DeleteFractal(_ context.Context, id string) error {
	f.deleted = append(f.deleted, id)
	return nil
}

type testHub struct{ fakeHub }

func (h *testHub) ServeSSE(http.ResponseWriter, *http.Request, string, sse.ClientInfo) *sse.Client {
	return nil
}

var testIndex = &exchange.Index{Items: []exchange.Item{
	{ID: "bifract-labs-001", Kind: exchange.KindDataset, Name: "Lab 001", Description: "Three days.",
		Dataset: &exchange.Dataset{URL: "https://example.com/d.jsonl.zst", SHA256: strings.Repeat("ab", 32), Bytes: 10, Logs: 3,
			Start: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)}},
	{ID: "soc-baseline", Kind: exchange.KindPolicy, Name: "SOC", Path: "p.yaml"},
}}

func newTestHandler(t *testing.T) (*Handler, *fakeFractals, *fakedb.PG, *testHub) {
	h, f, pg, hub, _ := newTestHandlerIndex(t)
	return h, f, pg, hub
}

func newTestHandlerIndex(t *testing.T) (*Handler, *fakeFractals, *fakedb.PG, *testHub, *fakeIndex) {
	t.Helper()
	pgc, pg := storagetest.Postgres()
	now := time.Now()
	pg.On("INSERT INTO dataset_imports", fakedb.Row(int64(9)))
	pg.On("WHERE i.id = $1", fakedb.Row(int64(9), "bifract-labs-001", testFractal, "bifract-labs-001",
		"https://example.com/d.jsonl.zst", strings.Repeat("ab", 32), int64(10), int64(3), StatusPending,
		int64(0), int64(0), "", "admin", now, nil, nil, now))
	f := &fakeFractals{}
	hub := &testHub{}
	idx := &fakeIndex{idx: testIndex}
	return NewHandler(idx, f, NewStore(pgc), hub), f, pg, hub, idx
}

func add(h *Handler, id string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/exchange/datasets/"+id+"/import", nil)
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", id)
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rc))
	w := httptest.NewRecorder()
	h.HandleAdd(w, r)
	return w
}

func TestHandleAdd(t *testing.T) {
	h, f, pg, hub := newTestHandler(t)
	w := add(h, "bifract-labs-001")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if len(f.created) != 1 || f.created[0] != "bifract-labs-001|Lab 001. Three days." || !f.start.Equal(testIndex.Items[0].Dataset.Start) {
		t.Errorf("created %v from %v", f.created, f.start)
	}
	ins := pg.Matching("INSERT INTO dataset_imports")
	if len(ins) != 1 || ins[0].Args[1] != testFractal || ins[0].Args[4] != strings.Repeat("ab", 32) {
		t.Errorf("insert = %+v", ins)
	}
	if len(hub.events) != 1 {
		t.Errorf("broadcasts = %v", hub.events)
	}
}

func TestHandleAddRefuses(t *testing.T) {
	h, f, _, _, idx := newTestHandlerIndex(t)
	if w := add(h, "nope"); w.Code != http.StatusNotFound {
		t.Errorf("unknown item: %d", w.Code)
	}
	if w := add(h, "soc-baseline"); w.Code != http.StatusNotFound {
		t.Errorf("a policy was added as a dataset: %d", w.Code)
	}
	f.createErr = fmt.Errorf("a fractal named 'bifract-labs-001' %w", fractals.ErrNameTaken)
	if w := add(h, "bifract-labs-001"); w.Code != http.StatusConflict {
		t.Errorf("existing fractal: %d", w.Code)
	}
	// Storage errors are logged, not echoed.
	f.createErr = errors.New("pq: connection refused at 10.0.0.5")
	if w := add(h, "bifract-labs-001"); w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), "10.0.0.5") {
		t.Errorf("storage error: %d %s", w.Code, w.Body)
	}
	idx.err = exchange.ErrDisabled
	if w := add(h, "bifract-labs-001"); w.Code != http.StatusNotFound {
		t.Errorf("Exchange off: %d", w.Code)
	}
}

// A fractal is never left behind without the import that would fill it.
func TestHandleAddRemovesFractalWhenQueueingFails(t *testing.T) {
	h, f, pg, _ := newTestHandler(t)
	pg.ClearRules()
	pg.On("INSERT INTO dataset_imports", fakedb.PGResult{Err: errors.New("boom")})
	if w := add(h, "bifract-labs-001"); w.Code != http.StatusInternalServerError {
		t.Fatalf("status %d", w.Code)
	}
	if len(f.deleted) != 1 || f.deleted[0] != testFractal {
		t.Errorf("deleted %v", f.deleted)
	}
}

func TestHandleRetryOnlyFailed(t *testing.T) {
	h, _, pg, _ := newTestHandler(t)
	retry := func() int {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		rc := chi.NewRouteContext()
		rc.URLParams.Add("importId", "9")
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rc))
		w := httptest.NewRecorder()
		h.HandleRetry(w, r)
		return w.Code
	}
	if code := retry(); code != http.StatusConflict {
		t.Errorf("retrying an import that is not failed: %d", code)
	}
	pg.On("SET status = 'pending'", fakedb.Affected(1))
	if code := retry(); code != http.StatusOK {
		t.Errorf("retrying a failed import: %d", code)
	}
	if calls := pg.Matching("SET status = 'pending'"); !strings.Contains(calls[0].Query, "status = 'failed'") {
		t.Errorf("retry must only reset failed imports: %s", calls[0].Query)
	}
}

func TestFractalDescriptionFits(t *testing.T) {
	for name, it := range map[string]*exchange.Item{
		"words":    {Name: "Lab", Description: strings.Repeat("word ", 200)},
		"no space": {Name: strings.Repeat("\u00e9", 400)},
	} {
		d := fractalDescription(it)
		if len(d) > maxDescription || !strings.HasSuffix(d, "...") || !utf8.ValidString(d) {
			t.Errorf("%s: description of %d bytes: %q", name, len(d), d)
		}
	}
}
