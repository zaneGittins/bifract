package feeds

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"bifract/internal/fakedb"
	"bifract/pkg/alerts"
	"bifract/pkg/fractals"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// caller describes who is asking and in which scope, as the auth middleware
// leaves it in the request context. Route-level access (viewer vs tenant admin)
// is enforced by the router, not these handlers; what they own is scope.
type caller struct {
	user        *storage.User
	attribution string // storage.AttributionUserKey
	fractal     string
	prism       string
	noScope     bool // X-Bifract-Scope: none
}

var (
	nobody    = caller{}
	inF1      = caller{user: &storage.User{Username: "ana"}, attribution: "ana", fractal: "f1"}
	inF2      = caller{user: &storage.User{Username: "ana"}, attribution: "ana", fractal: "f2"}
	inP1      = caller{user: &storage.User{Username: "ana"}, attribution: "ana", prism: "p1"}
	inBoth    = caller{user: &storage.User{Username: "ana"}, attribution: "ana", fractal: "f1", prism: "p1"}
	unscoped  = caller{user: &storage.User{Username: "ana"}, attribution: "ana"}
	declNone  = caller{user: &storage.User{Username: "ana"}, attribution: "ana", noScope: true}
	apiKeyInF = caller{user: &storage.User{Username: "apikey_k1", IsAdmin: true}, attribution: "keyowner", fractal: "f1"}
)

func (c caller) request(method, target, body string, params map[string]string) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	ctx := r.Context()
	if c.user != nil {
		ctx = context.WithValue(ctx, "user", c.user)
	}
	if c.attribution != "" {
		ctx = context.WithValue(ctx, storage.AttributionUserKey, c.attribution)
	}
	if c.fractal != "" {
		ctx = context.WithValue(ctx, "selected_fractal", c.fractal)
	}
	if c.prism != "" {
		ctx = context.WithValue(ctx, "selected_prism", c.prism)
	}
	if c.noScope {
		ctx = fractals.WithNoScope(ctx)
	}
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	ctx = context.WithValue(ctx, chi.RouteCtxKey, rctx)
	return r.WithContext(ctx)
}

type handlerEnv struct {
	h    *Handler
	pg   *fakedb.PG
	sync *Syncer
}

// newHandlerEnv builds a handler over one scripted database. feeds are the
// rows Get answers by id.
func newHandlerEnv(t *testing.T, feeds ...[]driver.Value) *handlerEnv {
	t.Helper()
	pg, fake := storagetest.Postgres()
	t.Cleanup(func() { pg.Close() })
	byID := map[string][]driver.Value{}
	for _, f := range feeds {
		byID[f[0].(string)] = f
	}
	fake.Respond(func(q string, args []driver.Value) (fakedb.PGResult, bool) {
		if strings.Contains(q, "WHERE f.id = $1") {
			if row, ok := byID[str(args[0])]; ok {
				return fakedb.Row(row...), true
			}
			return fakedb.PGResult{}, true
		}
		return fakedb.PGResult{}, false
	})
	fake.On("FROM fractals", fakedb.Row("default-frac", "default", "", true, false, "",
		time.Now(), time.Now(), nil, nil, nil, "reject", int64(0), int64(0), nil, nil))

	mgr := &Manager{pg: pg}
	am := alerts.NewManager(pg, alerts.NewEngineWithDicts(pg, nil, nil, "http://localhost"), nil)
	s := NewSyncer(mgr, am, nil)
	return &handlerEnv{
		h:    NewHandler(mgr, am, fractals.NewManager(pg, nil), s),
		pg:   fake,
		sync: s,
	}
}

func (e *handlerEnv) serve(t *testing.T, fn func(*Handler) http.HandlerFunc, r *http.Request) (int, apiBody) {
	t.Helper()
	rec := httptest.NewRecorder()
	fn(e.h)(rec, r)
	var body apiBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %s", rec.Body.String())
	}
	return rec.Code, body
}

type apiBody struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
	Error   string          `json:"error"`
}

func (e *handlerEnv) writes() []string {
	var out []string
	for _, c := range e.pg.Calls() {
		q := strings.TrimSpace(c.Query)
		for _, verb := range []string{"INSERT", "UPDATE", "DELETE"} {
			if strings.HasPrefix(q, verb) {
				out = append(out, q)
			}
		}
	}
	return out
}

func TestHandleListFeedsScope(t *testing.T) {
	tests := []struct {
		name      string
		as        caller
		status    int
		wantWhere string
		wantArg   string
	}{
		{"fractal", inF1, 200, "f.fractal_id = $1", "f1"},
		{"prism", inP1, 200, "f.prism_id = $1", "p1"},
		// With both selected the prism is the scope; listing by fractal would
		// show feeds the prism view does not own.
		{"prism wins over fractal", inBoth, 200, "f.prism_id = $1", "p1"},
		{"falls back to the default fractal", unscoped, 200, "f.fractal_id = $1", "default-frac"},
		// A client that declared no scope must not be answered from the default.
		{"declared no scope", declNone, 400, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newHandlerEnv(t)
			code, body := env.serve(t, func(h *Handler) http.HandlerFunc { return h.HandleListFeeds },
				tt.as.request(http.MethodGet, "/feeds", "", nil))
			if code != tt.status {
				t.Fatalf("status %d, want %d: %+v", code, tt.status, body)
			}
			calls := env.pg.Matching("FROM alert_feeds f")
			if tt.status != 200 {
				if len(calls) != 0 {
					t.Error("an unscoped request listed feeds")
				}
				return
			}
			if len(calls) != 1 || !strings.Contains(calls[0].Query, tt.wantWhere) || calls[0].Args[0] != tt.wantArg {
				t.Errorf("list query = %+v", calls)
			}
		})
	}

	env := newHandlerEnv(t)
	env.pg.On("FROM alert_feeds f", fakedb.PGResult{Err: errors.New("pq: relation does not exist")})
	code, body := env.serve(t, func(h *Handler) http.HandlerFunc { return h.HandleListFeeds }, inF1.request("GET", "/feeds", "", nil))
	if code != 500 || strings.Contains(body.Error, "relation") {
		t.Errorf("list failure = %d %q; want 500 without database detail", code, body.Error)
	}
}

// Every per-feed endpoint must refuse a feed outside the caller's scope with the
// same 404 as a missing one, and must not act on it.
func TestFeedEndpointsEnforceScope(t *testing.T) {
	endpoints := map[string]func(*Handler) http.HandlerFunc{
		"get":         func(h *Handler) http.HandlerFunc { return h.HandleGetFeed },
		"update":      func(h *Handler) http.HandlerFunc { return h.HandleUpdateFeed },
		"delete":      func(h *Handler) http.HandlerFunc { return h.HandleDeleteFeed },
		"sync":        func(h *Handler) http.HandlerFunc { return h.HandleSyncFeed },
		"alerts":      func(h *Handler) http.HandlerFunc { return h.HandleGetFeedAlerts },
		"enable-all":  func(h *Handler) http.HandlerFunc { return h.HandleEnableAllAlerts },
		"disable-all": func(h *Handler) http.HandlerFunc { return h.HandleDisableAllAlerts },
	}
	fractalFeed := feedRow("feed-f1", "f1", "", "")
	prismFeed := feedRow("feed-p1", "", "p1", "")
	cases := []struct {
		name string
		as   caller
		id   string
	}{
		{"other fractal", inF2, "feed-f1"},
		{"fractal feed seen from a prism", inP1, "feed-f1"},
		{"prism selected hides the fractal's feeds", inBoth, "feed-f1"},
		{"prism feed seen from a fractal", inF1, "feed-p1"},
		{"missing feed", inF1, "nope"},
		// An empty scope never matches a feed's empty prism or fractal id.
		{"declared no scope", declNone, "feed-f1"},
	}
	for name, ep := range endpoints {
		for _, c := range cases {
			t.Run(name+"/"+c.name, func(t *testing.T) {
				env := newHandlerEnv(t, fractalFeed, prismFeed)
				body := `{"name":"x","repo_url":"https://h/x","sync_schedule":"daily"}`
				code, resp := env.serve(t, ep, c.as.request(http.MethodPost, "/", body, map[string]string{"id": c.id}))
				if code != http.StatusNotFound || resp.Error != "Feed not found" {
					t.Fatalf("status %d %q, want 404 Feed not found", code, resp.Error)
				}
				if w := env.writes(); len(w) != 0 {
					t.Errorf("out-of-scope request wrote: %q", w)
				}
				if len(env.pg.Matching("WHERE feed_id = $1")) != 0 {
					t.Error("out-of-scope request read the feed's alerts")
				}
			})
		}
	}
}

func TestHandleGetFeed(t *testing.T) {
	env := newHandlerEnv(t, feedRow("feed-p1", "", "p1", "enc:secret-ciphertext"))
	code, body := env.serve(t, func(h *Handler) http.HandlerFunc { return h.HandleGetFeed },
		inP1.request("GET", "/", "", map[string]string{"id": "feed-p1"}))
	if code != 200 {
		t.Fatalf("status %d: %+v", code, body)
	}
	if strings.Contains(string(body.Data), "secret-ciphertext") || strings.Contains(string(body.Data), `"auth_token"`) {
		t.Errorf("feed response exposes the stored token: %s", body.Data)
	}
	var f Feed
	json.Unmarshal(body.Data, &f)
	if f.ID != "feed-p1" || !f.HasAuthToken {
		t.Errorf("feed = %+v", f)
	}
}

func TestHandleCreateFeed(t *testing.T) {
	create := func(h *Handler) http.HandlerFunc { return h.HandleCreateFeed }
	const good = `{"name":"Sigma","repo_url":"https://github.com/SigmaHQ/sigma","sync_schedule":"daily","auth_token":"ghp_x"}`

	t.Run("rejections", func(t *testing.T) {
		tests := []struct {
			name, body string
			as         caller
			status     int
			msg        string
		}{
			{"needs a scope", good, declNone, 400, "no fractal or prism selected"},
			{"needs JSON", `{`, inF1, 400, "invalid request body"},
			{"invalid feed", `{"name":"x","repo_url":"file:///etc/passwd","sync_schedule":"daily"}`, inF1, 400, "Failed to create feed"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				env := newHandlerEnv(t)
				code, body := env.serve(t, create, tt.as.request("POST", "/feeds", tt.body, nil))
				if code != tt.status || body.Error != tt.msg {
					t.Fatalf("got %d %q, want %d %q", code, body.Error, tt.status, tt.msg)
				}
				if w := env.writes(); len(w) != 0 {
					t.Errorf("rejected create wrote %q", w)
				}
			})
		}
	})

	t.Run("an API key's feed is credited to the key's creator", func(t *testing.T) {
		env := newHandlerEnv(t, feedRow("new-feed", "f1", "", ""))
		env.pg.On("INSERT INTO alert_feeds", fakedb.Row("new-feed"))
		code, body := env.serve(t, create, apiKeyInF.request("POST", "/feeds", good, nil))
		if code != http.StatusCreated {
			t.Fatalf("status %d: %+v", code, body)
		}
		ins := env.pg.Matching("INSERT INTO alert_feeds")[0].Args
		if ins[13] != "keyowner" || ins[11] != "f1" || ins[12] != nil {
			t.Errorf("created_by %v fractal %v prism %v", ins[13], ins[11], ins[12])
		}
	})

	t.Run("prism scope", func(t *testing.T) {
		env := newHandlerEnv(t, feedRow("new-feed", "", "p1", ""))
		env.pg.On("INSERT INTO alert_feeds", fakedb.Row("new-feed"))
		code, _ := env.serve(t, create, inBoth.request("POST", "/feeds", good, nil))
		if code != http.StatusCreated {
			t.Fatalf("status %d", code)
		}
		if ins := env.pg.Matching("INSERT INTO alert_feeds")[0].Args; ins[11] != nil || ins[12] != "p1" {
			t.Errorf("scope = fractal %v prism %v, want the prism only", ins[11], ins[12])
		}
	})
}

func TestHandleUpdateAndDeleteFeed(t *testing.T) {
	row := feedRow("feed-1", "f1", "", "")
	params := map[string]string{"id": "feed-1"}

	env := newHandlerEnv(t, row)
	update := func(h *Handler) http.HandlerFunc { return h.HandleUpdateFeed }
	if code, body := env.serve(t, update, inF1.request("PUT", "/", `{`, params)); code != 400 || body.Error != "invalid request body" {
		t.Errorf("bad JSON: %d %q", code, body.Error)
	}
	if code, body := env.serve(t, update, inF1.request("PUT", "/", `{"name":"x","repo_url":"ssh://h/x","sync_schedule":"daily"}`, params)); code != 400 || body.Error != "Failed to update feed" {
		t.Errorf("invalid update: %d %q", code, body.Error)
	}
	if w := env.writes(); len(w) != 0 {
		t.Fatalf("rejected updates wrote %q", w)
	}

	env.pg.On("UPDATE alert_feeds", fakedb.Affected(1))
	if code, body := env.serve(t, update, inF1.request("PUT", "/", `{"name":"x","repo_url":"https://h/x","sync_schedule":"weekly"}`, params)); code != 200 {
		t.Errorf("update: %d %+v", code, body)
	}

	del := func(h *Handler) http.HandlerFunc { return h.HandleDeleteFeed }
	if code, _ := env.serve(t, del, inF1.request("DELETE", "/", "", params)); code != 404 {
		t.Errorf("delete with 0 rows affected = %d, want 404", code)
	}
	env.pg.On("DELETE FROM alert_feeds", fakedb.Affected(1))
	if code, _ := env.serve(t, del, inF1.request("DELETE", "/", "", params)); code != 200 {
		t.Errorf("delete = %d", code)
	}
}

func TestHandleSyncFeed(t *testing.T) {
	isolateTempDir(t)
	repo := newGitRepo(t, map[string]string{"a.yml": nativeRule})
	row := feedRow("feed-1", "f1", "", "")
	row[3], row[5] = repo.URL, ""

	env := newHandlerEnv(t, row)
	env.pg.On("SELECT COALESCE(auth_token, '')", fakedb.Row(""))
	sync := func(h *Handler) http.HandlerFunc { return h.HandleSyncFeed }
	params := map[string]string{"id": "feed-1"}

	env.sync.acquire("feed-1")
	if code, body := env.serve(t, sync, inF1.request("POST", "/", "", params)); code != http.StatusConflict {
		t.Fatalf("sync while running = %d %+v, want 409", code, body)
	}
	env.sync.release("feed-1")

	if code, body := env.serve(t, sync, inF1.request("POST", "/", "", params)); code != http.StatusAccepted {
		t.Fatalf("sync = %d %+v, want 202", code, body)
	}
	// The sync is detached from the request; wait for it, then check it ran.
	env.sync.wg.Wait()
	w := statusWrites(env.pg)
	if len(w) != 2 || w[0].Status != "syncing" || w[1].Status != "success" {
		t.Errorf("status writes = %+v", w)
	}
}

func TestHandleGetFeedAlerts(t *testing.T) {
	env := newHandlerEnv(t, feedRow("feed-1", "f1", "", ""))
	get := func(h *Handler) http.HandlerFunc { return h.HandleGetFeedAlerts }
	params := map[string]string{"id": "feed-1"}

	if code, _ := env.serve(t, get, nobody.request("GET", "/", "", params)); code != 401 {
		t.Errorf("anonymous = %d, want 401", code)
	}
	code, body := env.serve(t, get, inF1.request("GET", "/", "", params))
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	if c := env.pg.Matching("FROM alerts\n\t\tWHERE feed_id = $1"); len(c) != 1 || c[0].Args[0] != "feed-1" {
		t.Errorf("alert listing = %+v", c)
	}
	_ = body

	env.pg.On("FROM alerts\n\t\tWHERE feed_id", fakedb.PGResult{Err: errors.New("boom")})
	if code, _ := env.serve(t, get, inF1.request("GET", "/", "", params)); code != 500 {
		t.Errorf("list failure = %d", code)
	}
}

func TestHandleEnableDisableAll(t *testing.T) {
	tests := []struct {
		name    string
		handler func(*Handler) http.HandlerFunc
		as      caller
		enabled bool
		by      driver.Value
	}{
		{"enable by a user", func(h *Handler) http.HandlerFunc { return h.HandleEnableAllAlerts }, inF1, true, "ana"},
		{"disable by a user", func(h *Handler) http.HandlerFunc { return h.HandleDisableAllAlerts }, inF1, false, "ana"},
		// updated_by is a foreign key on users: an API key's synthetic
		// apikey_<id> principal would fail it, so the key's creator is credited.
		{"enable by an API key", func(h *Handler) http.HandlerFunc { return h.HandleEnableAllAlerts }, apiKeyInF, true, "keyowner"},
		{"disable by an API key", func(h *Handler) http.HandlerFunc { return h.HandleDisableAllAlerts }, apiKeyInF, false, "keyowner"},
		// No attributable user is NULL, and no longer a nil-pointer panic.
		{"no user in context", func(h *Handler) http.HandlerFunc { return h.HandleDisableAllAlerts }, caller{fractal: "f1"}, false, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newHandlerEnv(t, feedRow("feed-1", "f1", "", ""))
			code, body := env.serve(t, tt.handler, tt.as.request("POST", "/", "", map[string]string{"id": "feed-1"}))
			if code != 200 {
				t.Fatalf("status %d %+v", code, body)
			}
			c := env.pg.Matching("UPDATE alerts SET enabled")
			if len(c) != 1 {
				t.Fatalf("toggle statements = %d", len(c))
			}
			if c[0].Args[0] != tt.enabled || c[0].Args[1] != tt.by || c[0].Args[2] != "feed-1" {
				t.Errorf("toggle args = %v, want enabled=%v by=%v feed-1", c[0].Args, tt.enabled, tt.by)
			}
		})
	}

	env := newHandlerEnv(t, feedRow("feed-1", "f1", "", ""))
	env.pg.On("UPDATE alerts SET enabled", fakedb.PGResult{Err: errors.New("boom")})
	for _, fn := range []func(*Handler) http.HandlerFunc{
		func(h *Handler) http.HandlerFunc { return h.HandleEnableAllAlerts },
		func(h *Handler) http.HandlerFunc { return h.HandleDisableAllAlerts },
	} {
		if code, _ := env.serve(t, fn, inF1.request("POST", "/", "", map[string]string{"id": "feed-1"})); code != 500 {
			t.Errorf("toggle failure = %d, want 500", code)
		}
	}
}

func TestParseFeedAlertQuery(t *testing.T) {
	r := httptest.NewRequest("GET", "/alerts/feed?"+url.Values{
		"search":   {"mimikatz"},
		"status":   {"all"},
		"feed_id":  {"feed-9"},
		"severity": {"all"},
		"label":    {"attack.t1003"},
		"sort":     {"severity"},
		"dir":      {"desc"},
		"limit":    {"50"},
		"offset":   {"oops"},
	}.Encode(), nil)

	got := parseFeedAlertQuery(r, "f1", "")
	want := alerts.FeedAlertQuery{
		FractalID: "f1", Search: "mimikatz", FeedID: "feed-9", Label: "attack.t1003",
		Sort: "severity", Dir: "desc", Limit: 50, Offset: 0,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseFeedAlertQuery =\n  %+v\nwant\n  %+v", got, want)
	}
}

func TestHandleListAllFeedAlerts(t *testing.T) {
	list := func(h *Handler) http.HandlerFunc { return h.HandleListAllFeedAlerts }

	env := newHandlerEnv(t)
	if code, _ := env.serve(t, list, nobody.request("GET", "/alerts/feed", "", nil)); code != 401 {
		t.Errorf("anonymous = %d", code)
	}
	if code, _ := env.serve(t, list, declNone.request("GET", "/alerts/feed", "", nil)); code != 400 {
		t.Errorf("no scope = %d", code)
	}
	if len(env.pg.Matching("FROM alerts a")) != 0 {
		t.Fatal("a rejected request queried alerts")
	}

	code, body := env.serve(t, list, inP1.request("GET", "/alerts/feed?severity=all", "", nil))
	if code != 200 {
		t.Fatalf("status %d %+v", code, body)
	}
	page := env.pg.Matching("COUNT(*) OVER ()")
	if len(page) != 1 || !strings.Contains(page[0].Query, "a.prism_id = $1") || page[0].Args[0] != "p1" {
		t.Errorf("page query = %+v", page)
	}
	if len(env.pg.Matching("unnest(a.labels)")) != 0 {
		t.Error("facets computed without facets=1")
	}

	env.serve(t, list, inF1.request("GET", "/alerts/feed?facets=1", "", nil))
	if len(env.pg.Matching("unnest(a.labels)")) != 1 {
		t.Error("facets=1 did not compute facets")
	}

	env.pg.On("unnest(a.labels)", fakedb.PGResult{Err: errors.New("boom")})
	if code, _ := env.serve(t, list, inF1.request("GET", "/alerts/feed?facets=1", "", nil)); code != 500 {
		t.Errorf("facet failure = %d", code)
	}
	env.pg.On("COUNT(*) OVER ()", fakedb.PGResult{Err: errors.New("boom")})
	if code, _ := env.serve(t, list, inF1.request("GET", "/alerts/feed", "", nil)); code != 500 {
		t.Errorf("page failure = %d", code)
	}
}
