package dictionaries

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/api"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// The manager below has no database, so every case here must be decided before
// the handler reaches one. A guard that regresses past its check panics on the
// nil client, which fails the test as surely as a wrong status would.
func testHandler() *Handler { return NewHandler(&Manager{}, nil) }

// principal describes who a request runs as.
type principal struct {
	user    *storage.User
	role    rbac.Role
	ceiling api.Access // empty for none
}

var (
	nobody   = principal{}
	viewer   = principal{user: &storage.User{Username: "v"}, role: rbac.RoleViewer}
	analyst  = principal{user: &storage.User{Username: "a"}, role: rbac.RoleAnalyst}
	admin    = principal{user: &storage.User{Username: "root", IsAdmin: true}}
	aiCapped = principal{user: &storage.User{Username: "root", IsAdmin: true}, ceiling: api.AccessAnalyst}
)

func scopedRequest(method, body string, who principal) *http.Request {
	r := httptest.NewRequest(method, "/api/v1/dictionaries", strings.NewReader(body))
	ctx := context.WithValue(r.Context(), "selected_fractal", "f-1")
	if who.user != nil {
		ctx = context.WithValue(ctx, "user", who.user)
	}
	if who.role != "" {
		ctx = context.WithValue(ctx, "fractal_role", string(who.role))
	}
	if who.ceiling != "" {
		ctx = api.WithCeiling(ctx, who.ceiling)
	}
	return r.WithContext(ctx)
}

type envelope struct {
	Success bool            `json:"success"`
	Error   string          `json:"error"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func serve(t *testing.T, handler http.HandlerFunc, r *http.Request) (int, envelope) {
	t.Helper()
	w := httptest.NewRecorder()
	handler(w, r)
	var env envelope
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("response is not a JSON envelope: %v\n%s", err, w.Body.String())
	}
	return w.Code, env
}

// Every route that changes a list, its rows or a dictionary action refuses a
// viewer, and a request with no user at all, before it reads anything.
func TestWriteRoutesRequireAnalyst(t *testing.T) {
	h := testHandler()
	routes := map[string]http.HandlerFunc{
		"create":                h.HandleCreateDictionary,
		"update":                h.HandleUpdateDictionary,
		"delete":                h.HandleDeleteDictionary,
		"add column":            h.HandleAddColumn,
		"remove column":         h.HandleRemoveColumn,
		"set column key":        h.HandleSetColumnKey,
		"unset column key":      h.HandleUnsetColumnKey,
		"case insensitive keys": h.HandleSetCaseInsensitiveKeys,
		"upsert rows":           h.HandleUpsertRows,
		"delete row":            h.HandleDeleteRow,
		"import csv":            h.HandleImportCSV,
		"reload":                h.HandleReloadDictionary,
		"sync source":           h.HandleSyncSource,
		"create action":         h.HandleCreateDictionaryAction,
		"update action":         h.HandleUpdateDictionaryAction,
		"delete action":         h.HandleDeleteDictionaryAction,
	}
	for name, route := range routes {
		for _, who := range []principal{nobody, viewer} {
			code, env := serve(t, route, scopedRequest(http.MethodPost, `{"name":"x"}`, who))
			if code != http.StatusForbidden || env.Success {
				t.Errorf("%s as %q: status %d, want 403", name, who.role, code)
			}
		}
	}
}

// Choosing what a source URL points at makes the server issue requests from
// inside its own network, so it takes a tenant administrator: an analyst is
// refused, and so is an administrator acting through an AI tool held to an
// analyst ceiling.
func TestSettingASourceRequiresTenantAdmin(t *testing.T) {
	h := testHandler()
	routes := map[string]http.HandlerFunc{
		"set source":    h.HandleSetSource,
		"detach source": h.HandleDetachSource,
	}
	for name, route := range routes {
		for label, who := range map[string]principal{"analyst": analyst, "capped admin": aiCapped} {
			code, _ := serve(t, route, scopedRequest(http.MethodPost, `{}`, who))
			if code != http.StatusForbidden {
				t.Errorf("%s as %s: status %d, want 403", name, label, code)
			}
		}
	}

	// Creating a list with a source is the same choice, made at creation.
	body := `{"name":"lolrmm","source":{"url":"https://example.com/a.csv","key_field":"k"}}`
	for label, who := range map[string]principal{"analyst": analyst, "capped admin": aiCapped} {
		code, env := serve(t, h.HandleCreateDictionary, scopedRequest(http.MethodPost, body, who))
		if code != http.StatusForbidden || !strings.Contains(env.Error, "administrator") {
			t.Errorf("create with a source as %s: status %d (%q), want 403", label, code, env.Error)
		}
	}
}

// What a create request gets wrong is reported as a 400 that says what, never a
// 500: the editor shows the reason to whoever filled in the form.
func TestCreateDictionaryRefusals(t *testing.T) {
	h := testHandler()
	cases := []struct {
		name string
		who  principal
		body string
		want string
	}{
		{"malformed body", analyst, `{"name":`, "invalid request body"},
		{"missing name", analyst, `{"description":"x"}`, "name is required"},
		{"unknown kind", analyst, `{"name":"x","kind":"ip_trie"}`, `unknown list kind "ip_trie"`},
		{"reserved pattern column", analyst,
			`{"name":"x","kind":"pattern","columns":[{"name":"p"},{"name":"_match"}]}`, "reserved"},
		{"source with its own columns", admin,
			`{"name":"x","columns":[{"name":"k"}],"source":{"url":"https://example.com/a.csv","key_field":"k"}}`,
			"takes its columns from the file"},
		{"source at a file URL", admin,
			`{"name":"x","source":{"url":"file:///etc/passwd","key_field":"k"}}`, "source URL"},
		{"source with no key field", admin,
			`{"name":"x","source":{"url":"https://example.com/a.csv"}}`, "holds the key"},
		{"glob keys on a values list", admin,
			`{"name":"x","source":{"url":"https://example.com/a.csv","key_field":"k","key_format":"glob"}}`, "Patterns list"},
	}
	for _, c := range cases {
		code, env := serve(t, h.HandleCreateDictionary, scopedRequest(http.MethodPost, c.body, c.who))
		if code != http.StatusBadRequest || !strings.Contains(env.Error, c.want) {
			t.Errorf("%s: status %d error %q, want 400 mentioning %q", c.name, code, env.Error, c.want)
		}
	}
}

func TestCreateDictionaryActionRefusals(t *testing.T) {
	h := testHandler()
	for body, want := range map[string]string{
		`not json`:                         "invalid request body",
		`{"name":"a"}`:                     "name and dictionary_name are required",
		`{"dictionary_name":"d"}`:          "name and dictionary_name are required",
		`{"name":"","dictionary_name":""}`: "name and dictionary_name are required",
	} {
		code, env := serve(t, h.HandleCreateDictionaryAction, scopedRequest(http.MethodPost, body, analyst))
		if code != http.StatusBadRequest || env.Error != want {
			t.Errorf("%s: status %d error %q, want 400 %q", body, code, env.Error, want)
		}
	}
}

// A prism session must resolve to the prism even when a stale fractal is still
// in the context: fractal-first writes rows under one scope and lists them
// under another, so they never appear.
func TestGetScopeResolvesPrismFirst(t *testing.T) {
	h := testHandler()
	cases := []struct {
		fractal, prism         string
		wantFractal, wantPrism string
	}{
		{"f-1", "", "f-1", ""},
		{"", "p-1", "", "p-1"},
		{"f-1", "p-1", "", "p-1"},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		ctx := context.WithValue(r.Context(), "selected_fractal", c.fractal)
		ctx = context.WithValue(ctx, "selected_prism", c.prism)
		f, p, err := h.getScope(r.WithContext(ctx))
		if err != nil || f != c.wantFractal || p != c.wantPrism {
			t.Errorf("fractal %q prism %q: got (%q, %q, %v), want (%q, %q)", c.fractal, c.prism, f, p, err, c.wantFractal, c.wantPrism)
		}
	}
}

// A client that declared it holds no scope is not resolved to the default
// fractal: listing must refuse rather than show the default fractal's lists.
func TestListRefusesADeclaredNoScope(t *testing.T) {
	h := NewHandler(&Manager{}, fractals.NewManager(nil, nil))
	r := httptest.NewRequest(http.MethodGet, "/api/v1/dictionaries", nil)
	r = r.WithContext(fractals.WithNoScope(r.Context()))
	if _, _, err := h.getScope(r); err == nil {
		t.Fatal("a declared no-scope resolved to a scope")
	}
	code, _ := serve(t, h.HandleListDictionaries, r)
	if code != http.StatusBadRequest {
		t.Errorf("list with no scope: status %d, want 400", code)
	}

	// With no fractal manager there is no default to fall back to either.
	bare := httptest.NewRequest(http.MethodGet, "/api/v1/dictionaries", nil)
	if _, _, err := testHandler().getScope(bare); err == nil {
		t.Error("no scope and no fractal manager resolved to a scope")
	}
}

// A load answers by what went wrong: the list when it is done, a 202 carrying
// the list when it goes on in the background, a conflict or a rate limit for a
// refresh that cannot start, and a 400 naming a caller-fixable problem.
// Anything else is a 500 whose text never carries the server-side detail.
func TestRespondSourceResult(t *testing.T) {
	h := testHandler()
	dict := &Dictionary{ID: "d1", Name: "lolrmm"}
	internal := errors.New(`dial tcp 10.1.2.3:5432: connection refused`)
	cases := []struct {
		name    string
		err     error
		status  int
		success bool
		want    string
	}{
		{"loaded", nil, http.StatusOK, true, ""},
		{"still loading", errSourceLoading, http.StatusAccepted, true, ""},
		{"still loading, wrapped", fmt.Errorf("first load: %w", errSourceLoading), http.StatusAccepted, true, ""},
		{"busy", errSourceBusy, http.StatusConflict, false, "refresh of this list is running"},
		{"cooling down", errSourceCooldown, http.StatusTooManyRequests, false, "less than a minute ago"},
		{"caller's fault", badInput("the file has no column %q", "Path"), http.StatusBadRequest, false, `no column "Path"`},
		{"server's fault", internal, http.StatusInternalServerError, false, "server log"},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		h.respondSourceResult(w, dict, c.err)
		var env envelope
		if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if w.Code != c.status || env.Success != c.success {
			t.Errorf("%s: status %d success %v, want %d %v", c.name, w.Code, env.Success, c.status, c.success)
		}
		if c.want != "" && !strings.Contains(env.Error, c.want) {
			t.Errorf("%s: error %q, want it to mention %q", c.name, env.Error, c.want)
		}
		if strings.Contains(env.Error, "10.1.2.3") {
			t.Errorf("%s: error %q leaks an internal address", c.name, env.Error)
		}
		// A load still running still answers with the list, so the editor can
		// show it filling in.
		if c.success && !bytes.Contains(env.Data, []byte(`"lolrmm"`)) {
			t.Errorf("%s: data %s does not carry the list", c.name, env.Data)
		}
	}
}

func TestRespondManagerError(t *testing.T) {
	h := testHandler()

	w := httptest.NewRecorder()
	h.respondManagerError(w, "Failed to add column", badInput("column %q already exists", "ioc"))
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), `column \"ioc\" already exists`) {
		t.Errorf("invalid input: %d %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	h.respondManagerError(w, "Failed to add column", errors.New("code: 60, table logs.dict_x does not exist"))
	if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), "dict_x") ||
		!strings.Contains(w.Body.String(), "Failed to add column") {
		t.Errorf("internal error: %d %s", w.Code, w.Body.String())
	}
}

// A gzip import is capped on what it expands to. Running past the cap must be
// an error: a clean end of file would read as a short CSV that imported fine.
func TestCappedReaderFailsRatherThanTruncates(t *testing.T) {
	under := &cappedReader{r: strings.NewReader("abcdef"), max: 6}
	if got, err := io.ReadAll(under); err != nil || string(got) != "abcdef" {
		t.Errorf("at the cap: %q, %v", got, err)
	}

	over := &cappedReader{r: strings.NewReader("abcdefg"), max: 6}
	_, err := io.ReadAll(over)
	if err == nil || !IsInvalidInput(err) {
		t.Fatalf("past the cap: err = %v, want an invalid-input error", err)
	}
	if importStatus(err) != http.StatusBadRequest {
		t.Errorf("an oversized upload is the caller's to fix, got status %d", importStatus(err))
	}
	if importStatus(errors.New("insert failed")) != http.StatusInternalServerError {
		t.Error("a failure of ours must be a 500")
	}
}

// An action id from another scope answers exactly like one that does not exist.
func TestDictionaryActionScope(t *testing.T) {
	h := testHandler()
	cases := []struct {
		name           string
		fractal, prism string
		action         DictionaryAction
		want           bool
	}{
		{"own fractal", "f-1", "", DictionaryAction{FractalID: "f-1"}, true},
		{"other fractal", "f-1", "", DictionaryAction{FractalID: "f-2"}, false},
		{"own prism", "", "p-1", DictionaryAction{PrismID: "p-1"}, true},
		{"fractal action from a prism session", "", "p-1", DictionaryAction{FractalID: "f-1"}, false},
		{"prism action from a fractal session", "f-1", "", DictionaryAction{PrismID: "p-1"}, false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		ctx := context.WithValue(r.Context(), "selected_fractal", c.fractal)
		ctx = context.WithValue(ctx, "selected_prism", c.prism)
		w := httptest.NewRecorder()
		got := h.requireDictionaryActionInScope(w, r.WithContext(ctx), &c.action)
		if got != c.want {
			t.Errorf("%s: in scope = %v, want %v", c.name, got, c.want)
		}
		if !got && w.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404 so the id's existence is not disclosed", c.name, w.Code)
		}
	}
}
