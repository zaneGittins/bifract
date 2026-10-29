package dictionaries

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/api"

	"github.com/go-chi/chi/v5"
)

// call runs a handler as who, in fractal A unless the context says otherwise,
// with chi URL params given as name, value pairs.
func call(t *testing.T, h http.HandlerFunc, method, target string, body io.Reader, who principal, scope context.Context, params ...string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, target, body)
	c := scope
	if c == nil {
		c = context.WithValue(r.Context(), "selected_fractal", fractalA)
	}
	if who.user != nil {
		c = context.WithValue(c, "user", who.user)
	}
	if who.role != "" {
		c = context.WithValue(c, "fractal_role", string(who.role))
	}
	rc := chi.NewRouteContext()
	for i := 0; i+1 < len(params); i += 2 {
		rc.URLParams.Add(params[i], params[i+1])
	}
	c = context.WithValue(c, chi.RouteCtxKey, rc)
	w := httptest.NewRecorder()
	h(w, r.WithContext(c))
	return w
}

func body(s string) io.Reader { return strings.NewReader(s) }

func decode(t *testing.T, w *httptest.ResponseRecorder) envelope {
	t.Helper()
	var env envelope
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("not a JSON envelope (%d): %s", w.Code, w.Body.String())
	}
	return env
}

func inFractal(id string) context.Context {
	return context.WithValue(context.Background(), "selected_fractal", id)
}

// A list is reachable by id from its own scope, and from anywhere when global;
// from any other scope it answers exactly like one that does not exist.
func TestHandlerScopesListsByID(t *testing.T) {
	w := newWorld(t)
	h := NewHandler(w.m, nil)
	own := mustCreate(t, w, CreateOptions{FractalID: fractalA, Name: "own", Columns: cols("k")})
	other := mustCreate(t, w, CreateOptions{FractalID: fractalB, Name: "other", Columns: cols("k")})
	shared := mustCreate(t, w, CreateOptions{FractalID: fractalB, Name: "shared", IsGlobal: true, Columns: cols("k")})

	for _, c := range []struct {
		id   string
		want int
	}{{own.ID, 200}, {shared.ID, 200}, {other.ID, 404}, {"missing", 404}} {
		if got := call(t, h.HandleGetDictionary, "GET", "/", nil, viewer, nil, "id", c.id).Code; got != c.want {
			t.Errorf("get %s: %d, want %d", c.id, got, c.want)
		}
		if c.want != 404 {
			continue
		}
		if got := call(t, h.HandleDeleteDictionary, "DELETE", "/", nil, analyst, nil, "id", c.id).Code; got != 404 {
			t.Errorf("delete %s: %d, want 404", c.id, got)
		}
	}

	rec := call(t, h.HandleListDictionaries, "GET", "/", nil, viewer, inFractal(fractalB))
	var list struct {
		Data []Dictionary `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &list)
	var names []string
	for _, d := range list.Data {
		names = append(names, d.Name)
	}
	if rec.Code != 200 || !slices.Equal(names, []string{"other", "shared"}) {
		t.Errorf("list from fractal B: %d %v", rec.Code, names)
	}
}

// A global list is readable from every scope but belongs to the one it was
// created in. Writing it from any other would let an analyst of one fractal
// change what every fractal's queries and alerts match against.
func TestHandlerGlobalListsAreWrittenFromHome(t *testing.T) {
	w := newWorld(t)
	h := NewHandler(w.m, nil)
	shared := mustCreate(t, w, CreateOptions{FractalID: fractalB, Name: "shared", IsGlobal: true, Columns: cols("k")})

	writes := []struct {
		name string
		fn   http.HandlerFunc
		body string
	}{
		{"update", h.HandleUpdateDictionary, `{"name":"shared","is_global":false}`},
		{"add column", h.HandleAddColumn, `{"name":"c"}`},
		{"upsert rows", h.HandleUpsertRows, `{"rows":[{"k":"x"}]}`},
		{"delete row", h.HandleDeleteRow, `{"key":"x"}`},
		{"reload", h.HandleReloadDictionary, ""},
		{"delete", h.HandleDeleteDictionary, ""},
	}
	for _, wr := range writes {
		if got := call(t, wr.fn, "POST", "/", body(wr.body), analyst, nil, "id", shared.ID).Code; got != 403 {
			t.Errorf("%s from another fractal: %d, want 403", wr.name, got)
		}
	}
	if got := call(t, h.HandleGetDictionary, "GET", "/", nil, viewer, nil, "id", shared.ID).Code; got != 200 {
		t.Errorf("read from another fractal: %d, want 200", got)
	}
	upd := `{"name":"shared","description":"d","is_global":true}`
	if got := call(t, h.HandleUpdateDictionary, "PUT", "/", body(upd), analyst, inFractal(fractalB), "id", shared.ID).Code; got != 200 {
		t.Errorf("update from home: %d, want 200", got)
	}
	if got := call(t, h.HandleUpdateDictionary, "PUT", "/", body(upd), admin, nil, "id", shared.ID).Code; got != 200 {
		t.Errorf("update by a tenant admin: %d, want 200", got)
	}
	// An admin's request held below tenant admin, as an unconfirmed AI tool is,
	// gets no more than an analyst would.
	capped := api.WithCeiling(inFractal(fractalA), api.AccessAnalyst)
	if got := call(t, h.HandleUpdateDictionary, "PUT", "/", body(upd), admin, capped, "id", shared.ID).Code; got != 403 {
		t.Errorf("update by a capped admin: %d, want 403", got)
	}
}

// Create and the column routes report what the manager refuses as a 400 with
// its reason, and a failure of ours as a 500 without detail.
func TestHandlerCreateAndColumns(t *testing.T) {
	w := newWorld(t)
	h := NewHandler(w.m, nil)

	rec := call(t, h.HandleCreateDictionary, "POST", "/", body(`{"name":"users","columns":[{"name":"email"}]}`), analyst, nil)
	env := decode(t, rec)
	var d Dictionary
	_ = json.Unmarshal(env.Data, &d)
	if rec.Code != 200 || d.KeyColumn != "email" || d.FractalID != fractalA {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	if rec, _ := w.dict(d.ID); rec.createdBy != "a" {
		t.Errorf("created by %q, want the session user", rec.createdBy)
	}

	rec = call(t, h.HandleCreateDictionary, "POST", "/", body(`{"name":"p","kind":"pattern","columns":[{"name":"re"},{"name":"_match"}]}`), analyst, nil)
	if rec.Code != 400 || !strings.Contains(decode(t, rec).Error, "reserved") {
		t.Errorf("reserved column: %d %s", rec.Code, rec.Body.String())
	}
	w.ch.FailExec("CREATE TABLE", errors.New("code: 999, secret detail"))
	rec = call(t, h.HandleCreateDictionary, "POST", "/", body(`{"name":"x","columns":[{"name":"k"}]}`), analyst, nil)
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "secret") {
		t.Errorf("ClickHouse failure: %d %s", rec.Code, rec.Body.String())
	}

	id := d.ID
	steps := []struct {
		name  string
		h     http.HandlerFunc
		body  string
		param string
		want  int
	}{
		{"add", h.HandleAddColumn, `{"name":"dept"}`, "", 200},
		{"add duplicate", h.HandleAddColumn, `{"name":"dept"}`, "", 400},
		{"add unnamed", h.HandleAddColumn, `{}`, "", 400},
		{"add bad json", h.HandleAddColumn, `{`, "", 400},
		{"set key", h.HandleSetColumnKey, "", "dept", 200},
		{"set key missing", h.HandleSetColumnKey, "", "nope", 400},
		{"unset key", h.HandleUnsetColumnKey, "", "dept", 200},
		{"unset primary", h.HandleUnsetColumnKey, "", "email", 400},
		{"remove", h.HandleRemoveColumn, "", "dept", 200},
		{"remove key", h.HandleRemoveColumn, "", "email", 400},
		{"update", h.HandleUpdateDictionary, `{"name":"renamed","description":"d"}`, "", 200},
		{"update bad json", h.HandleUpdateDictionary, `{`, "", 400},
		{"reload", h.HandleReloadDictionary, "", "", 200},
	}
	for _, s := range steps {
		rec := call(t, s.h, "POST", "/", body(s.body), analyst, nil, "id", id, "name", s.param)
		if rec.Code != s.want {
			t.Errorf("%s: %d %s, want %d", s.name, rec.Code, rec.Body.String(), s.want)
		}
	}
	if got, _ := w.dict(id); got.name != "renamed" {
		t.Errorf("rename not stored: %q", got.name)
	}
}

// The case toggle refuses unconfirmed collisions with the count, and the
// preview route reports them.
func TestHandlerCaseInsensitivity(t *testing.T) {
	w := newWorld(t)
	h := NewHandler(w.m, nil)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("email", "A")}, {Fields: fields("email", "a")}}); err != nil {
		t.Fatal(err)
	}

	rec := call(t, h.HandleKeyCollisions, "GET", "/", nil, viewer, nil, "id", d.ID)
	var report KeyCollisionReport
	_ = json.Unmarshal(decode(t, rec).Data, &report)
	if rec.Code != 200 || report.Collisions != 1 {
		t.Errorf("preview: %d %+v", rec.Code, report)
	}
	rec = call(t, h.HandleSetCaseInsensitiveKeys, "POST", "/", body(`{"enabled":true}`), analyst, nil, "id", d.ID)
	if rec.Code != 400 || !strings.Contains(decode(t, rec).Error, "confirm_collisions") {
		t.Errorf("unconfirmed: %d %s", rec.Code, rec.Body.String())
	}
	rec = call(t, h.HandleSetCaseInsensitiveKeys, "POST", "/", body(`{"enabled":true,"confirm_collisions":true}`), analyst, nil, "id", d.ID)
	if rec.Code != 200 {
		t.Errorf("confirmed: %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(t, h.HandleSetCaseInsensitiveKeys, "POST", "/", body(`{`), analyst, nil, "id", d.ID); rec.Code != 400 {
		t.Errorf("bad body: %d", rec.Code)
	}
}

// Rows: a page carries its total and bounds, a key with a slash is deleted
// through the query string, and a row the list's kind cannot hold is a 400.
func TestHandlerRows(t *testing.T) {
	w := newWorld(t)
	h := NewHandler(w.m, nil)
	d := mustCreate(t, w, CreateOptions{Name: "paths", Columns: cols("path")})

	rec := call(t, h.HandleUpsertRows, "POST", "/", body(`{"rows":[{"fields":{"path":"a/b"}},{"fields":{"path":"c"}},{"fields":{"path":"d"}}]}`), analyst, nil, "id", d.ID)
	if rec.Code != 200 {
		t.Fatalf("upsert: %d %s", rec.Code, rec.Body.String())
	}
	rec = call(t, h.HandleGetRows, "GET", "/?limit=2&offset=1&search=", nil, viewer, nil, "id", d.ID)
	var page struct {
		Data []DictionaryRow `json:"data"`
		Page struct{ Total, Limit, Offset int }
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if rec.Code != 200 || page.Page.Total != 3 || page.Page.Limit != 2 || page.Page.Offset != 1 || len(page.Data) != 2 || page.Data[0].Key != "c" {
		t.Errorf("page: %d %s", rec.Code, rec.Body.String())
	}

	if rec := call(t, h.HandleDeleteRow, "DELETE", "/?key=a%2Fb", nil, analyst, nil, "id", d.ID); rec.Code != 200 {
		t.Errorf("delete by query key: %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(t, h.HandleDeleteRow, "DELETE", "/", nil, analyst, nil, "id", d.ID, "key", "c"); rec.Code != 200 {
		t.Errorf("delete by path key: %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(t, h.HandleDeleteRow, "DELETE", "/?key=gone", nil, analyst, nil, "id", d.ID); rec.Code != 400 {
		t.Errorf("delete of a missing key: %d", rec.Code)
	}
	if got := keysOf(w.liveRows(d.ID), "path"); !slices.Equal(got, []string{"d"}) {
		t.Errorf("rows %v", got)
	}

	rec = call(t, h.HandleUpsertRows, "POST", "/", body(`{"rows":[{"fields":{"path":""}}]}`), analyst, nil, "id", d.ID)
	if rec.Code != 400 {
		t.Errorf("blank key: %d", rec.Code)
	}
	if rec := call(t, h.HandleUpsertRows, "POST", "/", body(`{`), analyst, nil, "id", d.ID); rec.Code != 400 {
		t.Errorf("bad body: %d", rec.Code)
	}
}

// An import takes a raw body, a multipart file or gzip; a parse error is a 400
// that still says how many rows landed before it.
func TestHandlerImportCSV(t *testing.T) {
	w := newWorld(t)
	h := NewHandler(w.m, nil)
	d := mustCreate(t, w, CreateOptions{Name: "hosts", Columns: cols("host")})
	imported := func(rec *httptest.ResponseRecorder) int {
		var n map[string]int
		_ = json.Unmarshal(decode(t, rec).Data, &n)
		return n["imported"]
	}

	rec := call(t, h.HandleImportCSV, "POST", "/", body("host\na\nb\n"), analyst, nil, "id", d.ID)
	if rec.Code != 200 || imported(rec) != 2 {
		t.Errorf("raw: %d %s", rec.Code, rec.Body.String())
	}

	var mp bytes.Buffer
	mw := multipart.NewWriter(&mp)
	fw, _ := mw.CreateFormFile("file", "hosts.csv")
	_, _ = fw.Write([]byte("host\nc\n"))
	_ = mw.Close()
	r := httptest.NewRequest("POST", "/?reload=false", &mp)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	reloads := len(w.reloads)
	rec = serveWith(h.HandleImportCSV, r, analyst, d.ID)
	if rec.Code != 200 || imported(rec) != 1 || len(w.reloads) != reloads {
		t.Errorf("multipart deferred: %d %s, %d reloads", rec.Code, rec.Body.String(), len(w.reloads)-reloads)
	}

	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write([]byte("host\nd\n"))
	_ = zw.Close()
	r = httptest.NewRequest("POST", "/", &gz)
	r.Header.Set("Content-Encoding", "gzip")
	if rec := serveWith(h.HandleImportCSV, r, analyst, d.ID); rec.Code != 200 || imported(rec) != 1 {
		t.Errorf("gzip: %d %s", rec.Code, rec.Body.String())
	}
	r = httptest.NewRequest("POST", "/", body("not gzip"))
	r.Header.Set("Content-Encoding", "gzip")
	if rec := serveWith(h.HandleImportCSV, r, analyst, d.ID); rec.Code != 400 {
		t.Errorf("bad gzip: %d", rec.Code)
	}
	r = httptest.NewRequest("POST", "/", body("x"))
	r.Header.Set("Content-Type", "multipart/form-data; boundary=nope")
	if rec := serveWith(h.HandleImportCSV, r, analyst, d.ID); rec.Code != 400 {
		t.Errorf("bad multipart: %d", rec.Code)
	}

	rec = call(t, h.HandleImportCSV, "POST", "/", body("host\ne\n\"unterminated\n"), analyst, nil, "id", d.ID)
	env := decode(t, rec)
	if rec.Code != 400 || env.Success || imported(rec) != 1 {
		t.Errorf("parse error: %d %s", rec.Code, rec.Body.String())
	}

	w.ch.FailExec("INSERT INTO", errors.New("too many parts"))
	if rec := call(t, h.HandleImportCSV, "POST", "/", body("host\nf\n"), analyst, nil, "id", d.ID); rec.Code != 500 {
		t.Errorf("server failure: %d", rec.Code)
	}
}

func serveWith(h http.HandlerFunc, r *http.Request, who principal, id string) *httptest.ResponseRecorder {
	c := context.WithValue(r.Context(), "selected_fractal", fractalA)
	c = context.WithValue(c, "user", who.user)
	c = context.WithValue(c, "fractal_role", string(who.role))
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", id)
	c = context.WithValue(c, chi.RouteCtxKey, rc)
	w := httptest.NewRecorder()
	h(w, r.WithContext(c))
	return w
}

// An export is a CSV download named after the list, with a quote in the name
// unable to break out of the header.
func TestHandlerExportCSV(t *testing.T) {
	w := newWorld(t)
	h := NewHandler(w.m, nil)
	d := mustCreate(t, w, CreateOptions{Name: `my "list"`, Columns: cols("host")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("host", "a")}}); err != nil {
		t.Fatal(err)
	}
	rec := call(t, h.HandleExportCSV, "GET", "/", nil, viewer, nil, "id", d.ID)
	if rec.Code != 200 || rec.Body.String() != "host\na\n" {
		t.Errorf("export: %d %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Disposition"); got != `attachment; filename="my _list_.csv"` {
		t.Errorf("disposition %q", got)
	}
}

// Source routes: attach and detach as an administrator, a busy list is a 409,
// and a refresh inside the cooldown is a 429.
func TestHandlerSourceRoutes(t *testing.T) {
	w := sourcedWorld(t, false)
	h := NewHandler(w.m, nil)
	f := newFeed(t, "host,owner\nweb1,ops\n")

	rec := call(t, h.HandleCreateDictionary, "POST", "/", body(`{"name":"feed","source":{"url":"`+f.url+`","key_field":"host"}}`), admin, nil)
	var d Dictionary
	_ = json.Unmarshal(decode(t, rec).Data, &d)
	if rec.Code != 200 || d.Source == nil {
		t.Fatalf("create with a source: %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(t, h.HandleSyncSource, "POST", "/", nil, analyst, nil, "id", d.ID); rec.Code != http.StatusTooManyRequests {
		t.Errorf("sync inside the cooldown: %d", rec.Code)
	}
	w.age(d.ID)
	if rec := call(t, h.HandleSyncSource, "POST", "/", nil, analyst, nil, "id", d.ID); rec.Code != 200 {
		t.Errorf("sync: %d %s", rec.Code, rec.Body.String())
	}

	w.busy[sourceLockID(d.ID)] = true
	for name, route := range map[string]http.HandlerFunc{"detach": h.HandleDetachSource, "delete": h.HandleDeleteDictionary} {
		if rec := call(t, route, "POST", "/", nil, admin, nil, "id", d.ID); rec.Code != http.StatusConflict {
			t.Errorf("%s under a held lock: %d", name, rec.Code)
		}
	}
	delete(w.busy, sourceLockID(d.ID))

	if rec := call(t, h.HandleSetSource, "POST", "/", body(`{"url":"`+f.url+`","key_field":"nope"}`), admin, nil, "id", d.ID); rec.Code != 400 {
		t.Errorf("set a source with a missing key column: %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(t, h.HandleSetSource, "POST", "/", body(`{`), admin, nil, "id", d.ID); rec.Code != 400 {
		t.Errorf("bad body: %d", rec.Code)
	}
	if rec := call(t, h.HandleSetSource, "POST", "/", body(`{"url":"`+f.url+`","key_field":"host","refresh_minutes":15}`), admin, nil, "id", d.ID); rec.Code != 200 {
		t.Errorf("set source: %d %s", rec.Code, rec.Body.String())
	}
	rec = call(t, h.HandleDetachSource, "POST", "/", nil, admin, nil, "id", d.ID)
	if rec.Code != 200 {
		t.Errorf("detach: %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(t, h.HandleDeleteDictionary, "DELETE", "/", nil, analyst, nil, "id", d.ID); rec.Code != 200 {
		t.Errorf("delete: %d", rec.Code)
	}
}

// Action routes resolve the action, then check it belongs to the session's
// scope: another scope's action is a 404 like a missing one.
func TestHandlerDictionaryActions(t *testing.T) {
	w := newWorld(t)
	h := NewHandler(w.m, nil)

	rec := call(t, h.HandleCreateDictionaryAction, "POST", "/", body(`{"name":"collect","dictionary_name":"seen","enabled":true}`), analyst, nil)
	var a DictionaryAction
	_ = json.Unmarshal(decode(t, rec).Data, &a)
	if rec.Code != 200 || a.FractalID != fractalA || a.CreatedBy != "a" {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	other, _ := w.m.CreateDictionaryAction(ctx, "theirs", "", "x", 0, true, "", fractalB, "")

	rec = call(t, h.HandleListDictionaryActions, "GET", "/", nil, viewer, nil)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), a.ID) || strings.Contains(rec.Body.String(), other.ID) {
		t.Errorf("list: %d %s", rec.Code, rec.Body.String())
	}
	for _, c := range []struct {
		name string
		h    http.HandlerFunc
		id   string
		body string
		want int
	}{
		{"get", h.HandleGetDictionaryAction, a.ID, "", 200},
		{"get other", h.HandleGetDictionaryAction, other.ID, "", 404},
		{"get missing", h.HandleGetDictionaryAction, "missing", "", 404},
		{"update", h.HandleUpdateDictionaryAction, a.ID, `{"name":"renamed","dictionary_name":"seen"}`, 200},
		{"update bad json", h.HandleUpdateDictionaryAction, a.ID, `{`, 400},
		{"update other", h.HandleUpdateDictionaryAction, other.ID, `{}`, 404},
		{"update missing", h.HandleUpdateDictionaryAction, "missing", `{}`, 404},
		{"delete other", h.HandleDeleteDictionaryAction, other.ID, "", 404},
		{"delete missing", h.HandleDeleteDictionaryAction, "missing", "", 404},
		{"delete", h.HandleDeleteDictionaryAction, a.ID, "", 200},
	} {
		if rec := call(t, c.h, "POST", "/", body(c.body), analyst, nil, "id", c.id); rec.Code != c.want {
			t.Errorf("%s: %d %s, want %d", c.name, rec.Code, rec.Body.String(), c.want)
		}
	}
	if _, err := w.m.GetDictionaryAction(ctx, other.ID); err != nil {
		t.Error("another scope's action was deleted")
	}
}

// A database failure behind a read route is a 500 whose body carries none of
// the failure's detail.
func TestHandlerReadFailuresAre500s(t *testing.T) {
	w := newWorld(t)
	h := NewHandler(w.m, nil)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email")})
	if _, err := w.m.CreateDictionaryAction(ctx, "collect", "", "users", 0, true, "", fractalA, ""); err != nil {
		t.Fatal(err)
	}
	const secret = "dial tcp 10.9.9.9:9000"
	w.ch.Respond(func(string, []any) (fakedb.CHResult, bool) {
		return fakedb.CHResult{Err: errors.New(secret)}, true
	})
	for name, route := range map[string]http.HandlerFunc{
		"rows":       h.HandleGetRows,
		"collisions": h.HandleKeyCollisions,
	} {
		rec := call(t, route, "GET", "/", nil, viewer, nil, "id", d.ID)
		if rec.Code != 500 || strings.Contains(rec.Body.String(), "10.9.9.9") {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}

	w.pg.Respond(func(q string, args []driver.Value) (fakedb.PGResult, bool) {
		if strings.Contains(q, "ORDER BY") {
			return fakedb.PGResult{Err: errors.New(secret)}, true
		}
		return w.answerPG(q, args)
	})
	for name, route := range map[string]http.HandlerFunc{
		"lists":   h.HandleListDictionaries,
		"actions": h.HandleListDictionaryActions,
	} {
		rec := call(t, route, "GET", "/", nil, viewer, nil)
		if rec.Code != 500 || strings.Contains(rec.Body.String(), "10.9.9.9") {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
}
