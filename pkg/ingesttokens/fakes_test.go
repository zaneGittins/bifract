package ingesttokens

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"

	"github.com/go-chi/chi/v5"
)

const (
	fractalA = "11111111-1111-1111-1111-111111111111"
	fractalB = "22222222-2222-2222-2222-222222222222"
)

var rowTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

type tokenRow struct {
	id, name, prefix, hash, value string
	fractalID, parser             string
	normalizerID                  string
	tsFields                      string
	createdBy                     string
	active, isDefault             bool
}

func (t tokenRow) values(normNames map[string]string) []driver.Value {
	var normID, normName driver.Value
	if t.normalizerID != "" {
		normID = t.normalizerID
		normName = normNames[t.normalizerID]
	}
	ts := t.tsFields
	if ts == "" {
		ts = "[]"
	}
	return []driver.Value{
		t.id, t.name, "", t.prefix, t.value, t.fractalID,
		t.parser, normID, normName,
		[]byte(ts), t.active, t.isDefault,
		t.createdBy, rowTime, rowTime, nil, int64(0), int64(0),
	}
}

type fractalRow struct {
	id, name, createdBy string
}

// tokenDB is an in-memory ingest_tokens table (plus the fractals, normalizers
// and permission rows the package reads) behind the scripted driver.
type tokenDB struct {
	mu        sync.Mutex
	pg        *fakedb.PG
	client    *storage.PostgresClient
	tokens    map[string]*tokenRow
	nextID    int
	fractals  []fractalRow
	normNames map[string]string
	// defaultNormalizer is the id the default-normalizer lookup answers, or "".
	defaultNormalizer string
	// roles: fractal id -> username -> role.
	roles map[string]map[string]string
	// failTokens fails every ingest_tokens statement whose query contains the
	// key, with the mapped error.
	failTokens map[string]error
	failList   error
}

func newTokenDB() *tokenDB {
	client, pg := storagetest.Postgres()
	db := &tokenDB{
		pg:         pg,
		client:     client,
		tokens:     map[string]*tokenRow{},
		fractals:   []fractalRow{{id: fractalA, name: "alpha"}, {id: fractalB, name: "beta"}},
		normNames:  map[string]string{"norm-1": "ecs", "norm-default": "default"},
		roles:      map[string]map[string]string{},
		failTokens: map[string]error{},
	}
	pg.Respond(db.respond)
	return db
}

func (db *tokenDB) add(t tokenRow) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if t.parser == "" {
		t.parser = "json"
	}
	if t.hash == "" {
		t.hash = "hash-of-" + t.id
	}
	db.tokens[t.id] = &t
}

func (db *tokenDB) get(id string) (tokenRow, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	t, ok := db.tokens[id]
	if !ok {
		return tokenRow{}, false
	}
	return *t, true
}

func (db *tokenDB) fractalValues(f fractalRow) []driver.Value {
	return []driver.Value{f.id, f.name, "", false, false, f.createdBy, rowTime, rowTime, nil, nil, nil, "reject", int64(0), int64(0), nil, nil, nil, nil}
}

func (db *tokenDB) sortedTokens() []*tokenRow {
	ids := make([]string, 0, len(db.tokens))
	for id := range db.tokens {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*tokenRow, len(ids))
	for i, id := range ids {
		out[i] = db.tokens[id]
	}
	return out
}

func (db *tokenDB) respond(q string, args []driver.Value) (fakedb.PGResult, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	str := func(i int) string { s, _ := args[i].(string); return s }

	switch {
	case strings.Contains(q, "FROM fractal_permissions"):
		return fakedb.Row(db.roles[str(0)][str(1)]), true
	case strings.Contains(q, "FROM fractals") && strings.Contains(q, "WHERE id = $1"):
		for _, f := range db.fractals {
			if f.id == str(0) {
				return fakedb.Row(db.fractalValues(f)...), true
			}
		}
		return fakedb.PGResult{}, true
	case strings.Contains(q, "FROM fractals"):
		if db.failList != nil {
			return fakedb.PGResult{Err: db.failList}, true
		}
		var rows [][]driver.Value
		for _, f := range db.fractals {
			rows = append(rows, db.fractalValues(f))
		}
		return fakedb.RowsOf(rows...), true
	case strings.Contains(q, "FROM normalizers WHERE is_default"):
		if db.defaultNormalizer == "" {
			return fakedb.PGResult{}, true
		}
		return fakedb.Row(db.defaultNormalizer), true
	case !strings.Contains(q, "ingest_tokens"):
		return fakedb.PGResult{}, false
	}
	for sub, err := range db.failTokens {
		if strings.Contains(q, sub) {
			return fakedb.PGResult{Err: err}, true
		}
	}

	switch {
	case strings.Contains(q, "INSERT INTO ingest_tokens"):
		db.nextID++
		t := &tokenRow{
			id:        "tok-" + strconv.Itoa(db.nextID),
			name:      str(0),
			prefix:    str(2),
			hash:      str(3),
			value:     str(4),
			fractalID: str(5),
			active:    true,
		}
		if len(args) == 8 { // CreateDefaultToken
			t.parser, t.isDefault, t.tsFields = "json", true, "[]"
			t.normalizerID = str(6)
			t.createdBy = str(7)
		} else {
			t.parser = str(6)
			t.normalizerID = str(7)
			t.tsFields = str(8)
			t.createdBy = str(9)
		}
		db.tokens[t.id] = t
		return fakedb.Row(t.values(db.normNames)...), true

	case strings.Contains(q, "t.token_hash = $1"):
		for _, t := range db.tokens {
			if t.hash == str(0) && t.active {
				var nid, nname, nver, tr, fm, vm, nts driver.Value
				if t.normalizerID != "" {
					nid, nname, nver = t.normalizerID, db.normNames[t.normalizerID], int64(3)
					tr, fm, vm, nts = []byte("[]"), []byte(`[{"sources":["src"],"target":"dst"}]`), []byte("[]"), []byte("[]")
				}
				return fakedb.Row(t.id, t.fractalID, t.parser, []byte(t.tsFields), nid, nname, nver, tr, fm, vm, nts), true
			}
		}
		return fakedb.PGResult{}, true

	case strings.Contains(q, "SELECT t.id, t.name"):
		var rows [][]driver.Value
		for _, t := range db.sortedTokens() {
			if strings.Contains(q, "WHERE t.id = $1") {
				if t.id == str(0) && t.fractalID == str(1) {
					rows = append(rows, t.values(db.normNames))
				}
			} else if t.fractalID == str(0) {
				rows = append(rows, t.values(db.normNames))
			}
		}
		return fakedb.RowsOf(rows...), true

	case strings.Contains(q, "SELECT is_active, is_default FROM ingest_tokens"):
		if t, ok := db.tokens[str(0)]; ok && t.fractalID == str(1) {
			return fakedb.Row(t.active, t.isDefault), true
		}
		return fakedb.PGResult{}, true

	case strings.Contains(q, "SELECT is_active FROM ingest_tokens"):
		if t, ok := db.tokens[str(0)]; ok && t.fractalID == str(1) {
			return fakedb.Row(t.active), true
		}
		return fakedb.PGResult{}, true

	case strings.Contains(q, "SELECT COUNT(*) FROM ingest_tokens"):
		n := int64(0)
		for _, t := range db.tokens {
			if t.fractalID == str(0) && (t.active || !strings.Contains(q, "is_active = true")) {
				n++
			}
		}
		return fakedb.Row(n), true

	case strings.Contains(q, "DELETE FROM ingest_tokens"):
		if t, ok := db.tokens[str(0)]; ok && t.fractalID == str(1) {
			delete(db.tokens, t.id)
			return fakedb.Affected(1), true
		}
		return fakedb.Affected(0), true

	case strings.Contains(q, "SET is_active = NOT is_active"):
		if t, ok := db.tokens[str(0)]; ok && t.fractalID == str(1) {
			t.active = !t.active
			return fakedb.Affected(1), true
		}
		return fakedb.Affected(0), true

	case strings.Contains(q, "UPDATE ingest_tokens"):
		return fakedb.Affected(1), true
	}
	return fakedb.PGResult{}, false
}

// writes returns the statements that change ingest_tokens.
func (db *tokenDB) writes() []fakedb.Call {
	var out []fakedb.Call
	for _, c := range db.pg.Calls() {
		if strings.Contains(c.Query, "INSERT INTO ingest_tokens") || strings.Contains(c.Query, "UPDATE ingest_tokens") || strings.Contains(c.Query, "DELETE FROM ingest_tokens") {
			out = append(out, c)
		}
	}
	return out
}

func newTestHandler(db *tokenDB, cache *TokenCache) *Handler {
	h := NewHandler(NewStorage(db.client), fractals.NewManager(db.client, nil), cache)
	h.SetRBAC(rbac.NewResolver(db.client))
	return h
}

func asUser(u *storage.User) func(context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		if u == nil {
			return ctx
		}
		ctx = context.WithValue(ctx, "user", u)
		return context.WithValue(ctx, storage.AttributionUserKey, u.Username)
	}
}

func call(fn http.HandlerFunc, method, body string, who func(context.Context) context.Context, params ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/", strings.NewReader(body))
	rctx := chi.NewRouteContext()
	for i := 0; i+1 < len(params); i += 2 {
		rctx.URLParams.Add(params[i], params[i+1])
	}
	r = r.WithContext(who(context.WithValue(r.Context(), chi.RouteCtxKey, rctx)))
	rec := httptest.NewRecorder()
	fn(rec, r)
	return rec
}

type envelope struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) envelope {
	t.Helper()
	var e envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("response is not JSON: %v: %s", err, rec.Body.String())
	}
	return e
}
