package apikeys

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/auth"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"

	"github.com/go-chi/chi/v5"
)

const (
	fractalA = "11111111-1111-1111-1111-111111111111"
	fractalB = "22222222-2222-2222-2222-222222222222"
	prismA   = "33333333-3333-3333-3333-333333333333"
	prismB   = "44444444-4444-4444-4444-444444444444"
)

var rowTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// keyRow is one api_keys row as the fake database holds it.
type keyRow struct {
	id, name, keyID, keyHash string
	fractalID, prismID       string
	createdBy, role          string
	expires                  *time.Time
	active, tenant           bool
}

// values renders the row in selectColumns order.
func (k keyRow) values(fractalNames, prismNames map[string]string) []driver.Value {
	var exp driver.Value
	if k.expires != nil {
		exp = *k.expires
	}
	return []driver.Value{
		k.id, k.name, "", k.keyID,
		k.fractalID, fractalNames[k.fractalID],
		k.prismID, prismNames[k.prismID],
		k.createdBy, exp, k.active, k.role, k.tenant,
		rowTime, rowTime, nil, int64(0),
	}
}

// keyDB is an in-memory api_keys table behind the scripted driver. It answers
// by reading the placeholders the SQL names, so a statement that binds its
// scope to the wrong argument reads the wrong value and misses, exactly as
// Postgres would.
type keyDB struct {
	mu           sync.Mutex
	pg           *fakedb.PG
	client       *storage.PostgresClient
	keys         map[string]*keyRow
	nextID       int
	fractalNames map[string]string
	prismNames   map[string]string
	// fractalRoles and prismRoles hold permission rows: scope id -> username -> role.
	fractalRoles map[string]map[string]string
	prismRoles   map[string]map[string]string
	// failAll fails every api_keys statement.
	failAll error
}

func newKeyDB() *keyDB {
	client, pg := storagetest.Postgres()
	db := &keyDB{
		pg:           pg,
		client:       client,
		keys:         map[string]*keyRow{},
		fractalNames: map[string]string{fractalA: "alpha-logs", fractalB: "beta"},
		prismNames:   map[string]string{prismA: "Prism A", prismB: "Prism B"},
		fractalRoles: map[string]map[string]string{},
		prismRoles:   map[string]map[string]string{},
	}
	pg.Respond(db.respond)
	return db
}

func (db *keyDB) add(k keyRow) *keyRow {
	db.mu.Lock()
	defer db.mu.Unlock()
	if k.keyHash == "" {
		k.keyHash = "hash-of-" + k.id
	}
	db.keys[k.id] = &k
	return &k
}

func (db *keyDB) get(id string) (keyRow, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	k, ok := db.keys[id]
	if !ok {
		return keyRow{}, false
	}
	return *k, true
}

var (
	idPlaceholder    = regexp.MustCompile(`\bid = \$(\d+)`)
	scopePlaceholder = regexp.MustCompile(`\b(fractal_id|prism_id) = \$(\d+)`)
)

func placeholderArg(args []driver.Value, n string) string {
	i, _ := strconv.Atoi(n)
	if i < 1 || i > len(args) {
		return ""
	}
	s, _ := args[i-1].(string)
	return s
}

// inScope reports whether row k satisfies the WHERE clause of query.
func (db *keyDB) inScope(query string, args []driver.Value, k *keyRow) bool {
	where := query[strings.LastIndex(query, "WHERE"):]
	if m := idPlaceholder.FindStringSubmatch(where); m != nil && placeholderArg(args, m[1]) != k.id {
		return false
	}
	if strings.Contains(where, "fractal_id IS NULL AND") {
		return k.fractalID == "" && k.prismID == "" && k.tenant
	}
	if m := scopePlaceholder.FindStringSubmatch(where); m != nil {
		want := placeholderArg(args, m[2])
		if m[1] == "fractal_id" {
			return k.fractalID == want
		}
		return k.prismID == want
	}
	return true
}

func (db *keyDB) respond(query string, args []driver.Value) (fakedb.PGResult, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	switch {
	case strings.Contains(query, "FROM fractal_permissions"):
		return fakedb.Row(db.fractalRoles[args[0].(string)][args[1].(string)]), true
	case strings.Contains(query, "FROM prism_permissions"):
		return fakedb.Row(db.prismRoles[args[0].(string)][args[1].(string)]), true
	case strings.Contains(query, "SELECT name FROM fractals"):
		if name, ok := db.fractalNames[args[0].(string)]; ok {
			return fakedb.Row(name), true
		}
		return fakedb.PGResult{}, true
	case !strings.Contains(query, "api_keys"):
		return fakedb.PGResult{}, false
	case db.failAll != nil:
		return fakedb.PGResult{Err: db.failAll}, true
	case strings.Contains(query, "INSERT INTO api_keys"):
		db.nextID++
		k := &keyRow{
			id:      "key-" + strconv.Itoa(db.nextID),
			name:    args[0].(string),
			keyID:   args[2].(string),
			keyHash: args[3].(string),
			role:    args[8].(string),
			tenant:  args[9].(bool),
			active:  true,
		}
		k.fractalID, _ = args[4].(string)
		k.prismID, _ = args[5].(string)
		k.createdBy, _ = args[6].(string)
		if t, ok := args[7].(time.Time); ok {
			k.expires = &t
		}
		db.keys[k.id] = k
		return fakedb.Row(k.id), true
	case strings.Contains(query, "ak.key_hash = $1"):
		for _, k := range db.keys {
			if k.keyHash == args[0] && k.active {
				return fakedb.Row(k.values(db.fractalNames, db.prismNames)...), true
			}
		}
		return fakedb.PGResult{}, true
	case strings.HasPrefix(strings.TrimSpace(query), "SELECT"):
		var rows [][]driver.Value
		for _, id := range sortedIDs(db.keys) {
			if k := db.keys[id]; db.inScope(query, args, k) {
				rows = append(rows, k.values(db.fractalNames, db.prismNames))
			}
		}
		return fakedb.RowsOf(rows...), true
	case strings.Contains(query, "DELETE FROM api_keys"):
		for id, k := range db.keys {
			if db.inScope(query, args, k) {
				delete(db.keys, id)
				return fakedb.Affected(1), true
			}
		}
		return fakedb.Affected(0), true
	case strings.Contains(query, "UPDATE api_keys"):
		for _, k := range db.keys {
			if db.inScope(query, args, k) {
				if strings.Contains(query, "is_active = NOT is_active") {
					k.active = !k.active
				}
				return fakedb.Affected(1), true
			}
		}
		return fakedb.Affected(0), true
	}
	return fakedb.PGResult{}, false
}

func sortedIDs(m map[string]*keyRow) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// writes returns the api_keys statements that change data.
func (db *keyDB) writes() []fakedb.Call {
	var out []fakedb.Call
	for _, c := range db.pg.Calls() {
		q := c.Query
		if strings.Contains(q, "INSERT INTO api_keys") || strings.Contains(q, "UPDATE api_keys") || strings.Contains(q, "DELETE FROM api_keys") {
			out = append(out, c)
		}
	}
	return out
}

// newTestHandler wires a Handler, its RBAC resolver and a prism resolver to db.
func newTestHandler(db *keyDB) *Handler {
	h := NewHandler(db.client)
	h.SetRBAC(rbac.NewResolver(db.client))
	h.SetPrismResolver(prismLookup{db})
	return h
}

type prismLookup struct{ db *keyDB }

func (p prismLookup) GetPrismInfo(_ context.Context, id string) (string, string, string, error) {
	if name, ok := p.db.prismNames[id]; ok {
		return id, name, "", nil
	}
	return "", "", "", io.EOF
}

// principal is the identity a request carries, set up the way the auth
// middleware does for a session or an API key.
type principal func(context.Context) context.Context

func sessionUser(u *storage.User) principal {
	return func(ctx context.Context) context.Context {
		ctx = context.WithValue(ctx, "user", u)
		return context.WithValue(ctx, storage.AttributionUserKey, u.Username)
	}
}

// apiKeyPrincipal mirrors AuthMiddleware for a validated key.
func apiKeyPrincipal(k *auth.ValidatedAPIKey) principal {
	return func(ctx context.Context) context.Context {
		u := &storage.User{Username: "apikey_" + k.KeyID, IsAdmin: k.TenantAdmin}
		ctx = context.WithValue(ctx, "user", u)
		ctx = context.WithValue(ctx, "auth_type", "api_key")
		ctx = context.WithValue(ctx, storage.AttributionUserKey, k.CreatedBy)
		ctx = context.WithValue(ctx, "api_key", k)
		switch {
		case k.TenantAdmin:
			ctx = context.WithValue(ctx, "fractal_role", "admin")
			ctx = context.WithValue(ctx, "prism_role", "admin")
		case k.PrismID != "":
			ctx = context.WithValue(ctx, "selected_prism", k.PrismID)
			ctx = context.WithValue(ctx, "prism_role", k.Role)
		default:
			ctx = context.WithValue(ctx, "selected_fractal", k.FractalID)
			ctx = context.WithValue(ctx, "fractal_role", k.Role)
		}
		return ctx
	}
}

func anonymous(ctx context.Context) context.Context { return ctx }

// call runs one handler with chi URL params and a principal.
func call(fn http.HandlerFunc, method, body string, who principal, params ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/", strings.NewReader(body))
	rctx := chi.NewRouteContext()
	for i := 0; i+1 < len(params); i += 2 {
		rctx.URLParams.Add(params[i], params[i+1])
	}
	ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rctx)
	r = r.WithContext(who(ctx))
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
