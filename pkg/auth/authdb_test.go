package auth

import (
	"database/sql/driver"
	"fmt"
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
	"bifract/pkg/fractals"
	"bifract/pkg/settings"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"

	"golang.org/x/crypto/bcrypt"
)

const (
	systemFractalID  = "00000000-0000-4000-8000-000000000001"
	defaultFractalID = "00000000-0000-4000-8000-000000000002"
	opsFractalID     = "00000000-0000-4000-8000-000000000003"
	labFractalID     = "00000000-0000-4000-8000-000000000004"
	opsPrismID       = "00000000-0000-4000-8000-0000000000a1"

	userPassword = "correct horse battery staple"
)

// row is one table row, keyed by column name.
type row map[string]any

// authDB is an in-memory Postgres behind the scripted driver, holding the tables
// the auth handlers touch. Writes are interpreted from the statement itself: SET
// and WHERE clauses are read column by column and bound through the placeholders
// they name, so a statement that binds a value to the wrong $n, or filters on
// the wrong column, changes the wrong row or none, as it would in Postgres. An
// unrecognized statement fails the test rather than answering empty.
type authDB struct {
	t      *testing.T
	pg     *fakedb.PG
	client *storage.PostgresClient

	mu        sync.Mutex
	tables    map[string][]row
	fail      map[string]error
	unhandled []string
}

func newAuthDB(t *testing.T) *authDB {
	t.Helper()
	client, pg := storagetest.Postgres()
	db := &authDB{t: t, pg: pg, client: client, tables: map[string][]row{}, fail: map[string]error{}}
	now := time.Now()
	for _, f := range []row{
		{"id": systemFractalID, "name": "system", "is_default": false, "is_system": true},
		{"id": defaultFractalID, "name": "default", "is_default": true, "is_system": false},
		{"id": opsFractalID, "name": "ops", "is_default": false, "is_system": false},
		{"id": labFractalID, "name": "lab", "is_default": false, "is_system": false},
	} {
		f["created_at"], f["updated_at"] = now, now
		db.tables["fractals"] = append(db.tables["fractals"], f)
	}
	db.tables["prisms"] = []row{{"id": opsPrismID}}
	pg.Respond(db.respond)
	t.Cleanup(func() {
		db.mu.Lock()
		defer db.mu.Unlock()
		for _, q := range db.unhandled {
			t.Errorf("authDB: unhandled statement: %s", q)
		}
	})
	return db
}

// newDBHandler builds the handler the way the server does, over db.
func newDBHandler(t *testing.T) (*AuthHandler, *authDB) {
	t.Helper()
	db := newAuthDB(t)
	return newHandlerOver(db), db
}

func newHandlerOver(db *authDB) *AuthHandler {
	return NewAuthHandlerWithAPIKeys(db.client, nil, fractals.NewManager(db.client, nil), nil)
}

// failOn fails every statement containing sub.
func (db *authDB) failOn(sub string, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.fail[sub] = err
}

func (db *authDB) insert(table string, r row) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.tables[table] = append(db.tables[table], r)
}

// rows returns copies of the rows of table matching every key/value in where.
func (db *authDB) rows(table string, where row) []row {
	db.mu.Lock()
	defer db.mu.Unlock()
	var out []row
	for _, r := range db.tables[table] {
		ok := true
		for k, v := range where {
			if r[k] != v {
				ok = false
				break
			}
		}
		if ok {
			c := row{}
			for k, v := range r {
				c[k] = v
			}
			out = append(out, c)
		}
	}
	return out
}

func (db *authDB) user(name string) row {
	db.t.Helper()
	rs := db.rows("users", row{"username": name})
	if len(rs) != 1 {
		db.t.Fatalf("user %q: %d rows", name, len(rs))
	}
	return rs[0]
}

func (db *authDB) sessionsOf(name string) []row { return db.rows("sessions", row{"username": name}) }

// addUser inserts a local, enabled user whose password is userPassword under
// the pepper in effect now. mods adjust the row.
func (db *authDB) addUser(name string, mods ...func(row)) row {
	db.t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(pepperPassword(userPassword)), bcrypt.MinCost)
	if err != nil {
		db.t.Fatal(err)
	}
	r := row{
		"username": name, "password_hash": string(hash), "display_name": strings.ToUpper(name),
		"gravatar_color": "#123456", "gravatar_initial": strings.ToUpper(name[:1]),
		"created_at": time.Now().Add(-time.Duration(len(db.rows("users", nil))) * time.Hour),
		"last_login": nil, "is_admin": false, "auth_provider": "local",
		"force_password_change": false, "enabled": true, "display_timezone": "UTC",
		"totp_secret": nil, "totp_enrolled_at": nil, "totp_last_counter": int64(0),
		"invite_token_hash": nil, "invite_expires_at": nil,
	}
	for _, m := range mods {
		m(r)
	}
	db.insert("users", r)
	return r
}

func asAdmin(r row)    { r["is_admin"] = true }
func disabled(r row)   { r["enabled"] = false }
func mustChange(r row) { r["force_password_change"] = true }
func viaSSO(r row)     { r["auth_provider"] = "oidc"; r["password_hash"] = nil }

// grantFractal adds a fractal_permissions row for a user, or for a group when
// the grantee starts with "group:".
func (db *authDB) grantFractal(fractalID, grantee, role string) {
	db.grant("fractal_permissions", "fractal_id", fractalID, grantee, role)
}

func (db *authDB) grantPrism(prismID, grantee, role string) {
	db.grant("prism_permissions", "prism_id", prismID, grantee, role)
}

func (db *authDB) grant(table, col, id, grantee, role string) {
	r := row{col: id, "role": role, "username": nil, "group_id": nil}
	if g, ok := strings.CutPrefix(grantee, "group:"); ok {
		r["group_id"] = g
	} else {
		r["username"] = grantee
	}
	db.insert(table, r)
}

func (db *authDB) joinGroup(group, username string) {
	db.insert("group_members", row{"group_id": group, "username": username})
}

var (
	wsRe     = regexp.MustCompile(`\s+`)
	updateRe = regexp.MustCompile(`^UPDATE (\w+) SET (.+?) WHERE (.+)$`)
	deleteRe = regexp.MustCompile(`^DELETE FROM (\w+)(?: WHERE (.+))?$`)
	insertRe = regexp.MustCompile(`^INSERT INTO (\w+) \(([^)]*)\) VALUES \(([^)]*)\)(?: ON CONFLICT \((\w+)\) DO UPDATE SET (.+))?$`)
	whereRe  = regexp.MustCompile(` WHERE (.+?)(?: ORDER BY .*| LIMIT \d+)?$`)

	condEqArg  = regexp.MustCompile(`^([\w.]+) = \$(\d+)$`)
	condEqLit  = regexp.MustCompile(`^([\w.]+) = '([^']*)'$`)
	condEqBool = regexp.MustCompile(`^([\w.]+) = (?i)(true|false)$`)
	condNow    = regexp.MustCompile(`^([\w.]+) ([<>]) NOW\(\)$`)
	condLtArg  = regexp.MustCompile(`^([\w.]+) < \$(\d+)$`)
	condNull   = regexp.MustCompile(`^([\w.]+) IS (NOT )?NULL$`)
	condNeCoal = regexp.MustCompile(`^COALESCE\((\w+), '([^']*)'\) != '([^']*)'$`)
)

func normalize(q string) string { return strings.TrimSpace(wsRe.ReplaceAllString(q, " ")) }

// column drops a table alias: s.session_id -> session_id.
func column(c string) string {
	if i := strings.LastIndexByte(c, '.'); i >= 0 {
		return c[i+1:]
	}
	return c
}

func argN(args []driver.Value, n string) (any, error) {
	i, _ := strconv.Atoi(n)
	if i < 1 || i > len(args) {
		return nil, fmt.Errorf("placeholder $%s out of range (%d args)", n, len(args))
	}
	return args[i-1], nil
}

// boundArg returns the argument bound to "col = $n" in q, where col is not
// itself qualified by another alias (gm.username does not match username).
func boundArg(q, col string, args []driver.Value) (any, bool) {
	re := regexp.MustCompile(`(?:^|[^\w.])` + regexp.QuoteMeta(col) + ` = \$(\d+)`)
	m := re.FindStringSubmatch(q)
	if m == nil {
		return nil, false
	}
	v, err := argN(args, m[1])
	return v, err == nil
}

func timeOf(v any) (time.Time, bool) {
	t, ok := v.(time.Time)
	return t, ok
}

// matches evaluates a WHERE clause of AND-ed simple conditions against r.
func matches(r row, where string, args []driver.Value) (bool, error) {
	if strings.TrimSpace(where) == "" {
		return true, nil
	}
	for _, cond := range strings.Split(where, " AND ") {
		cond = strings.TrimSpace(cond)
		var ok bool
		switch {
		case condEqArg.MatchString(cond):
			m := condEqArg.FindStringSubmatch(cond)
			v, err := argN(args, m[2])
			if err != nil {
				return false, err
			}
			ok = r[column(m[1])] != nil && r[column(m[1])] == v
		case condEqLit.MatchString(cond):
			m := condEqLit.FindStringSubmatch(cond)
			ok = r[column(m[1])] == m[2]
		case condEqBool.MatchString(cond):
			m := condEqBool.FindStringSubmatch(cond)
			ok = r[column(m[1])] == strings.EqualFold(m[2], "true")
		case condNow.MatchString(cond):
			m := condNow.FindStringSubmatch(cond)
			tv, has := timeOf(r[column(m[1])])
			if m[2] == ">" {
				ok = has && tv.After(time.Now())
			} else {
				ok = has && tv.Before(time.Now())
			}
		case condLtArg.MatchString(cond):
			m := condLtArg.FindStringSubmatch(cond)
			v, err := argN(args, m[2])
			if err != nil {
				return false, err
			}
			have, _ := r[column(m[1])].(int64)
			want, _ := v.(int64)
			ok = have < want
		case condNull.MatchString(cond):
			m := condNull.FindStringSubmatch(cond)
			ok = (r[column(m[1])] == nil) == (m[2] == "")
		case condNeCoal.MatchString(cond):
			m := condNeCoal.FindStringSubmatch(cond)
			v := r[m[1]]
			if v == nil {
				v = m[2]
			}
			ok = v != m[3]
		default:
			return false, fmt.Errorf("unsupported condition %q", cond)
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// value evaluates the right-hand side of an assignment or a VALUES entry.
func value(expr string, args []driver.Value) (any, error) {
	expr = strings.TrimSpace(expr)
	switch {
	case strings.HasPrefix(expr, "$"):
		return argN(args, expr[1:])
	case strings.EqualFold(expr, "NULL"):
		return nil, nil
	case expr == "NOW()":
		return time.Now(), nil
	case strings.EqualFold(expr, "TRUE"), strings.EqualFold(expr, "FALSE"):
		return strings.EqualFold(expr, "TRUE"), nil
	case strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'"):
		return expr[1 : len(expr)-1], nil
	}
	if n, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return n, nil
	}
	return nil, fmt.Errorf("unsupported expression %q", expr)
}

func (db *authDB) respond(query string, args []driver.Value) (fakedb.PGResult, bool) {
	q := normalize(query)
	db.mu.Lock()
	defer db.mu.Unlock()
	for sub, err := range db.fail {
		if strings.Contains(q, sub) {
			return fakedb.PGResult{Err: err}, true
		}
	}
	res, err := db.exec(q, args)
	if err != nil {
		db.unhandled = append(db.unhandled, fmt.Sprintf("%s (%v)", q, err))
		return fakedb.PGResult{Err: err}, true
	}
	return res, true
}

func (db *authDB) exec(q string, args []driver.Value) (fakedb.PGResult, error) {
	switch {
	case strings.HasPrefix(q, "UPDATE "):
		return db.update(q, args)
	case strings.HasPrefix(q, "DELETE "):
		return db.delete(q, args)
	case strings.HasPrefix(q, "INSERT "):
		return db.insertStmt(q, args)
	case strings.Contains(q, "FROM fractal_permissions"):
		return db.permissions(q, args, "fractal_permissions", "fractal_id", "fp")
	case strings.Contains(q, "FROM prism_permissions"):
		return db.permissions(q, args, "prism_permissions", "prism_id", "pp")
	case strings.Contains(q, "FROM sessions s"):
		return db.selectSession(q, args)
	}
	table := ""
	for _, name := range []string{"users", "user_recovery_codes", "settings", "fractals"} {
		if strings.Contains(q, " FROM "+name) {
			table = name
		}
	}
	if table == "" {
		return fakedb.PGResult{}, fmt.Errorf("unknown statement")
	}
	where := ""
	if m := whereRe.FindStringSubmatch(q); m != nil {
		where = m[1]
	}
	var hits []row
	for _, r := range db.tables[table] {
		ok, err := matches(r, where, args)
		if err != nil {
			return fakedb.PGResult{}, err
		}
		if ok {
			hits = append(hits, r)
		}
	}
	return project(q, table, hits)
}

func coalesce(v, def any) any {
	if v == nil {
		return def
	}
	return v
}

// project renders the selected rows the way each known SELECT lists its columns.
func project(q, table string, hits []row) (fakedb.PGResult, error) {
	var out [][]driver.Value
	switch {
	case table == "users" && strings.Contains(q, "COALESCE(totp_last_counter, 0)"): // GetUser
		for _, r := range hits {
			out = append(out, []driver.Value{
				r["username"], r["password_hash"], r["display_name"], r["gravatar_color"], r["gravatar_initial"],
				r["created_at"], r["last_login"], r["is_admin"], coalesce(r["auth_provider"], "local"),
				coalesce(r["force_password_change"], false), coalesce(r["enabled"], true),
				coalesce(r["display_timezone"], "UTC"), coalesce(r["totp_secret"], ""),
				r["totp_enrolled_at"] != nil, coalesce(r["totp_last_counter"], int64(0)),
			})
		}
	case table == "users" && strings.HasPrefix(q, "SELECT password_hash FROM users"):
		for _, r := range hits {
			out = append(out, []driver.Value{r["password_hash"]})
		}
	case table == "users" && strings.Contains(q, "AS invite_pending"): // ListUsers
		sort.SliceStable(hits, func(i, j int) bool {
			return hits[i]["created_at"].(time.Time).After(hits[j]["created_at"].(time.Time))
		})
		for _, r := range hits {
			out = append(out, []driver.Value{
				r["username"], r["display_name"], r["gravatar_color"], r["gravatar_initial"],
				r["created_at"], r["last_login"], r["is_admin"], r["invite_token_hash"] != nil,
				coalesce(r["auth_provider"], "local"), coalesce(r["enabled"], true), r["totp_enrolled_at"] != nil,
			})
		}
	case table == "users" && strings.HasPrefix(q, "SELECT username, display_name, gravatar_color, gravatar_initial, created_at, is_admin FROM users"):
		for _, r := range hits {
			out = append(out, []driver.Value{r["username"], r["display_name"], r["gravatar_color"], r["gravatar_initial"], r["created_at"], r["is_admin"]})
		}
	case table == "user_recovery_codes" && strings.HasPrefix(q, "SELECT COUNT(*)"):
		out = append(out, []driver.Value{int64(len(hits))})
	case table == "settings" && strings.HasPrefix(q, "SELECT value FROM settings"):
		for _, r := range hits {
			out = append(out, []driver.Value{r["value"]})
		}
	case table == "fractals" && strings.Contains(q, "COALESCE(disk_quota_action, 'reject')"):
		for _, r := range hits {
			out = append(out, []driver.Value{
				r["id"], r["name"], "", r["is_default"], r["is_system"], "", r["created_at"], r["updated_at"],
				nil, nil, nil, "reject", int64(0), int64(0), nil, nil, nil, nil,
			})
		}
	default:
		return fakedb.PGResult{}, fmt.Errorf("unknown projection")
	}
	return fakedb.PGResult{Rows: out}, nil
}

func (db *authDB) update(q string, args []driver.Value) (fakedb.PGResult, error) {
	m := updateRe.FindStringSubmatch(q)
	if m == nil {
		return fakedb.PGResult{}, fmt.Errorf("unparsed UPDATE")
	}
	sets := map[string]any{}
	for _, a := range strings.Split(m[2], ",") {
		col, expr, ok := strings.Cut(a, "=")
		if !ok {
			return fakedb.PGResult{}, fmt.Errorf("unparsed assignment %q", a)
		}
		v, err := value(expr, args)
		if err != nil {
			return fakedb.PGResult{}, err
		}
		sets[strings.TrimSpace(col)] = v
	}
	var n int64
	for _, r := range db.tables[m[1]] {
		ok, err := matches(r, m[3], args)
		if err != nil {
			return fakedb.PGResult{}, err
		}
		if !ok {
			continue
		}
		for c, v := range sets {
			r[c] = v
		}
		n++
	}
	return fakedb.Affected(n), nil
}

func (db *authDB) delete(q string, args []driver.Value) (fakedb.PGResult, error) {
	m := deleteRe.FindStringSubmatch(q)
	if m == nil {
		return fakedb.PGResult{}, fmt.Errorf("unparsed DELETE")
	}
	var keep []row
	var n int64
	for _, r := range db.tables[m[1]] {
		ok, err := matches(r, m[2], args)
		if err != nil {
			return fakedb.PGResult{}, err
		}
		if ok {
			n++
		} else {
			keep = append(keep, r)
		}
	}
	db.tables[m[1]] = keep
	return fakedb.Affected(n), nil
}

func (db *authDB) insertStmt(q string, args []driver.Value) (fakedb.PGResult, error) {
	m := insertRe.FindStringSubmatch(q)
	if m == nil {
		return fakedb.PGResult{}, fmt.Errorf("unparsed INSERT")
	}
	cols := strings.Split(m[2], ",")
	vals := strings.Split(m[3], ",")
	if len(cols) != len(vals) {
		return fakedb.PGResult{}, fmt.Errorf("%d columns, %d values", len(cols), len(vals))
	}
	r := row{}
	for i, c := range cols {
		v, err := value(vals[i], args)
		if err != nil {
			return fakedb.PGResult{}, err
		}
		r[strings.TrimSpace(c)] = v
	}
	if m[1] == "users" {
		// Column defaults, plus what the set_user_gravatar trigger fills in.
		name, _ := r["username"].(string)
		defaults := row{
			"created_at": time.Now(), "auth_provider": "local", "enabled": true, "force_password_change": false,
			"display_timezone": "UTC", "totp_last_counter": int64(0),
			"gravatar_color": "#abcdef", "gravatar_initial": strings.ToUpper(name[:min(1, len(name))]),
		}
		for k, v := range defaults {
			if _, ok := r[k]; !ok {
				r[k] = v
			}
		}
	}
	if key := m[4]; key != "" {
		for _, existing := range db.tables[m[1]] {
			if existing[key] == r[key] {
				for _, a := range strings.Split(m[5], ",") {
					col, expr, _ := strings.Cut(a, "=")
					col, expr = strings.TrimSpace(col), strings.TrimSpace(expr)
					if expr != "EXCLUDED."+col {
						return fakedb.PGResult{}, fmt.Errorf("unsupported upsert assignment %q", a)
					}
					existing[col] = r[col]
				}
				return fakedb.Affected(1), nil
			}
		}
	}
	db.tables[m[1]] = append(db.tables[m[1]], r)
	return fakedb.Affected(1), nil
}

// selectSession answers pgSessionStore.Get, resolving a selected scope that no
// longer exists to NULL as its LEFT JOINs do.
func (db *authDB) selectSession(q string, args []driver.Value) (fakedb.PGResult, error) {
	m := whereRe.FindStringSubmatch(q)
	if m == nil {
		return fakedb.PGResult{}, fmt.Errorf("session lookup without WHERE")
	}
	exists := func(table string, id any) bool {
		for _, r := range db.tables[table] {
			if id != nil && r["id"] == id {
				return true
			}
		}
		return false
	}
	var out [][]driver.Value
	for _, r := range db.tables["sessions"] {
		ok, err := matches(r, m[1], args)
		if err != nil {
			return fakedb.PGResult{}, err
		}
		if !ok {
			continue
		}
		var fractal, prism driver.Value
		if exists("fractals", r["selected_fractal"]) {
			fractal = r["selected_fractal"]
		}
		if exists("prisms", r["selected_prism"]) {
			prism = r["selected_prism"]
		}
		out = append(out, []driver.Value{r["username"], r["created_at"], r["expires_at"], fractal, prism, coalesce(r["mfa_pending"], false)})
	}
	return fakedb.PGResult{Rows: out}, nil
}

func roleWeight(role any) int {
	switch role {
	case "admin":
		return 3
	case "analyst":
		return 2
	case "viewer":
		return 1
	}
	return 0
}

// permissions answers the rbac resolver: the strongest of the user's direct
// grants and the grants of the groups they belong to, read through whichever
// placeholders each arm of the statement binds.
func (db *authDB) permissions(q string, args []driver.Value, table, col, alias string) (fakedb.PGResult, error) {
	directUser, ok1 := boundArg(q, "username", args)
	groupUser, ok2 := boundArg(q, "gm.username", args)
	if !ok1 || !ok2 {
		return fakedb.PGResult{}, fmt.Errorf("permission lookup missing a username arm")
	}
	directScope, scoped := boundArg(q, col, args)
	groupScope, _ := boundArg(q, alias+"."+col, args)

	groups := map[any]bool{}
	for _, gm := range db.tables["group_members"] {
		if gm["username"] == groupUser {
			groups[gm["group_id"]] = true
		}
	}
	best := map[string]int{}
	for _, p := range db.tables[table] {
		id := p[col].(string)
		if p["username"] != nil && p["username"] == directUser && (!scoped || p[col] == directScope) {
			best[id] = max(best[id], roleWeight(p["role"]))
		}
		if p["group_id"] != nil && groups[p["group_id"]] && (!scoped || p[col] == groupScope) {
			best[id] = max(best[id], roleWeight(p["role"]))
		}
	}
	names := []string{"", "viewer", "analyst", "admin"}
	if scoped {
		// Single-scope lookup: COALESCE(... LIMIT 1, '').
		w := 0
		for _, v := range best {
			w = max(w, v)
		}
		return fakedb.Row(names[w]), nil
	}
	ids := make([]string, 0, len(best))
	for id := range best {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var out [][]driver.Value
	for _, id := range ids {
		out = append(out, []driver.Value{id, names[best[id]]})
	}
	return fakedb.PGResult{Rows: out}, nil
}

// setRequireMFA loads settings with require_mfa set, through settings.Init over
// a scripted settings table, and restores the defaults afterwards.
func setRequireMFA(t *testing.T, on bool) {
	t.Helper()
	load := func(values map[string]string) {
		client, pg := storagetest.Postgres()
		pg.Respond(func(q string, args []driver.Value) (fakedb.PGResult, bool) {
			if !strings.Contains(q, "FROM settings") || len(args) != 1 {
				return fakedb.PGResult{}, false
			}
			if v, ok := values[args[0].(string)]; ok {
				return fakedb.Row(v), true
			}
			return fakedb.PGResult{}, true
		})
		if err := settings.Init(client); err != nil {
			t.Fatal(err)
		}
	}
	load(map[string]string{"require_mfa": strconv.FormatBool(on)})
	if settings.Get().RequireMFA != on {
		t.Fatalf("settings did not load require_mfa=%v", on)
	}
	t.Cleanup(func() { load(nil) })
}

// sessionCookie returns the session cookie a response set, or nil.
func sessionCookie(rec *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			return c
		}
	}
	return nil
}

// login posts credentials and returns the response.
func login(h *AuthHandler, username, password, ip string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login",
		strings.NewReader(fmt.Sprintf(`{"username":%q,"password":%q}`, username, password)))
	r.RemoteAddr = ip + ":40000"
	rec := httptest.NewRecorder()
	h.HandleLogin(rec, r)
	return rec
}

// throughMiddleware sends a request carrying sessionID through AuthMiddleware.
func throughMiddleware(h *AuthHandler, method, path, sessionID string, headers map[string]string) (*httptest.ResponseRecorder, *ctxCapture) {
	next := &ctxCapture{}
	r := httptest.NewRequest(method, path, nil)
	if sessionID != "" {
		r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.AuthMiddleware(next).ServeHTTP(rec, r)
	return rec, next
}

// newSession stores a session for username directly, bypassing login.
func (db *authDB) newSession(id, username string, mods ...func(row)) string {
	r := row{
		"session_id": id, "username": username, "created_at": time.Now(),
		"expires_at": time.Now().Add(time.Hour), "selected_fractal": nil, "selected_prism": nil, "mfa_pending": false,
	}
	for _, m := range mods {
		m(r)
	}
	db.insert("sessions", r)
	return id
}

func onFractal(id string) func(row) { return func(r row) { r["selected_fractal"] = id } }
func onPrism(id string) func(row) {
	return func(r row) { r["selected_fractal"] = nil; r["selected_prism"] = id }
}
func mfaPending(r row) { r["mfa_pending"] = true }
