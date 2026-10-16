package rbac

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

const (
	fOps = "11111111-1111-4111-8111-111111111111"
	fLab = "22222222-2222-4222-8222-222222222222"
	pOps = "33333333-3333-4333-8333-333333333333"
	pLab = "44444444-4444-4444-8444-444444444444"
)

// grant is one permission row: a role on a scope for a user or a group.
type grant struct {
	scope, user, group, role string
}

// grantsDB holds fractal_permissions, prism_permissions and group_members in
// memory. It answers the resolver's statements by reading the scope and user
// through the placeholders each arm of the SQL binds, so a statement that binds
// the username where the scope belongs, or leaves out an arm, resolves the
// wrong grants exactly as Postgres would.
type grantsDB struct {
	t        *testing.T
	fractals []grant
	prisms   []grant
	members  map[string][]string // group -> usernames
	err      error
}

func newGrantsDB(t *testing.T) (*Resolver, *grantsDB, *fakedb.PG) {
	client, pg := storagetest.Postgres()
	db := &grantsDB{t: t, members: map[string][]string{}}
	pg.Respond(db.respond)
	return NewResolver(client), db, pg
}

func bound(q, col string, args []driver.Value) (string, bool) {
	m := regexp.MustCompile(`(?:^|[^\w.])` + regexp.QuoteMeta(col) + `\s*=\s*\$(\d+)`).FindStringSubmatch(q)
	if m == nil {
		return "", false
	}
	i, _ := strconv.Atoi(m[1])
	if i < 1 || i > len(args) {
		return "", false
	}
	s, _ := args[i-1].(string)
	return s, true
}

func weight(role string) int {
	return map[string]int{"viewer": 1, "analyst": 2, "admin": 3}[role]
}

func (db *grantsDB) respond(q string, args []driver.Value) (fakedb.PGResult, bool) {
	if db.err != nil {
		return fakedb.PGResult{Err: db.err}, true
	}
	var rows []grant
	var col, alias string
	switch {
	case strings.Contains(q, "FROM fractal_permissions"):
		rows, col, alias = db.fractals, "fractal_id", "fp"
	case strings.Contains(q, "FROM prism_permissions"):
		rows, col, alias = db.prisms, "prism_id", "pp"
	default:
		db.t.Errorf("unexpected statement: %s", q)
		return fakedb.PGResult{Err: fmt.Errorf("unexpected statement")}, true
	}

	directUser, ok1 := bound(q, "username", args)
	groupUser, ok2 := bound(q, "gm.username", args)
	if !ok1 || !ok2 || !strings.Contains(q, "JOIN group_members gm ON gm.group_id = "+alias+".group_id") {
		db.t.Errorf("statement lacks a direct or a group arm: %s", q)
		return fakedb.PGResult{Err: fmt.Errorf("malformed")}, true
	}
	directScope, scoped := bound(q, col, args)
	groupScope, _ := bound(q, alias+"."+col, args)

	inGroup := map[string]bool{}
	for g, users := range db.members {
		for _, u := range users {
			if u == groupUser {
				inGroup[g] = true
			}
		}
	}
	best := map[string]int{}
	for _, g := range rows {
		if g.user != "" && g.user == directUser && (!scoped || g.scope == directScope) {
			best[g.scope] = max(best[g.scope], weight(g.role))
		}
		if g.group != "" && inGroup[g.group] && (!scoped || g.scope == groupScope) {
			best[g.scope] = max(best[g.scope], weight(g.role))
		}
	}
	names := []string{"", "viewer", "analyst", "admin"}
	if scoped {
		w := 0
		for _, v := range best {
			w = max(w, v)
		}
		return fakedb.Row(names[w]), true
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
	return fakedb.PGResult{Rows: out}, true
}

// The effective role is the strongest of the user's direct grants and their
// groups' grants on that scope, and nothing granted elsewhere or to others.
func TestResolveFractalAndPrismRole(t *testing.T) {
	cases := []struct {
		name   string
		grants []grant
		groups map[string][]string
		want   Role
	}{
		{name: "no grants", want: RoleNone},
		{name: "direct", grants: []grant{{user: "alice", role: "analyst"}}, want: RoleAnalyst},
		{name: "group", grants: []grant{{group: "soc", role: "viewer"}}, groups: map[string][]string{"soc": {"alice"}}, want: RoleViewer},
		{name: "group above direct", grants: []grant{{user: "alice", role: "viewer"}, {group: "soc", role: "admin"}}, groups: map[string][]string{"soc": {"alice"}}, want: RoleAdmin},
		{name: "direct above group", grants: []grant{{user: "alice", role: "admin"}, {group: "soc", role: "viewer"}}, groups: map[string][]string{"soc": {"alice"}}, want: RoleAdmin},
		{name: "strongest of several groups", grants: []grant{{group: "a", role: "viewer"}, {group: "b", role: "analyst"}}, groups: map[string][]string{"a": {"alice"}, "b": {"alice"}}, want: RoleAnalyst},
		{name: "group alice is not in", grants: []grant{{group: "soc", role: "admin"}}, groups: map[string][]string{"soc": {"bob"}}, want: RoleNone},
		{name: "another user's grant", grants: []grant{{user: "bob", role: "admin"}}, want: RoleNone},
		{name: "grant on another scope", grants: []grant{{user: "alice", role: "admin", scope: "other"}}, want: RoleNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, db, _ := newGrantsDB(t)
			for _, g := range tc.grants {
				fg, pg := g, g
				if g.scope == "" {
					fg.scope, pg.scope = fOps, pOps
				} else {
					fg.scope, pg.scope = fLab, pLab
				}
				db.fractals = append(db.fractals, fg)
				db.prisms = append(db.prisms, pg)
			}
			db.members = tc.groups
			if db.members == nil {
				db.members = map[string][]string{}
			}

			got, err := r.ResolveFractalRole(context.Background(), "alice", fOps)
			if err != nil || got != tc.want {
				t.Errorf("ResolveFractalRole = (%q, %v), want %q", got, err, tc.want)
			}
			got, err = r.ResolvePrismRole(context.Background(), "alice", pOps)
			if err != nil || got != tc.want {
				t.Errorf("ResolvePrismRole = (%q, %v), want %q", got, err, tc.want)
			}
		})
	}
}

func TestResolveRoleLookupErrors(t *testing.T) {
	r, db, _ := newGrantsDB(t)
	db.err = errors.New("pq: connection reset")

	if role, err := r.ResolveFractalRole(context.Background(), "alice", fOps); err == nil || role != RoleNone {
		t.Errorf("ResolveFractalRole = (%q, %v), want an error and no role", role, err)
	}
	if role, err := r.ResolvePrismRole(context.Background(), "alice", pOps); err == nil || role != RoleNone {
		t.Errorf("ResolvePrismRole = (%q, %v), want an error and no role", role, err)
	}
	// The convenience forms have no error to return, so they fail closed.
	alice := &storage.User{Username: "alice"}
	if role := r.ResolveRole(context.Background(), alice, fOps); role != RoleNone {
		t.Errorf("ResolveRole on a failing lookup = %q", role)
	}
	if role := r.ResolvePrismRoleWithAdmin(context.Background(), alice, pOps); role != RoleNone {
		t.Errorf("ResolvePrismRoleWithAdmin on a failing lookup = %q", role)
	}
	if got, err := r.GetAccessibleFractals(context.Background(), "alice"); err == nil || got != nil {
		t.Errorf("GetAccessibleFractals = (%v, %v), want an error", got, err)
	}
	if got, err := r.GetAccessiblePrisms(context.Background(), "alice"); err == nil || got != nil {
		t.Errorf("GetAccessiblePrisms = (%v, %v), want an error", got, err)
	}
}

func TestResolveRoleForPeople(t *testing.T) {
	r, db, pg := newGrantsDB(t)
	db.fractals = []grant{{scope: fOps, user: "alice", role: "analyst"}}
	db.prisms = []grant{{scope: pOps, group: "soc", role: "viewer"}}
	db.members["soc"] = []string{"alice"}
	alice := &storage.User{Username: "alice"}

	if got := r.ResolveRole(context.Background(), alice, fOps); got != RoleAnalyst {
		t.Errorf("ResolveRole = %q, want analyst", got)
	}
	if got := r.ResolveRole(context.Background(), alice, fLab); got != RoleNone {
		t.Errorf("ResolveRole on an ungranted fractal = %q", got)
	}
	if got := r.ResolvePrismRoleWithAdmin(context.Background(), alice, pOps); got != RoleViewer {
		t.Errorf("ResolvePrismRoleWithAdmin = %q, want viewer", got)
	}

	// Tenant admins and empty scopes are decided without a lookup.
	before := len(pg.Calls())
	root := &storage.User{Username: "root", IsAdmin: true}
	if r.ResolveRole(context.Background(), root, fLab) != RoleAdmin || r.ResolvePrismRoleWithAdmin(context.Background(), root, pLab) != RoleAdmin {
		t.Error("tenant admin is not admin everywhere")
	}
	if r.ResolveRole(context.Background(), alice, "") != RoleNone || r.ResolvePrismRoleWithAdmin(context.Background(), alice, "") != RoleNone {
		t.Error("a role resolved for no scope")
	}
	if n := len(pg.Calls()) - before; n != 0 {
		t.Errorf("%d lookups for decisions that need none", n)
	}
}

// A key's own grant lives in the request context and is honoured only on the
// scope it was issued for. Anywhere else it is looked up as its synthetic
// principal, which owns no grants, and is denied. A session user cannot borrow
// the mechanism by putting a role in context.
func TestResolveRoleForAPIKeys(t *testing.T) {
	r, db, pg := newGrantsDB(t)
	db.fractals = []grant{{scope: fLab, user: "alice", role: "admin"}}
	db.prisms = []grant{{scope: pLab, user: "alice", role: "admin"}}
	key := &storage.User{Username: "apikey_k1"}

	ctx := context.WithValue(context.Background(), "auth_type", "api_key")
	ctx = context.WithValue(ctx, "selected_fractal", fOps)
	ctx = context.WithValue(ctx, "fractal_role", "analyst")
	ctx = context.WithValue(ctx, "selected_prism", pOps)
	ctx = context.WithValue(ctx, "prism_role", "viewer")

	before := len(pg.Calls())
	if got := r.ResolveRole(ctx, key, fOps); got != RoleAnalyst {
		t.Errorf("key on its fractal = %q, want analyst", got)
	}
	if got := r.ResolvePrismRoleWithAdmin(ctx, key, pOps); got != RoleViewer {
		t.Errorf("key on its prism = %q, want viewer", got)
	}
	if n := len(pg.Calls()) - before; n != 0 {
		t.Errorf("%d lookups for a key on its own scope", n)
	}

	if got := r.ResolveRole(ctx, key, fLab); got != RoleNone {
		t.Errorf("key on another fractal = %q", got)
	}
	if got := r.ResolvePrismRoleWithAdmin(ctx, key, pLab); got != RoleNone {
		t.Errorf("key on another prism = %q", got)
	}
	for _, c := range pg.Calls()[before:] {
		if !argsInclude(c.Args, "apikey_k1") {
			t.Errorf("fallback lookup not made as the key principal: %v", c.Args)
		}
	}

	// A session user's context role is not a grant.
	sess := context.WithValue(ctx, "auth_type", "session")
	if got := r.ResolveRole(sess, &storage.User{Username: "mallory"}, fOps); got != RoleNone {
		t.Errorf("session user with a role in context = %q", got)
	}
	if got := r.ResolvePrismRoleWithAdmin(sess, &storage.User{Username: "mallory"}, pOps); got != RoleNone {
		t.Errorf("session user with a prism role in context = %q", got)
	}
}

func argsInclude(args []driver.Value, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func TestGetAccessibleFractalsAndPrisms(t *testing.T) {
	r, db, _ := newGrantsDB(t)
	db.fractals = []grant{
		{scope: fOps, user: "alice", role: "viewer"},
		{scope: fOps, group: "soc", role: "analyst"},
		{scope: fLab, group: "soc", role: "viewer"},
		{scope: "99999999-9999-4999-8999-999999999999", user: "bob", role: "admin"},
		{scope: "88888888-8888-4888-8888-888888888888", group: "red", role: "admin"},
	}
	db.prisms = []grant{
		{scope: pOps, user: "alice", role: "admin"},
		{scope: pOps, group: "soc", role: "viewer"},
		{scope: pLab, user: "bob", role: "viewer"},
	}
	db.members = map[string][]string{"soc": {"alice", "bob"}, "red": {"bob"}}

	fr, err := r.GetAccessibleFractals(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	wantF := []FractalAccess{{FractalID: fOps, Role: RoleAnalyst}, {FractalID: fLab, Role: RoleViewer}}
	if fmt.Sprint(fr) != fmt.Sprint(wantF) {
		t.Errorf("fractals = %v, want %v", fr, wantF)
	}

	pr, err := r.GetAccessiblePrisms(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	wantP := []PrismAccess{{PrismID: pOps, Role: RoleAdmin}}
	if fmt.Sprint(pr) != fmt.Sprint(wantP) {
		t.Errorf("prisms = %v, want %v", pr, wantP)
	}

	if fr, err := r.GetAccessibleFractals(context.Background(), "nobody"); err != nil || len(fr) != 0 {
		t.Errorf("no grants: (%v, %v)", fr, err)
	}
}

// A row the driver cannot scan is an error, not a silently shorter list.
func TestGetAccessibleScanError(t *testing.T) {
	client, pg := storagetest.Postgres()
	r := NewResolver(client)
	pg.On("GROUP BY fractal_id", fakedb.RowsOf([]driver.Value{fOps, "viewer"}, []driver.Value{fLab, nil}))
	pg.On("GROUP BY prism_id", fakedb.RowsOf([]driver.Value{pOps, nil}))

	if got, err := r.GetAccessibleFractals(context.Background(), "alice"); err == nil {
		t.Errorf("GetAccessibleFractals = %v, want a scan error", got)
	}
	if got, err := r.GetAccessiblePrisms(context.Background(), "alice"); err == nil {
		t.Errorf("GetAccessiblePrisms = %v, want a scan error", got)
	}
}
