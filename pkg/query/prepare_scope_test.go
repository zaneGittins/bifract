package query

import (
	"database/sql/driver"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// prepareQuery's scope-dependent resolution, against Postgres-backed managers:
// which fractals a search covers, which dictionaries, models and commented logs
// it resolves, and whether legacy rows with no fractal_id are included. Each is
// resolved for the scope the session selected, prism before fractal.

func scopeEnv(t *testing.T) *testEnv {
	e := newTestEnv(t, 1000)
	e.set(func(e *testEnv) {
		e.members["p1"] = []string{"f1", "f2"}
		e.members["p2"] = []string{"f1", "fdef"}
	})
	return e
}

func sessionOn(k, v string) ctxVals {
	return ctxVals{"user": testAdmin, "auth_type": "session", k: v}
}

// Rows stored before fractal_id existed carry ” and belong to the default
// fractal, so they are searched exactly when the default fractal is in scope.
func TestPrepareQueryLegacyRowsFollowTheDefaultFractal(t *testing.T) {
	cases := []struct {
		name      string
		vals      ctxVals
		reqFrac   string
		want      bool
		wantScope []string
	}{
		{name: "default fractal", vals: sessionOn("selected_fractal", "fdef"), want: true},
		{name: "default fractal by request", vals: adminCtx(), reqFrac: "fdef", want: true},
		{name: "another fractal", vals: sessionOn("selected_fractal", "f1"), want: false},
		{name: "prism holding the default", vals: sessionOn("selected_prism", "p2"), want: true, wantScope: []string{"f1", "fdef"}},
		{name: "prism without the default", vals: sessionOn("selected_prism", "p1"), want: false, wantScope: []string{"f1", "f2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := scopeEnv(t)
			prep := prepare(t, e.h, purposeSearch, searchBody(t, QueryRequest{Query: "level=error", FractalID: tc.reqFrac}), tc.vals)
			opts := prep.translationOpts
			if opts.IncludeEmptyFractalID != tc.want {
				t.Errorf("IncludeEmptyFractalID = %v, want %v", opts.IncludeEmptyFractalID, tc.want)
			}
			if tc.wantScope != nil {
				if !reflect.DeepEqual(opts.FractalIDs, tc.wantScope) || opts.FractalID != "" || prep.prismID == "" {
					t.Errorf("prism scope = %v / %q / prism %q", opts.FractalIDs, opts.FractalID, prep.prismID)
				}
			}
			if hasEmpty := strings.Contains(prep.sql, ", '')"); hasEmpty != tc.want {
				t.Errorf("SQL legacy predicate present=%v: %s", hasEmpty, prep.sql)
			}
		})
	}
}

// pgCallArgs returns the arguments of the Postgres calls whose text contains sub.
func pgCallArgs(e *testEnv, sub string) [][]driver.Value {
	var out [][]driver.Value
	for _, c := range e.pg.Matching(sub) {
		out = append(out, c.Args)
	}
	return out
}

// match() resolves dictionaries owned by the selected scope: the prism's own when
// a prism is selected, the fractal's otherwise.
func TestPrepareQueryResolvesDictionariesForScope(t *testing.T) {
	const q = `* | match(dict="users", field=user, column=name, include=[tier])`
	for _, tc := range []struct {
		name    string
		vals    ctxVals
		wantArg string
	}{
		{"fractal", sessionOn("selected_fractal", "f1"), "f1"},
		{"prism", sessionOn("selected_prism", "p1"), "p1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := scopeEnv(t)
			e.set(func(e *testEnv) { e.dictRows = [][]driver.Value{{"d1", "users", "name"}} })
			prep := prepare(t, e.h, purposeSearch, searchBody(t, QueryRequest{Query: q}), tc.vals)
			calls := pgCallArgs(e, "FROM dictionaries")
			if len(calls) != 1 || calls[0][0] != tc.wantArg {
				t.Fatalf("dictionary reads = %v, want one for %s", calls, tc.wantArg)
			}
			if _, ok := prep.translationOpts.Dictionaries["users"]; !ok {
				t.Errorf("dictionaries = %v", prep.translationOpts.Dictionaries)
			}
			// Dictionary objects are database-qualified, or a shard running the remote
			// half of the query resolves the name in its own database.
			if !strings.Contains(prep.sql, "dictGetOrDefault('logs.lookup_d1'") {
				t.Errorf("unqualified dictionary in %s", prep.sql)
			}
		})
	}
}

// model_lookup() resolves against the querying fractal's models, or in a prism
// against every member's (models have no prism of their own), keeping the owner.
func TestPrepareQueryResolvesModelsForScope(t *testing.T) {
	const q = `* | model_lookup(model="beacons", key=[src_ip])`
	model := []driver.Value{"m1", "beacons", "first_seen", []byte(`{"min_sample":5}`), "f2"}
	for _, tc := range []struct {
		name      string
		vals      ctxVals
		wantQuery string
		wantOwner string
	}{
		{"fractal", sessionOn("selected_fractal", "f1"), "WHERE fractal_id = $1", "f1"},
		{"prism", sessionOn("selected_prism", "p1"), "fractal_id = ANY($1)", "f2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := scopeEnv(t)
			e.set(func(e *testEnv) { e.modelRows = [][]driver.Value{model} })
			prep := prepare(t, e.h, purposeSearch, searchBody(t, QueryRequest{Query: q}), tc.vals)
			if calls := e.pg.Matching("FROM analytics_models"); len(calls) != 1 || !strings.Contains(calls[0].Query, tc.wantQuery) {
				t.Fatalf("model reads = %+v", calls)
			}
			mi, ok := prep.translationOpts.Models["beacons"]
			if !ok || mi.ID != "m1" || mi.FractalID != tc.wantOwner || mi.ModelType != "first_seen" || mi.MinSample != 5 {
				t.Errorf("model = %+v", mi)
			}
			if !strings.Contains(prep.sql, mi.TableName) {
				t.Errorf("model table %q not joined: %s", mi.TableName, prep.sql)
			}
		})
	}
}

// comment() runs as an IN filter over the log ids whose comments match, read from
// Postgres for the selected scope and the search's own window.
func TestPrepareQueryResolvesComments(t *testing.T) {
	const q = `comment(tags=[triage], keyword="beacon")`
	for _, tc := range []struct {
		name    string
		vals    ctxVals
		wantArg string
	}{
		{"fractal", sessionOn("selected_fractal", "f1"), "f1"},
		{"prism", sessionOn("selected_prism", "p1"), "p1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := scopeEnv(t)
			e.set(func(e *testEnv) { e.commentIDs = []string{"L7", "L9"} })
			prep := prepare(t, e.h, purposeSearch, searchBody(t, QueryRequest{Query: q}), tc.vals)
			calls := e.pg.Matching("FROM comments")
			if len(calls) != 1 {
				t.Fatalf("comment reads = %+v", calls)
			}
			args := calls[0].Args
			if args[2] != tc.wantArg || !strings.Contains(calls[0].Query, "text ILIKE") || args[4] != "%beacon%" {
				t.Errorf("comment read = %s %v", calls[0].Query, args)
			}
			if !reflect.DeepEqual(prep.translationOpts.CommentLogIDs, []string{"L7", "L9"}) || !strings.Contains(prep.sql, "'L7'") {
				t.Errorf("comment ids = %v, sql %s", prep.translationOpts.CommentLogIDs, prep.sql)
			}
		})
	}

	t.Run("lookup fails", func(t *testing.T) {
		e := scopeEnv(t)
		e.set(func(e *testEnv) { e.commentErr = errors.New("pg down") })
		rec := httptest.NewRecorder()
		if prep := e.h.prepareQuery(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: q}), sessionOn("selected_fractal", "f1")), purposeSearch); prep != nil {
			t.Fatal("ran without its comment filter")
		}
		if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "pg down") {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})
}

// lookupIP() needs the GeoIP dictionaries, which exist only once MaxMind is
// configured; the server's setting is what the translator sees.
func TestPrepareQueryGeoIPFollowsSetting(t *testing.T) {
	const q = `* | lookupIP(field=src_ip, include=[country])`
	e := newTestEnv(t, 1000)
	rec := httptest.NewRecorder()
	if e.h.prepareQuery(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: q, FractalID: "f1"}), adminCtx()), purposeSearch) != nil ||
		!strings.Contains(rec.Body.String(), "requires MaxMind GeoLite2") {
		t.Fatalf("lookupIP() ran without GeoIP: %d %s", rec.Code, rec.Body.String())
	}
	e.h.SetGeoIPEnabled(true)
	prep := prepare(t, e.h, purposeSearch, searchBody(t, QueryRequest{Query: q, FractalID: "f1"}), adminCtx())
	if !strings.Contains(prep.sql, "'logs.") {
		t.Errorf("GeoIP lookup not database-qualified: %s", prep.sql)
	}
}
