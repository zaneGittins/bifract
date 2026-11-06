//go:build chcluster

package storage

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"bifract/pkg/parser"
)

// in(field, &binding) on a Distributed scan, run the way a search runs: as the search
// identity, under its row policy. The set is built once across every shard, so a key
// seen on one shard selects rows on the other, and it stays inside the fractal.
func TestBindingSetAcrossShards(t *testing.T) {
	dropEverything(t)
	initialize(t)
	c := newTestClusterClient(t)
	defer c.Close()
	ctx := context.Background()
	c.reconcileQueryIdentities(ctx, map[string]int64{QuerySearchWorkload: 1 << 30})
	defer c.closeQueryIdentities()
	if m := c.queryConns.Load(); m == nil || (*m)[QuerySearchWorkload] == nil {
		t.Fatal("search identity was not provisioned")
	}

	const insert = `INSERT INTO logs.logs (timestamp, log_id, fields, fractal_id, ingest_timestamp) VALUES `
	row := func(fractal, id, fields string) string {
		return fmt.Sprintf(`('2026-10-01 00:00:00', '%s', '%s', '%s', now64())`, id, fields, fractal)
	}
	execOnShard(t, chTestHosts[0], insert+row("f1", "a1", `{"user":"alice","user_type":"admin","host":"h1"}`)+", "+
		row("f1", "b1", `{"user":"bob","user_type":"guest","host":"h1"}`)+", "+
		row("f2", "z1", `{"user":"zed","user_type":"admin","host":"h9"}`))
	execOnShard(t, chTestHosts[1], insert+row("f1", "a2", `{"user":"alice","user_type":"guest","host":"h2"}`)+", "+
		row("f1", "b2", `{"user":"bob","user_type":"guest","host":"h2","manager":"alice"}`)+", "+
		row("f1", "z2", `{"user":"zed","user_type":"guest","host":"h3"}`))

	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	opts := parser.QueryOptions{
		FractalID: "f1", StartTime: start, EndTime: start.Add(24 * time.Hour), MaxRows: 100,
		TableName: c.ReadTable(),
	}
	search := UserSearchContext(ctx, FractalScope{FractalIDs: []string{"f1"}})
	const admins = `let &a := user_type="admin" | groupby(user); `
	for query, want := range map[string][]string{
		admins + `* | in(user, &a)`:                                             {"a1", "a2"},
		admins + `* | NOT in(user, &a)`:                                         {"b1", "b2", "z2"},
		admins + `* | NOT in(user, &a) | in(manager, &a)`:                       {"b2"},
		admins + `let &b := * | in(user, &a) | groupby(host); * | in(host, &b)`: {"a1", "a2", "b1", "b2"},
		admins + `* | in(user, &a) | join(user) { &a }`:                         {"a1", "a2"},
		admins + `* | in(user, &a) OR host="h3"`:                                {"a1", "a2", "z2"},
		`* | join(user) { user_type="admin" | groupby(user) }`:                  {"a1", "a2"},
	} {
		t.Run(query, func(t *testing.T) {
			pipeline, err := parser.ParseQuery(query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			result, err := parser.TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			rows, err := c.QueryUserSearchWithID(search, "", "SELECT log_id FROM ("+result.SQL+") ORDER BY log_id")
			if err != nil {
				t.Fatalf("query: %v\n%s", err, result.SQL)
			}
			var got []string
			for _, r := range rows {
				got = append(got, fmt.Sprint(r["log_id"]))
			}
			if !slices.Equal(got, want) {
				t.Errorf("got %v, want %v\n%s", got, want, result.SQL)
			}
		})
	}
}
