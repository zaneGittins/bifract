package dictionaries

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// Startup reconcile rebuilds every list's ClickHouse objects from Postgres: a
// missing table, column, ordinal, companion or dictionary comes back, each
// with the layout its kind asks for. A list with no key is skipped, and one
// that fails is logged and left for the next start without stopping the rest.
func TestReconcileDictionaries(t *testing.T) {
	w := newClusterWorld(t)
	missing := w.seed(dictRec{name: "users", fractalID: fractalA, keyCol: "email",
		cols: []byte(`[{"name":"email"},{"name":"dept","is_key":true}]`)})
	stale := w.seed(dictRec{name: "nets", fractalID: fractalA, keyCol: "cidr", kind: KindNetwork,
		cols: []byte(`[{"name":"cidr"},{"name":"owner"}]`)})
	w.seed(dictRec{name: "empty", fractalID: fractalA})
	broken := w.seed(dictRec{name: "broken", fractalID: fractalA, keyCol: "k", cols: []byte(`[{"name":"k"}]`)})

	// The network list's table predates its owner column and the ordinal.
	w.mu.Lock()
	w.tables[chTableName(stale.id)] = &chTable{cols: []string{"cidr"}, rows: []tblRow{row(0, "cidr", "10.0.0.0/8")}}
	w.mu.Unlock()
	w.ch.FailExecFunc(func(stmt string) error {
		if strings.Contains(stmt, "CREATE TABLE IF NOT EXISTS `"+chTableName(broken.id)+"`") {
			return errors.New("keeper unavailable")
		}
		return nil
	})

	n, err := w.m.ReconcileDictionaries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("reconciled %d, want 2", n)
	}
	for _, name := range []string{chTableName(missing.id), chDistTableName(missing.id), chDistTableName(stale.id)} {
		if !w.hasTable(name) {
			t.Errorf("%s not created", name)
		}
	}
	for _, name := range []string{chDictName(missing.id), chColDictName(missing.id, "dept"), chDictName(stale.id)} {
		if !w.hasDictObject(name) {
			t.Errorf("%s not created", name)
		}
	}
	if got := w.tableCols(chTableName(stale.id)); !slices.Equal(got, []string{"cidr", seqColumn, "owner"}) {
		t.Errorf("stale table columns %v", got)
	}
	if ddl := w.dictDDL(chDictName(stale.id)); !strings.Contains(ddl, "IP_TRIE") {
		t.Errorf("network list rebuilt without its layout:\n%s", ddl)
	}
	if w.hasDictObject(chDictName(broken.id)) {
		t.Error("a list whose table failed got a dictionary")
	}
	if len(w.liveRows(stale.id)) != 1 {
		t.Error("reconcile touched existing rows")
	}
}

// Rows an older release scattered across shards are gathered onto shard 1: the
// copy already there wins, a key held only elsewhere keeps its lowest shard's
// copy, every key keeps its ordinal, and nothing off shard 1 remains.
func TestReconcileConsolidatesShards(t *testing.T) {
	w := newClusterWorld(t)
	d := w.seed(dictRec{name: "users", fractalID: fractalA, keyCol: "email", cols: []byte(`[{"name":"email"},{"name":"dept"}]`)})
	at := func(shard uint32, r tblRow) tblRow { r.shard = shard; return r }
	w.seedTable(d.id, []string{"email", "dept"},
		at(1, row(1, "email", "a", "dept", "current")),
		at(2, row(1, "email", "a", "dept", "stale")),
		at(3, row(2, "email", "b", "dept", "from-3")),
		at(2, row(2, "email", "b", "dept", "from-2")),
		at(1, row(3, "email", "c", "dept", "only")),
	)

	if n, err := w.m.ReconcileDictionaries(ctx); err != nil || n != 1 {
		t.Fatalf("reconcile: %d %v", n, err)
	}
	live := w.liveRows(d.id)
	got := map[string]string{}
	for _, r := range live {
		if r.shard != writeShardNum {
			t.Errorf("row %v left on shard %d", r.vals, r.shard)
		}
		got[r.vals["email"]] = r.vals["dept"]
	}
	if len(live) != 3 || got["a"] != "current" || got["b"] != "from-2" || got["c"] != "only" {
		t.Errorf("after consolidation %v", got)
	}
	if !slices.Equal(seqsOf(live), []uint64{1, 2, 3}) {
		t.Errorf("ordinals %v", seqsOf(live))
	}

	// Once consolidated, a second reconcile writes nothing.
	inserts := len(w.ch.ExecutedMatching("INSERT INTO"))
	if _, err := w.m.ReconcileDictionaries(ctx); err != nil {
		t.Fatal(err)
	}
	if len(w.ch.ExecutedMatching("INSERT INTO")) != inserts {
		t.Error("a consolidated list was rewritten")
	}
}

// A rewrite that fails deletes nothing: the duplicates stay for the next start.
func TestConsolidationFailureLosesNothing(t *testing.T) {
	w := newClusterWorld(t)
	d := w.seed(dictRec{name: "users", fractalID: fractalA, keyCol: "email", cols: []byte(`[{"name":"email"}]`)})
	stray := row(1, "email", "a")
	stray.shard = 2
	w.seedTable(d.id, []string{"email"}, stray)
	w.ch.FailExec("INSERT INTO", errors.New("too many parts"))

	if n, _ := w.m.ReconcileDictionaries(ctx); n != 0 {
		t.Errorf("a failed consolidation counted as reconciled")
	}
	if len(w.ch.ExecutedMatching("DELETE WHERE")) != 0 || len(w.liveRows(d.id)) != 1 {
		t.Error("rows were deleted before their replacement landed")
	}
}

// On a single node there are no shards to gather.
func TestReconcileSingleNodeReadsNoShards(t *testing.T) {
	w := newWorld(t)
	d := w.seed(dictRec{name: "users", fractalID: fractalA, keyCol: "email", cols: []byte(`[{"name":"email"}]`)})
	w.seedTable(d.id, []string{"email"}, row(1, "email", "a"))
	if n, err := w.m.ReconcileDictionaries(ctx); err != nil || n != 1 {
		t.Fatalf("reconcile: %d %v", n, err)
	}
	if q := w.ch.QueriedMatching("_shard_num"); len(q) != 0 {
		t.Errorf("single node read shards: %v", q)
	}
}

// Dictionary actions are stored per scope; maxLogs defaults to 1000.
func TestDictionaryActionCRUD(t *testing.T) {
	w := newWorld(t)
	a, err := w.m.CreateDictionaryAction(ctx, "collect", "d", "seen", 0, true, "alice", fractalA, "")
	if err != nil {
		t.Fatal(err)
	}
	if a.MaxLogsPerTrigger != 1000 || a.FractalID != fractalA || a.PrismID != "" || a.CreatedBy != "alice" {
		t.Errorf("created %+v", a)
	}
	p, err := w.m.CreateDictionaryAction(ctx, "prism-collect", "", "seen", 5, false, "", "", prismA)
	if err != nil {
		t.Fatal(err)
	}
	if list, _ := w.m.ListDictionaryActions(ctx, fractalA, ""); len(list) != 1 || list[0].ID != a.ID {
		t.Errorf("fractal list %v", list)
	}
	if list, _ := w.m.ListDictionaryActions(ctx, "", prismA); len(list) != 1 || list[0].ID != p.ID {
		t.Errorf("prism list %v", list)
	}
	if list, _ := w.m.ListDictionaryActions(ctx, "", ""); len(list) != 2 {
		t.Errorf("unscoped list %v", list)
	}

	a, err = w.m.UpdateDictionaryAction(ctx, a.ID, "renamed", "", "seen2", -1, false)
	if err != nil || a.Name != "renamed" || a.MaxLogsPerTrigger != 1000 || a.Enabled {
		t.Errorf("updated %+v %v", a, err)
	}

	w.alertActions["alert-1"] = []string{a.ID, p.ID}
	linked, err := w.m.GetDictionaryActionsByAlertID(ctx, "alert-1")
	if err != nil || len(linked) != 2 {
		t.Errorf("linked %v %v", linked, err)
	}

	if err := w.m.DeleteDictionaryAction(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := w.m.GetDictionaryAction(ctx, a.ID); err == nil {
		t.Error("deleted action still found")
	}
}

// singleKeyLog has one top-level field, so it is the key whatever the map order
// (picking the key from map iteration is a known issue).
func singleKeyLog(ip string, kv ...string) map[string]interface{} {
	f := map[string]interface{}{}
	for i := 0; i+1 < len(kv); i += 2 {
		f[kv[i]] = kv[i+1]
	}
	return map[string]interface{}{"ip": ip, "fields": f}
}

// A trigger with no list of that name creates one in the action's scope, keyed
// by the first field, and fills it. Logs past the action's cap are dropped and
// a log with no key is skipped.
func TestExecuteDictionaryActionCreates(t *testing.T) {
	w := newClusterWorld(t)
	action := &DictionaryAction{Name: "collect", DictionaryName: "seen-ips", MaxLogsPerTrigger: 2}
	n, err := w.m.ExecuteDictionaryAction(ctx, action, "", prismA, []map[string]interface{}{
		singleKeyLog("1.1.1.1", "user", "alice"),
		{"fields": map[string]interface{}{"user": "nokey"}},
		singleKeyLog("3.3.3.3", "user", "carol"),
	})
	if err != nil || n != 1 {
		t.Fatalf("execute: %d %v", n, err)
	}
	d, err := w.m.GetDictionaryByName(ctx, "", prismA, "seen-ips")
	if err != nil {
		t.Fatal(err)
	}
	if d.KeyColumn != "ip" || d.PrismID != prismA || d.CreatedBy != "admin" {
		t.Errorf("created %+v", d)
	}
	live := w.liveRows(d.ID)
	if len(live) != 1 || live[0].vals["ip"] != "1.1.1.1" || live[0].vals["user"] != "alice" {
		t.Errorf("rows %v", live)
	}
}

// A trigger against an existing list rebuilds its schema to the logs' fields and
// replaces its rows outright.
func TestExecuteDictionaryActionReplaces(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "seen-ips", Columns: cols("host", "old")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("host", "h1", "old", "x")}}); err != nil {
		t.Fatal(err)
	}
	action := &DictionaryAction{Name: "collect", DictionaryName: "seen-ips", CreatedBy: "bob"}
	n, err := w.m.ExecuteDictionaryAction(ctx, action, fractalA, "", []map[string]interface{}{
		singleKeyLog("1.1.1.1", "user", "alice"),
		singleKeyLog("2.2.2.2", "user", "bob"),
	})
	if err != nil || n != 2 {
		t.Fatalf("execute: %d %v", n, err)
	}
	d, _ = w.m.GetDictionary(ctx, d.ID)
	if d.KeyColumn != "ip" || !slices.Equal(colNames(d.Columns), []string{"ip", "user"}) {
		t.Errorf("schema key %q columns %v", d.KeyColumn, colNames(d.Columns))
	}
	if got := keysOf(w.liveRows(d.ID), "ip"); !slices.Equal(got, []string{"1.1.1.1", "2.2.2.2"}) {
		t.Errorf("rows %v", got)
	}
	if len(w.dicts) != 1 {
		t.Errorf("a second list was created: %d", len(w.dicts))
	}
}

// A global of the same name owned by another scope is never written: the
// trigger creates a list of its own instead. A sourced list is refused.
func TestExecuteDictionaryActionWritesOnlyOwnedLists(t *testing.T) {
	w := newWorld(t)
	global, err := w.m.CreateDictionary(ctx, CreateOptions{FractalID: fractalB, Name: "shared", IsGlobal: true, Columns: cols("ip")})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.m.UpsertRows(ctx, global.ID, []DictionaryRow{{Fields: fields("ip", "9.9.9.9")}}); err != nil {
		t.Fatal(err)
	}
	action := &DictionaryAction{Name: "collect", DictionaryName: "shared"}
	if _, err := w.m.ExecuteDictionaryAction(ctx, action, fractalA, "", []map[string]interface{}{singleKeyLog("1.1.1.1")}); err != nil {
		t.Fatal(err)
	}
	if got := keysOf(w.liveRows(global.ID), "ip"); !slices.Equal(got, []string{"9.9.9.9"}) {
		t.Errorf("the other scope's global was overwritten: %v", got)
	}
	if own, err := w.m.GetOwnedDictionaryByName(ctx, fractalA, "", "shared"); err != nil || own.ID == global.ID {
		t.Errorf("no list of its own: %v %v", own, err)
	}

	w.seed(dictRec{name: "feed", fractalID: fractalA, keyCol: "ip", cols: []byte(`[{"name":"ip"}]`),
		source: []byte(`{"url":"https://example.com/a.csv","key_field":"ip"}`)})
	action.DictionaryName = "feed"
	if _, err := w.m.ExecuteDictionaryAction(ctx, action, fractalA, "", []map[string]interface{}{singleKeyLog("1.1.1.1")}); !IsInvalidInput(err) {
		t.Errorf("sourced list: %v", err)
	}
}

// A trigger whose rows the list's kind refuses changes nothing: the check runs
// before the rebuild and TRUNCATE, which would otherwise leave the list empty
// with nothing written in its place.
func TestExecuteDictionaryActionRefusedRowsKeepTheList(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "nets", Kind: KindNetwork, Columns: cols("ip", "owner")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("ip", "10.0.0.0/8", "owner", "corp")}}); err != nil {
		t.Fatal(err)
	}
	execs := len(w.ch.Executed())
	action := &DictionaryAction{Name: "collect", DictionaryName: "nets"}
	if _, err := w.m.ExecuteDictionaryAction(ctx, action, fractalA, "", []map[string]interface{}{singleKeyLog("1.2.3.4")}); !IsInvalidInput(err) {
		t.Fatalf("err = %v", err)
	}
	if got := keysOf(w.liveRows(d.ID), "ip"); !slices.Equal(got, []string{"10.0.0.0/8"}) {
		t.Errorf("rows %v", got)
	}
	if n := len(w.ch.Executed()) - execs; n != 0 {
		t.Errorf("a refused trigger ran %d ClickHouse statements", n)
	}
	if !slices.Equal(colNames(w.columnsOf(d.ID)), []string{"ip", "owner"}) {
		t.Errorf("schema changed: %v", w.columnsOf(d.ID))
	}
}
