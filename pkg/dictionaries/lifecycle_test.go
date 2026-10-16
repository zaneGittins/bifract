package dictionaries

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

var ctx = context.Background()

func mustCreate(t *testing.T, w *world, opts CreateOptions) *Dictionary {
	t.Helper()
	if opts.FractalID == "" && opts.PrismID == "" {
		opts.FractalID = fractalA
	}
	d, err := w.m.CreateDictionary(ctx, opts)
	if err != nil {
		t.Fatalf("create %q: %v", opts.Name, err)
	}
	return d
}

func cols(names ...string) []DictionaryColumn {
	out := make([]DictionaryColumn, len(names))
	for i, n := range names {
		out[i] = DictionaryColumn{Name: n, Type: "string"}
	}
	return out
}

func colNames(cs []DictionaryColumn) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Name
	}
	return out
}

func (w *world) tableCols(name string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if t := w.tables[name]; t != nil {
		return slices.Clone(t.cols)
	}
	return nil
}

// A new list gets its backing table (every column plus the ordinal) and its
// HASHED dictionary, and on a single node nothing else: no ON CLUSTER (the fake
// checks every statement) and no Distributed companion.
func TestCreateDictionarySingleNode(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email", "dept"), CreatedBy: "alice"})

	if d.KeyColumn != "email" || d.CHTableName != chTableName(d.ID) {
		t.Fatalf("created %+v", d)
	}
	if got := w.tableCols(d.CHTableName); !slices.Equal(got, []string{"email", "dept", seqColumn}) {
		t.Errorf("table columns %v", got)
	}
	if w.hasTable(chDistTableName(d.ID)) {
		t.Error("a single node got a Distributed companion")
	}
	ddl := w.dictDDL(d.CHDictName)
	if !strings.Contains(ddl, "LAYOUT(HASHED())") || !strings.Contains(ddl, "FROM `logs`.`"+d.CHTableName+"` FINAL") {
		t.Errorf("dictionary DDL:\n%s", ddl)
	}
	if rec, _ := w.dict(d.ID); rec.createdBy != "alice" || rec.kind != KindValue {
		t.Errorf("stored %+v", rec)
	}
}

// On a cluster every DDL runs ON CLUSTER with a replicated engine (both checked
// by the fake), rows go through a Distributed companion pinned to shard 1, and
// the dictionary reads that companion by its database-qualified name.
func TestCreateDictionaryOnCluster(t *testing.T) {
	w := newClusterWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email", "dept")})

	if !w.hasTable(chDistTableName(d.ID)) {
		t.Fatal("no Distributed companion")
	}
	ddl := w.dictDDL(d.CHDictName)
	if !strings.Contains(ddl, "FROM `logs`.`"+chDistTableName(d.ID)+"` FINAL") {
		t.Errorf("cluster dictionary does not read the companion by qualified name:\n%s", ddl)
	}
	if !strings.Contains(ddl, "USER 'bifract_schema' PASSWORD 's3cret'") {
		t.Errorf("dictionary does not authenticate as the schema identity:\n%s", ddl)
	}
	// The rows written through the companion are readable back through it.
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: map[string]string{"email": "a@x", "dept": "ops"}}}); err != nil {
		t.Fatal(err)
	}
	if got := keysOf(w.liveRows(d.ID), "email"); !slices.Equal(got, []string{"a@x"}) {
		t.Errorf("rows %v", got)
	}
}

// The key column a caller names is prepended when the columns leave it out, and
// an empty key column defaults to the first column.
func TestCreateDictionaryKeyColumnDefaults(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "a", KeyColumn: "ip", Columns: cols("owner")})
	if d.KeyColumn != "ip" || !slices.Equal(colNames(d.Columns), []string{"ip", "owner"}) {
		t.Errorf("named key: key %q columns %v", d.KeyColumn, colNames(d.Columns))
	}
	d = mustCreate(t, w, CreateOptions{Name: "b", Columns: cols("host", "owner")})
	if d.KeyColumn != "host" {
		t.Errorf("default key %q", d.KeyColumn)
	}
}

// A list made with no columns has no ClickHouse objects; the first column added
// becomes its key and builds them.
func TestFirstColumnBuildsTheObjects(t *testing.T) {
	w := newClusterWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "empty"})
	if w.hasTable(d.CHTableName) || len(w.ch.Executed()) != 0 {
		t.Fatalf("an empty list created objects: %v", w.ch.Executed())
	}
	rows, total, err := w.m.GetRows(ctx, d.ID, "", 10, 0)
	if err != nil || rows != nil || total != 0 {
		t.Errorf("rows of an empty list: %v %d %v", rows, total, err)
	}

	d, err = w.m.AddColumn(ctx, d.ID, "host")
	if err != nil {
		t.Fatal(err)
	}
	if d.KeyColumn != "host" || !w.hasTable(d.CHTableName) || !w.hasTable(chDistTableName(d.ID)) || !w.hasDictObject(d.CHDictName) {
		t.Errorf("first column: key %q, objects table=%v dist=%v dict=%v", d.KeyColumn,
			w.hasTable(d.CHTableName), w.hasTable(chDistTableName(d.ID)), w.hasDictObject(d.CHDictName))
	}
}

// A list whose ClickHouse objects cannot be built is not left behind in
// Postgres, where match() would resolve it to a dictionary that does not exist.
func TestCreateDictionaryRemovesTheRowWhenClickHouseFails(t *testing.T) {
	w := newWorld(t)
	w.ch.FailExec("CREATE TABLE", errors.New("disk full"))
	_, err := w.m.CreateDictionary(ctx, CreateOptions{FractalID: fractalA, Name: "users", Columns: cols("email")})
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("err = %v", err)
	}
	if len(w.dicts) != 0 {
		t.Errorf("the Postgres row survived: %v", w.dicts)
	}
}

// A second column alters the table and rebuilds the dictionary with the new
// attribute. On a cluster the companion must be recreated too, since it
// snapshots its columns: without that the next write naming the new column
// fails (the fake refuses a column the companion does not have).
func TestAddColumnReachesTheCompanion(t *testing.T) {
	w := newClusterWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email")})
	d, err := w.m.AddColumn(ctx, d.ID, "dept")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(colNames(w.columnsOf(d.ID)), []string{"email", "dept"}) {
		t.Errorf("stored columns %v", w.columnsOf(d.ID))
	}
	if !strings.Contains(w.dictDDL(d.CHDictName), "`dept` String DEFAULT ''") {
		t.Errorf("dictionary not rebuilt with the new attribute:\n%s", w.dictDDL(d.CHDictName))
	}
	err = w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: map[string]string{"email": "a@x", "dept": "ops"}}})
	if err != nil {
		t.Fatalf("write naming the new column: %v", err)
	}
	if _, err := w.m.AddColumn(ctx, d.ID, "dept"); !IsInvalidInput(err) {
		t.Errorf("duplicate column: %v", err)
	}
}

// Removing a promoted column drops its secondary dictionary, the column and the
// stale companion; the key column cannot be removed.
func TestRemoveColumn(t *testing.T) {
	w := newClusterWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email", "dept", "site")})
	if _, err := w.m.SetColumnKey(ctx, d.ID, "dept"); err != nil {
		t.Fatal(err)
	}
	byDept := chColDictName(d.ID, "dept")
	if !w.hasDictObject(byDept) {
		t.Fatal("no secondary dictionary")
	}

	d, err := w.m.RemoveColumn(ctx, d.ID, "dept")
	if err != nil {
		t.Fatal(err)
	}
	if w.hasDictObject(byDept) {
		t.Error("secondary dictionary survived its column")
	}
	if slices.Contains(w.tableCols(d.CHTableName), "dept") {
		t.Error("column still in the table")
	}
	if !slices.Equal(colNames(d.Columns), []string{"email", "site"}) {
		t.Errorf("columns %v", colNames(d.Columns))
	}
	// The companion was rebuilt without it, so a write through it still works.
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: map[string]string{"email": "a@x", "site": "s"}}}); err != nil {
		t.Errorf("write after removing a column: %v", err)
	}
	if _, err := w.m.RemoveColumn(ctx, d.ID, "email"); !IsInvalidInput(err) {
		t.Errorf("removing the key: %v", err)
	}
	if _, err := w.m.RemoveColumn(ctx, d.ID, "nope"); !IsInvalidInput(err) {
		t.Errorf("removing a missing column: %v", err)
	}
}

// Promoting a column builds a HASHED dictionary keyed by it, whatever the list's
// own layout; demoting drops it. Both are idempotent and the primary key needs
// neither.
func TestSecondaryKeys(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "nets", Kind: KindNetwork, Columns: cols("cidr", "owner")})
	before := len(w.ch.Executed())
	if _, err := w.m.SetColumnKey(ctx, d.ID, "cidr"); err != nil || len(w.ch.Executed()) != before {
		t.Errorf("promoting the primary key: %v, %d statements", err, len(w.ch.Executed())-before)
	}

	d, err := w.m.SetColumnKey(ctx, d.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	ddl := w.dictDDL(chColDictName(d.ID, "owner"))
	if !strings.Contains(ddl, "LAYOUT(HASHED())") || !strings.Contains(ddl, "PRIMARY KEY `owner`") {
		t.Errorf("secondary DDL:\n%s", ddl)
	}
	if !strings.Contains(w.dictDDL(d.CHDictName), "IP_TRIE") {
		t.Error("the list's own dictionary lost its layout")
	}
	if !w.columnsOf(d.ID)[1].IsKey {
		t.Error("promotion not stored")
	}
	before = len(w.ch.Executed())
	if _, err := w.m.SetColumnKey(ctx, d.ID, "owner"); err != nil || len(w.ch.Executed()) != before {
		t.Errorf("re-promoting: %v", err)
	}
	if _, err := w.m.SetColumnKey(ctx, d.ID, "nope"); !IsInvalidInput(err) {
		t.Errorf("promoting a missing column: %v", err)
	}

	if _, err := w.m.UnsetColumnKey(ctx, d.ID, "owner"); err != nil {
		t.Fatal(err)
	}
	if w.hasDictObject(chColDictName(d.ID, "owner")) || w.columnsOf(d.ID)[1].IsKey {
		t.Error("demotion left the dictionary or the flag")
	}
	if _, err := w.m.UnsetColumnKey(ctx, d.ID, "owner"); err != nil {
		t.Errorf("demoting twice: %v", err)
	}
	if _, err := w.m.UnsetColumnKey(ctx, d.ID, "cidr"); !IsInvalidInput(err) {
		t.Errorf("demoting the primary key: %v", err)
	}
	if _, err := w.m.UnsetColumnKey(ctx, d.ID, "nope"); !IsInvalidInput(err) {
		t.Errorf("demoting a missing column: %v", err)
	}
}

// Renaming a list touches only Postgres: every ClickHouse name derives from the
// id, so match() keeps resolving through the rename.
func TestRenameTouchesNoClickHouseObject(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "old", Columns: cols("k")})
	before := len(w.ch.Executed())
	d, err := w.m.UpdateDictionary(ctx, d.ID, "new", "desc", true)
	if err != nil {
		t.Fatal(err)
	}
	if d.Name != "new" || d.Description != "desc" || !d.IsGlobal {
		t.Errorf("updated %+v", d)
	}
	if n := len(w.ch.Executed()) - before; n != 0 {
		t.Errorf("rename ran %d ClickHouse statements", n)
	}
}

// Deleting a list drops every object it owns, including secondary dictionaries
// and the companion, then the Postgres row. A refresh holding the list's lock
// (here, another replica) makes it refuse rather than drop tables under a load.
func TestDeleteDictionary(t *testing.T) {
	w := newClusterWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email", "dept")})
	if _, err := w.m.SetColumnKey(ctx, d.ID, "dept"); err != nil {
		t.Fatal(err)
	}

	w.busy[sourceLockID(d.ID)] = true
	if err := w.m.DeleteDictionary(ctx, d.ID); !errors.Is(err, errSourceBusy) {
		t.Fatalf("delete under a held lock: %v", err)
	}
	if !w.hasTable(d.CHTableName) {
		t.Fatal("a refused delete dropped the table")
	}
	delete(w.busy, sourceLockID(d.ID))

	if err := w.m.DeleteDictionary(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{d.CHTableName, chDistTableName(d.ID)} {
		if w.hasTable(name) {
			t.Errorf("%s survived", name)
		}
	}
	for _, name := range []string{d.CHDictName, chColDictName(d.ID, "dept")} {
		if w.hasDictObject(name) {
			t.Errorf("%s survived", name)
		}
	}
	if _, ok := w.dict(d.ID); ok {
		t.Error("the Postgres row survived")
	}
	if len(w.locks) != 0 {
		t.Errorf("lock not released: %v", w.locks)
	}
	if err := w.m.DeleteDictionary(ctx, "missing"); err == nil {
		t.Error("deleting a missing list succeeded")
	}
}

// Turning case-insensitivity on where keys collide is refused with the count
// until confirmed; once applied, every key column's dictionary hashes lower(key)
// and only then is the flag stored. Off rebuilds them on exact bytes.
func TestCaseInsensitiveKeys(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email", "dept")})
	if _, err := w.m.SetColumnKey(ctx, d.ID, "dept"); err != nil {
		t.Fatal(err)
	}
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{
		{Fields: map[string]string{"email": "A@x", "dept": "Ops"}},
		{Fields: map[string]string{"email": "a@x", "dept": "ops"}},
		{Fields: map[string]string{"email": "b@x", "dept": "dev"}},
	}); err != nil {
		t.Fatal(err)
	}

	report, err := w.m.KeyCollisions(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Collisions != 2 || len(report.Samples) != 2 || report.CollisionsCapped {
		t.Fatalf("report %+v", report)
	}
	if s := report.Samples[0]; s.Column != "email" || s.Key != "a@x" || !slices.Equal(s.Stored, []string{"A@x", "a@x"}) {
		t.Errorf("first sample %+v", s)
	}

	_, err = w.m.SetCaseInsensitiveKeys(ctx, d.ID, true, false)
	if !IsInvalidInput(err) || !strings.Contains(err.Error(), "2 key(s)") {
		t.Fatalf("unconfirmed collisions: %v", err)
	}
	if rec, _ := w.dict(d.ID); rec.ci {
		t.Fatal("a refused toggle was stored")
	}

	d, err = w.m.SetCaseInsensitiveKeys(ctx, d.ID, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if !d.CaseInsensitiveKeys {
		t.Error("flag not stored")
	}
	for _, name := range []string{d.CHDictName, chColDictName(d.ID, "dept")} {
		if !strings.Contains(w.dictDDL(name), "lower(`") {
			t.Errorf("%s not rebuilt over lower():\n%s", name, w.dictDDL(name))
		}
	}
	reloads := len(w.reloads)
	if _, err := w.m.SetCaseInsensitiveKeys(ctx, d.ID, true, true); err != nil || len(w.reloads) != reloads {
		t.Errorf("re-applying the same value: %v", err)
	}

	d, err = w.m.SetCaseInsensitiveKeys(ctx, d.ID, false, false)
	if err != nil || d.CaseInsensitiveKeys || strings.Contains(w.dictDDL(d.CHDictName), "lower(") {
		t.Errorf("turning it off: %v, ci=%v", err, d != nil && d.CaseInsensitiveKeys)
	}
}

// A rebuild that fails part way puts the objects back on the old expression and
// leaves the flag alone, so lookups never probe lowercased against exact keys.
func TestCaseInsensitiveRebuildFailureRollsBack(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email", "dept")})
	if _, err := w.m.SetColumnKey(ctx, d.ID, "dept"); err != nil {
		t.Fatal(err)
	}
	secondary := chColDictName(d.ID, "dept")
	w.ch.FailExecFunc(func(stmt string) error {
		if strings.HasPrefix(stmt, "CREATE OR REPLACE DICTIONARY `"+secondary+"`") && strings.Contains(stmt, "lower(") {
			return errors.New("boom")
		}
		return nil
	})
	if _, err := w.m.SetCaseInsensitiveKeys(ctx, d.ID, true, true); err == nil {
		t.Fatal("a failed rebuild reported success")
	}
	if rec, _ := w.dict(d.ID); rec.ci {
		t.Error("flag stored after a failed rebuild")
	}
	if strings.Contains(w.dictDDL(d.CHDictName), "lower(") {
		t.Error("the primary was left on the new expression")
	}

	// When the undo fails too, the error says to restart.
	w.ch.FailExecFunc(func(stmt string) error {
		if strings.HasPrefix(stmt, "CREATE OR REPLACE DICTIONARY `"+secondary+"`") {
			return errors.New("boom")
		}
		return nil
	})
	_, err := w.m.SetCaseInsensitiveKeys(ctx, d.ID, true, true)
	if err == nil || !strings.Contains(err.Error(), "restart") {
		t.Errorf("double failure: %v", err)
	}
}

// Ignoring case belongs to Values lists only.
func TestCaseInsensitiveRefusedForOtherKinds(t *testing.T) {
	w := newWorld(t)
	for _, kind := range []string{KindNetwork, KindPattern} {
		d := mustCreate(t, w, CreateOptions{Name: kind, Kind: kind, Columns: cols("k")})
		if _, err := w.m.SetCaseInsensitiveKeys(ctx, d.ID, true, true); !IsInvalidInput(err) {
			t.Errorf("%s: %v", kind, err)
		}
	}
}

// A collision count past the cap is reported as a floor.
func TestKeyCollisionsCapped(t *testing.T) {
	w := newWorld(t)
	d := mustCreate(t, w, CreateOptions{Name: "users", Columns: cols("email")})
	w.ch.Respond(func(q string, a []any) (fakedb.CHResult, bool) {
		if strings.HasPrefix(q, "SELECT count() FROM (SELECT lower(") {
			return fakedb.Count(maxCollisionCount), true
		}
		return w.answerCH(q, a)
	})
	report, err := w.m.KeyCollisions(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !report.CollisionsCapped || report.Collisions != maxCollisionCount {
		t.Errorf("report %+v", report)
	}

	empty := mustCreate(t, w, CreateOptions{Name: "empty"})
	if r, err := w.m.KeyCollisions(ctx, empty.ID); err != nil || r.Collisions != 0 {
		t.Errorf("list with no key: %+v %v", r, err)
	}
}

// Name resolution: a scope's own list shadows a global of the same name, a
// global is visible from elsewhere, and the owned-only lookup a writer uses
// never reaches another scope's global.
func TestDictionaryByName(t *testing.T) {
	w := newWorld(t)
	global := w.seed(dictRec{name: "iocs", fractalID: fractalB, global: true})
	own := w.seed(dictRec{name: "iocs", fractalID: fractalA})
	prismOwn := w.seed(dictRec{name: "assets", prismID: prismA})

	if d, err := w.m.GetDictionaryByName(ctx, fractalA, "", "iocs"); err != nil || d.ID != own.id {
		t.Errorf("fractal A resolved %v, %v; want its own", d, err)
	}
	if d, err := w.m.GetDictionaryByName(ctx, "", prismA, "iocs"); err != nil || d.ID != global.id {
		t.Errorf("prism resolved %v, %v; want the global", d, err)
	}
	if _, err := w.m.GetOwnedDictionaryByName(ctx, "", prismA, "iocs"); err == nil {
		t.Error("owned-only lookup reached another scope's global")
	}
	if d, err := w.m.GetOwnedDictionaryByName(ctx, "", prismA, "assets"); err != nil || d.ID != prismOwn.id {
		t.Errorf("prism-owned lookup: %v %v", d, err)
	}

	list, err := w.m.ListDictionaries(ctx, fractalA, "")
	if err != nil || len(list) != 2 {
		t.Fatalf("fractal list: %d %v", len(list), err)
	}
	list, _ = w.m.ListDictionaries(ctx, "", prismA)
	var names []string
	for _, d := range list {
		names = append(names, d.Name)
	}
	if !slices.Equal(names, []string{"assets", "iocs"}) {
		t.Errorf("prism list %v", names)
	}
}

// match() resolves names through these mappings: own and global lists, each key
// column to its object, and the layout flags keyed by object.
func TestListDictionaryMappingsScope(t *testing.T) {
	w := newWorld(t)
	own := w.seed(dictRec{name: "users", fractalID: fractalA, keyCol: "email",
		cols: []byte(`[{"name":"email"},{"name":"dept","is_key":true}]`), ci: true})
	nets := w.seed(dictRec{name: "nets", fractalID: fractalB, global: true, keyCol: "cidr", kind: KindNetwork,
		cols: []byte(`[{"name":"cidr"}]`)})
	w.seed(dictRec{name: "other", fractalID: fractalB, keyCol: "k", cols: []byte(`[{"name":"k"}]`)})
	w.seed(dictRec{name: "prism-only", prismID: prismA, keyCol: "k", kind: KindPattern, cols: []byte(`[{"name":"k"}]`)})

	s, err := w.m.ListDictionaryMappings(ctx, fractalA, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Mappings) != 2 || s.Mappings["other"] != nil || s.Mappings["prism-only"] != nil {
		t.Errorf("fractal scope sees %v", s.Mappings)
	}
	if s.Mappings["users"]["dept"] != chColDictName(own.id, "dept") || !s.CaseInsensitive["users"] {
		t.Errorf("users: %v ci=%v", s.Mappings["users"], s.CaseInsensitive["users"])
	}
	if !s.Network[chDictName(nets.id)] {
		t.Error("network layout not reported")
	}

	s, _ = w.m.ListDictionaryMappings(ctx, "", prismA)
	if len(s.Mappings) != 2 || s.Mappings["prism-only"] == nil || s.Mappings["nets"] == nil {
		t.Errorf("prism scope sees %v", s.Mappings)
	}
	if len(s.Pattern) != 1 {
		t.Errorf("pattern flags %v", s.Pattern)
	}
}
