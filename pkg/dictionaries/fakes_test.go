package dictionaries

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

const (
	fractalA = "11111111-1111-1111-1111-111111111111"
	fractalB = "22222222-2222-2222-2222-222222222222"
	prismA   = "33333333-3333-3333-3333-333333333333"
)

// world is an in-memory Postgres (the dictionaries, dictionary_actions and
// settings tables, plus advisory locks) and an in-memory ClickHouse (backing
// tables, Distributed companions and dictionary objects) behind the scripted
// drivers, wired into a real Manager.
//
// Both sides interpret only the statement shapes this package sends, and fail
// the test on any other, so a change to the SQL is noticed here rather than
// answered with an empty result. The ClickHouse side models what the tests rely
// on: FINAL keeps the last copy of a key per shard, a Distributed companion
// snapshots its columns when created and refuses mutations, and a row written
// through it lands on shard 1.
type world struct {
	t       *testing.T
	pg      *fakedb.PG
	ch      *fakedb.CH
	chc     *storage.ClickHouseClient
	m       *Manager
	cluster bool

	mu           sync.Mutex
	dicts        map[string]*dictRec
	actions      map[string]*actionRec
	alertActions map[string][]string
	settings     map[string]string
	locks        map[int64]bool
	// busy marks lock ids held by another replica.
	busy  map[int64]bool
	next  int
	clock time.Time

	tables   map[string]*chTable
	dists    map[string]*distTable
	dictObjs map[string]string
	reloads  []string
}

type dictRec struct {
	id, name, desc     string
	fractalID, prismID string
	global             bool
	keyCol             string
	cols               []byte
	rowCount           int64
	ci                 bool
	kind               string
	createdBy          string
	created            time.Time
	source, status     []byte
}

func (d *dictRec) values() []driver.Value {
	return []driver.Value{d.id, d.name, d.desc, d.fractalID, d.prismID, d.global, d.keyCol, d.cols,
		d.rowCount, d.ci, d.kind, d.createdBy, d.created, d.created, nilIfEmpty(d.source), nilIfEmpty(d.status)}
}

func nilIfEmpty(b []byte) driver.Value {
	if b == nil {
		return nil
	}
	return b
}

type actionRec struct {
	id, name, desc, dictName string
	maxLogs                  int64
	enabled                  bool
	createdBy                string
	created                  time.Time
	fractalID, prismID       string
}

func (a *actionRec) values() []driver.Value {
	return []driver.Value{a.id, a.name, a.desc, a.dictName, a.maxLogs, a.enabled, a.createdBy,
		a.created, a.created, a.fractalID, a.prismID}
}

type tblRow struct {
	vals  map[string]string
	seq   uint64
	shard uint32
}

type chTable struct {
	cols []string
	rows []tblRow
}

type distTable struct {
	local string
	cols  []string
}

func newWorld(t *testing.T) *world {
	return newWorldWith(t, storage.Topology{Kind: storage.DeploymentSingleNode})
}

func newClusterWorld(t *testing.T) *world {
	topo, err := storage.DeriveTopology(storage.TopologySpec{Cluster: "bifract"})
	if err != nil {
		t.Fatal(err)
	}
	w := newWorldWith(t, topo)
	w.cluster = true
	return w
}

func newWorldWith(t *testing.T, topo storage.Topology) *world {
	pgc, pg := storagetest.Postgres()
	chc, ch := storagetest.ClickHouseWith(topo)
	chc.User, chc.Password = "bifract_schema", "s3cret"
	w := &world{
		t: t, pg: pg, ch: ch, chc: chc,
		dicts: map[string]*dictRec{}, actions: map[string]*actionRec{}, alertActions: map[string][]string{},
		settings: map[string]string{}, locks: map[int64]bool{}, busy: map[int64]bool{},
		clock:  time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		tables: map[string]*chTable{}, dists: map[string]*distTable{}, dictObjs: map[string]string{},
	}
	pg.Respond(w.answerPG)
	ch.Respond(w.answerCH)
	ch.FailExecFunc(w.execCH)
	w.m = NewManager(pgc, chc)
	return w
}

func (w *world) newID() string {
	w.next++
	return fmt.Sprintf("aaaaaaaa-0000-4000-8000-%012d", w.next)
}

func (w *world) tick() time.Time {
	w.clock = w.clock.Add(time.Second)
	return w.clock
}

// dict returns a copy of a stored dictionary row.
func (w *world) dict(id string) (dictRec, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	d, ok := w.dicts[id]
	if !ok {
		return dictRec{}, false
	}
	return *d, true
}

func (w *world) columnsOf(id string) []DictionaryColumn {
	d, _ := w.dict(id)
	var cols []DictionaryColumn
	_ = json.Unmarshal(d.cols, &cols)
	return cols
}

func (w *world) statusOf(id string) SourceStatus {
	d, _ := w.dict(id)
	var st SourceStatus
	if d.status != nil {
		_ = json.Unmarshal(d.status, &st)
	}
	return st
}

// seed stores a dictionary row directly, as a previous release left it.
func (w *world) seed(d dictRec) *dictRec {
	w.mu.Lock()
	defer w.mu.Unlock()
	if d.id == "" {
		d.id = w.newID()
	}
	if d.kind == "" {
		d.kind = KindValue
	}
	if d.cols == nil {
		d.cols = []byte("[]")
	}
	d.created = w.tick()
	w.dicts[d.id] = &d
	return &d
}

// seedTable creates a dictionary's ClickHouse objects directly, with rows.
func (w *world) seedTable(id string, cols []string, rows ...tblRow) {
	w.mu.Lock()
	defer w.mu.Unlock()
	all := append(slices.Clone(cols), seqColumn)
	w.tables[chTableName(id)] = &chTable{cols: all}
	for _, r := range rows {
		if r.shard == 0 {
			r.shard = 1
		}
		w.tables[chTableName(id)].rows = append(w.tables[chTableName(id)].rows, r)
	}
	if w.cluster {
		w.dists[chDistTableName(id)] = &distTable{local: chTableName(id), cols: slices.Clone(all)}
	}
	w.dictObjs[chDictName(id)] = "seeded"
}

func row(seq uint64, kv ...string) tblRow {
	r := tblRow{vals: map[string]string{}, seq: seq, shard: 1}
	for i := 0; i+1 < len(kv); i += 2 {
		r.vals[kv[i]] = kv[i+1]
	}
	return r
}

// final is a table's rows as FINAL reads them: the last copy of each key on each
// shard, in the order those copies were written.
func (w *world) final(tbl *chTable, key string) []tblRow {
	last := map[string]int{}
	for i, r := range tbl.rows {
		last[fmt.Sprintf("%d\x00%s", r.shard, r.vals[key])] = i
	}
	var out []tblRow
	for i, r := range tbl.rows {
		if last[fmt.Sprintf("%d\x00%s", r.shard, r.vals[key])] == i {
			out = append(out, r)
		}
	}
	return out
}

// liveRows is what the editor would show for a list: FINAL rows sorted by ordinal.
func (w *world) liveRows(id string) []tblRow {
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.dicts[id]
	tbl := w.tables[chTableName(id)]
	if d == nil || tbl == nil {
		return nil
	}
	rows := w.final(tbl, d.keyCol)
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].seq < rows[j].seq })
	return rows
}

// keysOf lists the key column of rows, in order.
func keysOf(rows []tblRow, key string) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.vals[key]
	}
	return out
}

func (w *world) hasTable(name string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.tables[name]
	_, dok := w.dists[name]
	return ok || dok
}

func (w *world) hasDictObject(name string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.dictObjs[name]
	return ok
}

func (w *world) dictDDL(name string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dictObjs[name]
}

// ---- Postgres ----

var (
	pgSetClause = regexp.MustCompile(`(\w+) = (\$\d+|NULL)`)
	pgWhereID   = regexp.MustCompile(`WHERE id = \$(\d+)`)
)

func argAt(args []driver.Value, placeholder string) driver.Value {
	i, _ := strconv.Atoi(strings.TrimPrefix(placeholder, "$"))
	return args[i-1]
}

func str(v driver.Value) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	}
	return ""
}

func bytesOf(v driver.Value) []byte {
	switch s := v.(type) {
	case []byte:
		return slices.Clone(s)
	case string:
		return []byte(s)
	}
	return nil
}

func (w *world) answerPG(q string, args []driver.Value) (fakedb.PGResult, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case strings.Contains(q, "pg_try_advisory_lock"):
		id := args[0].(int64)
		if w.busy[id] || w.locks[id] {
			return fakedb.Row(false), true
		}
		w.locks[id] = true
		return fakedb.Row(true), true
	case strings.Contains(q, "pg_advisory_unlock"):
		id := args[0].(int64)
		held := w.locks[id]
		delete(w.locks, id)
		return fakedb.Row(held), true
	case strings.Contains(q, "FROM settings WHERE key"):
		if v, ok := w.settings[str(args[0])]; ok {
			return fakedb.Row(v), true
		}
		return fakedb.PGResult{}, true
	case strings.Contains(q, "dictionary_actions"):
		return w.answerActions(q, args), true
	case strings.Contains(q, "dictionaries"):
		return w.answerDicts(q, args), true
	}
	w.t.Errorf("unexpected Postgres statement: %s", q)
	return fakedb.PGResult{}, true
}

func (w *world) answerDicts(q string, args []driver.Value) fakedb.PGResult {
	switch {
	case strings.HasPrefix(strings.TrimSpace(q), "INSERT INTO dictionaries"):
		d := &dictRec{id: w.newID(), name: str(args[0]), desc: str(args[1]), fractalID: str(args[2]), prismID: str(args[3]),
			keyCol: str(args[4]), cols: bytesOf(args[5]), global: args[6].(bool), kind: str(args[7]),
			createdBy: str(args[8]), source: bytesOf(args[9]), status: bytesOf(args[10]), created: w.tick()}
		w.dicts[d.id] = d
		return fakedb.Row(d.id)

	case strings.HasPrefix(strings.TrimSpace(q), "UPDATE dictionaries SET"):
		m := pgWhereID.FindStringSubmatch(q)
		d := w.dicts[str(argAt(args, "$"+m[1]))]
		if d == nil {
			return fakedb.Affected(0)
		}
		set := q[strings.Index(q, "SET")+3 : strings.Index(q, "WHERE")]
		for _, sm := range pgSetClause.FindAllStringSubmatch(set, -1) {
			var v driver.Value
			if sm[2] != "NULL" {
				v = argAt(args, sm[2])
			}
			switch sm[1] {
			case "name":
				d.name = str(v)
			case "description":
				d.desc = str(v)
			case "is_global":
				d.global = v.(bool)
			case "key_column":
				d.keyCol = str(v)
			case "columns":
				d.cols = bytesOf(v)
			case "case_insensitive_keys":
				d.ci = v.(bool)
			case "row_count":
				d.rowCount = v.(int64)
			case "source":
				d.source = bytesOf(v)
			case "source_status":
				d.status = bytesOf(v)
			default:
				w.t.Errorf("unexpected column %q in %s", sm[1], q)
			}
		}
		return fakedb.Affected(1)

	case strings.HasPrefix(strings.TrimSpace(q), "DELETE FROM dictionaries WHERE id = $1"):
		delete(w.dicts, str(args[0]))
		return fakedb.Affected(1)

	case strings.Contains(q, "WHERE source IS NOT NULL"):
		var rows [][]driver.Value
		for _, d := range w.sortedDicts() {
			if d.source != nil {
				rows = append(rows, []driver.Value{d.id, d.source, nilIfEmpty(d.status)})
			}
		}
		return fakedb.RowsOf(rows...)

	case strings.TrimSpace(q) == "SELECT id FROM dictionaries":
		var rows [][]driver.Value
		for _, d := range w.sortedDicts() {
			rows = append(rows, []driver.Value{d.id})
		}
		return fakedb.RowsOf(rows...)

	case strings.HasPrefix(strings.TrimSpace(q), "SELECT id, COALESCE(key_column, ''), columns"):
		var rows [][]driver.Value
		for _, d := range w.sortedDicts() {
			rows = append(rows, []driver.Value{d.id, d.keyCol, d.cols, d.ci, d.kind})
		}
		return fakedb.RowsOf(rows...)

	case strings.HasPrefix(strings.TrimSpace(q), "SELECT id, name, key_column, columns"):
		// ListDictionaryMappings.
		var rows [][]driver.Value
		for _, d := range w.inScope(q, str(args[0])) {
			rows = append(rows, []driver.Value{d.id, d.name, d.keyCol, d.cols, d.ci, d.kind})
		}
		return fakedb.RowsOf(rows...)

	case strings.Contains(q, "FROM dictionaries WHERE id = $1"):
		if d := w.dicts[str(args[0])]; d != nil {
			return fakedb.Row(d.values()...)
		}
		return fakedb.PGResult{}

	case strings.Contains(q, "WHERE name = $2"):
		return w.byName(q, str(args[0]), str(args[1]))

	case strings.Contains(q, "OR is_global = true ORDER BY name ASC"):
		var rows [][]driver.Value
		ds := w.inScope(q, str(args[0]))
		sort.SliceStable(ds, func(i, j int) bool { return ds[i].name < ds[j].name })
		for _, d := range ds {
			rows = append(rows, d.values())
		}
		return fakedb.RowsOf(rows...)
	}
	w.t.Errorf("unexpected dictionaries statement: %s", q)
	return fakedb.PGResult{}
}

func (w *world) sortedDicts() []*dictRec {
	out := make([]*dictRec, 0, len(w.dicts))
	for _, d := range w.dicts {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].created.Before(out[j].created) })
	return out
}

// inScope answers "WHERE fractal_id = $1 OR is_global = true" and its prism twin.
func (w *world) inScope(q, scope string) []*dictRec {
	byPrism := strings.Contains(q, "prism_id = $1")
	var out []*dictRec
	for _, d := range w.sortedDicts() {
		own := d.fractalID == scope
		if byPrism {
			own = d.prismID == scope
		}
		if own || d.global {
			out = append(out, d)
		}
	}
	return out
}

// byName answers the four by-name lookups: owned-first, then oldest.
func (w *world) byName(q, scope, name string) fakedb.PGResult {
	byPrism := strings.Contains(q, "prism_id::text = $1")
	withGlobals := strings.Contains(q, "OR is_global = true")
	var match []*dictRec
	for _, d := range w.sortedDicts() {
		if d.name != name {
			continue
		}
		own := d.fractalID == scope
		if byPrism {
			own = d.prismID == scope
		}
		if own || (withGlobals && d.global) {
			match = append(match, d)
		}
	}
	sort.SliceStable(match, func(i, j int) bool {
		oi := match[i].fractalID == scope || match[i].prismID == scope
		oj := match[j].fractalID == scope || match[j].prismID == scope
		return oi && !oj
	})
	if len(match) == 0 {
		return fakedb.PGResult{}
	}
	return fakedb.Row(match[0].values()...)
}

func (w *world) answerActions(q string, args []driver.Value) fakedb.PGResult {
	trimmed := strings.TrimSpace(q)
	switch {
	case strings.HasPrefix(trimmed, "INSERT INTO dictionary_actions"):
		a := &actionRec{id: w.newID(), name: str(args[0]), desc: str(args[1]), dictName: str(args[2]),
			maxLogs: args[3].(int64), enabled: args[4].(bool), createdBy: str(args[5]),
			fractalID: str(args[6]), prismID: str(args[7]), created: w.tick()}
		w.actions[a.id] = a
		return fakedb.Row(a.id)
	case strings.HasPrefix(trimmed, "UPDATE dictionary_actions"):
		a := w.actions[str(args[5])]
		if a == nil {
			return fakedb.Affected(0)
		}
		a.name, a.desc, a.dictName = str(args[0]), str(args[1]), str(args[2])
		a.maxLogs, a.enabled = args[3].(int64), args[4].(bool)
		return fakedb.Affected(1)
	case strings.HasPrefix(trimmed, "DELETE FROM dictionary_actions"):
		delete(w.actions, str(args[0]))
		return fakedb.Affected(1)
	case strings.Contains(q, "JOIN alert_dictionary_actions"):
		var rows [][]driver.Value
		for _, id := range w.alertActions[str(args[0])] {
			if a := w.actions[id]; a != nil {
				rows = append(rows, a.values())
			}
		}
		return fakedb.RowsOf(rows...)
	case strings.Contains(q, "WHERE da.id = $1"):
		if a := w.actions[str(args[0])]; a != nil {
			return fakedb.Row(a.values()...)
		}
		return fakedb.PGResult{}
	case strings.Contains(q, "ORDER BY da.name ASC"):
		var out []*actionRec
		for _, a := range w.actions {
			switch {
			case strings.Contains(q, "da.prism_id = $1"):
				if a.prismID != str(args[0]) {
					continue
				}
			case strings.Contains(q, "da.fractal_id = $1"):
				if a.fractalID != str(args[0]) {
					continue
				}
			}
			out = append(out, a)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
		var rows [][]driver.Value
		for _, a := range out {
			rows = append(rows, a.values())
		}
		return fakedb.RowsOf(rows...)
	}
	w.t.Errorf("unexpected dictionary_actions statement: %s", q)
	return fakedb.PGResult{}
}

// ---- ClickHouse ----

const (
	identRe    = "`((?:[^`]|``)+)`"
	onCluster  = `(?: ON CLUSTER '[^']*')?`
	quotedStrs = `'((?:[^'\\]|\\.)*)'`
)

var (
	reCreateTable = regexp.MustCompile(`^CREATE TABLE IF NOT EXISTS ` + identRe + onCluster + ` \((.*)\) ENGINE = (\w+)\(.*ORDER BY \(` + identRe + `\)$`)
	reColDef      = regexp.MustCompile(identRe + ` (String|UInt64) DEFAULT`)
	reCreateDist  = regexp.MustCompile(`^CREATE OR REPLACE TABLE ` + identRe + onCluster + ` AS ` + identRe + ` ENGINE = Distributed\('([^']*)', currentDatabase\(\), '([^']*)', 0\)$`)
	reAddColumn   = regexp.MustCompile(`^ALTER TABLE ` + identRe + onCluster + ` ADD COLUMN IF NOT EXISTS ` + identRe + ` (String|UInt64) DEFAULT`)
	reDropColumn  = regexp.MustCompile(`^ALTER TABLE ` + identRe + onCluster + ` DROP COLUMN IF EXISTS ` + identRe + `$`)
	reDeleteRows  = regexp.MustCompile(`^ALTER TABLE ` + identRe + onCluster + ` DELETE WHERE (.*) SETTINGS mutations_sync = (\d)$`)
	reTruncate    = regexp.MustCompile(`^TRUNCATE TABLE ` + identRe + onCluster + `$`)
	reDropTable   = regexp.MustCompile(`^DROP TABLE IF EXISTS ` + identRe + onCluster + `( SYNC.*)?$`)
	reCreateDict  = regexp.MustCompile(`(?s)^CREATE OR REPLACE DICTIONARY ` + identRe + onCluster + ` \(`)
	reDropDict    = regexp.MustCompile(`^DROP DICTIONARY IF EXISTS ` + identRe + onCluster + `$`)
	reReload      = regexp.MustCompile(`^SYSTEM RELOAD DICTIONARY` + onCluster + ` ` + identRe + `\.` + identRe + `$`)
	reInsert      = regexp.MustCompile(`^INSERT INTO ` + identRe + ` \(([^)]*)\)(?: SETTINGS [^V]*)? VALUES `)
	reIdent       = regexp.MustCompile(identRe)
	reQuoted      = regexp.MustCompile(quotedStrs)

	reWhereIn     = regexp.MustCompile("^" + identRe + ` IN \((.*)\)(?: AND ` + "`_bf_seq`" + ` < (\d+))?$`)
	reWhereSeqLt  = regexp.MustCompile("^`_bf_seq` < (\\d+)$")
	reWhereEq     = regexp.MustCompile("^" + identRe + ` = ` + quotedStrs + `$`)
	reWhereNotEmp = regexp.MustCompile("^" + identRe + ` != ''$`)
	reWhereSearch = regexp.MustCompile(`^\(positionCaseInsensitive\(` + identRe + `, ` + quotedStrs + `\) > 0`)

	reSelect = regexp.MustCompile(`^SELECT (.+?) FROM ` + identRe + `( FINAL)?\s*(?:WHERE (.+?))?\s*(?:ORDER BY (.+?))?\s*(?:LIMIT (\d+)(?: OFFSET (\d+))?)?$`)
	reLower  = regexp.MustCompile(`FROM ` + identRe + ` FINAL WHERE notEmpty\(` + identRe + `\)`)
)

func unident(s string) string { return strings.ReplaceAll(s, "``", "`") }

func unquote(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// resolve names the local table a statement reaches, and the column set it sees.
func (w *world) resolve(name string) (*chTable, []string, bool, error) {
	if d, ok := w.dists[name]; ok {
		tbl := w.tables[d.local]
		if tbl == nil {
			return nil, nil, true, fakedb.Exception(60, "Table "+d.local+" does not exist (via "+name+")")
		}
		return tbl, d.cols, true, nil
	}
	if tbl, ok := w.tables[name]; ok {
		return tbl, tbl.cols, false, nil
	}
	return nil, nil, false, fakedb.Exception(60, "Table "+name+" does not exist")
}

func (w *world) needClusterClause(stmt string) {
	if w.cluster != strings.Contains(stmt, " ON CLUSTER 'bifract'") {
		w.t.Errorf("ON CLUSTER is %v on a cluster=%v deployment: %s", !w.cluster, w.cluster, stmt)
	}
}

// execCH applies a statement to the in-memory ClickHouse. It runs as the fake's
// exec-failure hook, the one place an Exec can be observed as it happens; a
// failure scripted with FailExec after construction runs first and stops it.
func (w *world) execCH(stmt string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case reInsert.MatchString(stmt):
		return w.insert(stmt)
	case strings.HasPrefix(stmt, "SYSTEM RELOAD"):
		m := reReload.FindStringSubmatch(stmt)
		if m == nil {
			w.t.Errorf("unparsed reload: %s", stmt)
			return nil
		}
		if w.cluster != strings.Contains(stmt, "ON CLUSTER") {
			w.t.Errorf("reload not cluster-wide: %s", stmt)
		}
		if unident(m[1]) != "logs" {
			w.t.Errorf("reload names database %q, want logs", m[1])
		}
		w.reloads = append(w.reloads, unident(m[2]))
		return nil
	}
	w.needClusterClause(stmt)
	if m := reCreateTable.FindStringSubmatch(stmt); m != nil {
		name := unident(m[1])
		if _, ok := w.tables[name]; ok {
			return nil
		}
		tbl := &chTable{}
		for _, c := range reColDef.FindAllStringSubmatch(m[2], -1) {
			tbl.cols = append(tbl.cols, unident(c[1]))
		}
		if w.cluster != strings.HasPrefix(m[3], "Replicated") {
			w.t.Errorf("engine %s on cluster=%v", m[3], w.cluster)
		}
		w.tables[name] = tbl
		return nil
	}
	if m := reCreateDist.FindStringSubmatch(stmt); m != nil {
		local := unident(m[2])
		tbl, ok := w.tables[local]
		if !ok {
			return fakedb.Exception(60, "Table "+local+" does not exist")
		}
		if m[3] != "bifract" || m[4] != local {
			w.t.Errorf("Distributed companion points at cluster %q table %q: %s", m[3], m[4], stmt)
		}
		w.dists[unident(m[1])] = &distTable{local: local, cols: slices.Clone(tbl.cols)}
		return nil
	}
	if m := reAddColumn.FindStringSubmatch(stmt); m != nil {
		tbl, err := w.localForDDL(unident(m[1]))
		if err != nil {
			return err
		}
		if c := unident(m[2]); !slices.Contains(tbl.cols, c) {
			tbl.cols = append(tbl.cols, c)
		}
		return nil
	}
	if m := reDropColumn.FindStringSubmatch(stmt); m != nil {
		tbl, err := w.localForDDL(unident(m[1]))
		if err != nil {
			return err
		}
		c := unident(m[2])
		tbl.cols = slices.DeleteFunc(tbl.cols, func(s string) bool { return s == c })
		for _, r := range tbl.rows {
			delete(r.vals, c)
		}
		return nil
	}
	if m := reDeleteRows.FindStringSubmatch(stmt); m != nil {
		tbl, err := w.localForDDL(unident(m[1]))
		if err != nil {
			return err
		}
		if want := map[bool]string{true: "2", false: "1"}[w.cluster]; m[3] != want {
			w.t.Errorf("mutations_sync = %s on cluster=%v, want %s", m[3], w.cluster, want)
		}
		keep, err := w.predicate(m[2])
		if err != nil {
			return err
		}
		tbl.rows = slices.DeleteFunc(tbl.rows, func(r tblRow) bool { return keep(r) })
		return nil
	}
	if m := reTruncate.FindStringSubmatch(stmt); m != nil {
		tbl, err := w.localForDDL(unident(m[1]))
		if err != nil {
			return err
		}
		tbl.rows = nil
		return nil
	}
	if m := reDropTable.FindStringSubmatch(stmt); m != nil {
		name := unident(m[1])
		if _, ok := w.tables[name]; ok && !strings.Contains(stmt, "SYNC") {
			w.t.Errorf("backing table dropped without SYNC: %s", stmt)
		}
		delete(w.tables, name)
		delete(w.dists, name)
		return nil
	}
	if m := reCreateDict.FindStringSubmatch(stmt); m != nil {
		w.dictObjs[unident(m[1])] = stmt
		return nil
	}
	if m := reDropDict.FindStringSubmatch(stmt); m != nil {
		delete(w.dictObjs, unident(m[1]))
		return nil
	}
	w.t.Errorf("unexpected ClickHouse statement: %s", stmt)
	return nil
}

// localForDDL is a local table for a DDL or mutation. A Distributed companion
// does not accept mutations.
func (w *world) localForDDL(name string) (*chTable, error) {
	if _, ok := w.dists[name]; ok {
		return nil, fakedb.Exception(48, "Table engine Distributed doesn't support mutations")
	}
	tbl, ok := w.tables[name]
	if !ok {
		return nil, fakedb.Exception(60, "Table "+name+" does not exist")
	}
	return tbl, nil
}

func (w *world) insert(stmt string) error {
	m := reInsert.FindStringSubmatch(stmt)
	name := unident(m[1])
	tbl, visible, viaDist, err := w.resolve(name)
	if err != nil {
		return err
	}
	if w.cluster && !viaDist {
		w.t.Errorf("rows written to the local table on a cluster: %s", stmt)
	}
	if viaDist && !strings.Contains(stmt, "distributed_foreground_insert = 1") {
		w.t.Errorf("Distributed insert is not foreground: %s", stmt)
	}
	var cols []string
	for _, c := range reIdent.FindAllStringSubmatch(m[2], -1) {
		cols = append(cols, unident(c[1]))
	}
	for _, c := range cols {
		if !slices.Contains(visible, c) {
			return fakedb.Exception(16, "No such column "+c+" in table "+name)
		}
	}
	execs := w.ch.Executed()
	args := w.ch.ExecArgs()
	if len(execs) == 0 || execs[len(execs)-1] != stmt {
		w.t.Errorf("insert arguments are not the last recorded Exec")
		return nil
	}
	vals := args[len(args)-1]
	if len(vals)%len(cols) != 0 {
		w.t.Errorf("insert binds %d values for %d columns", len(vals), len(cols))
		return nil
	}
	for i := 0; i < len(vals); i += len(cols) {
		r := tblRow{vals: map[string]string{}, shard: 1}
		for j, c := range cols {
			if c == seqColumn {
				r.seq = vals[i+j].(uint64)
				continue
			}
			r.vals[c] = vals[i+j].(string)
		}
		tbl.rows = append(tbl.rows, r)
	}
	return nil
}

// predicate compiles the WHERE clauses this package writes.
func (w *world) predicate(where string) (func(tblRow) bool, error) {
	where = strings.TrimSpace(where)
	if where == "" {
		return func(tblRow) bool { return true }, nil
	}
	if m := reWhereIn.FindStringSubmatch(where); m != nil {
		col := unident(m[1])
		keys := map[string]bool{}
		for _, q := range reQuoted.FindAllStringSubmatch(m[2], -1) {
			keys[unquote(q[1])] = true
		}
		below := uint64(0)
		if m[3] != "" {
			below, _ = strconv.ParseUint(m[3], 10, 64)
		}
		return func(r tblRow) bool { return keys[r.vals[col]] && (below == 0 || r.seq < below) }, nil
	}
	if m := reWhereSeqLt.FindStringSubmatch(where); m != nil {
		below, _ := strconv.ParseUint(m[1], 10, 64)
		return func(r tblRow) bool { return r.seq < below }, nil
	}
	if m := reWhereEq.FindStringSubmatch(where); m != nil {
		col, v := unident(m[1]), unquote(m[2])
		return func(r tblRow) bool { return r.vals[col] == v }, nil
	}
	if m := reWhereNotEmp.FindStringSubmatch(where); m != nil {
		col := unident(m[1])
		return func(r tblRow) bool { return r.vals[col] != "" }, nil
	}
	if m := reWhereSearch.FindStringSubmatch(where); m != nil {
		needle := strings.ToLower(unquote(m[2]))
		return func(r tblRow) bool {
			for _, v := range r.vals {
				if strings.Contains(strings.ToLower(v), needle) {
					return true
				}
			}
			return false
		}, nil
	}
	w.t.Errorf("unparsed WHERE: %s", where)
	return nil, fmt.Errorf("unparsed WHERE")
}

func (w *world) answerCH(q string, _ []any) (fakedb.CHResult, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case strings.HasPrefix(q, "SELECT count() FROM (SELECT lower("):
		groups, err := w.collisions(q)
		if err != nil {
			return fakedb.CHResult{Err: err}, true
		}
		return fakedb.Count(uint64(len(groups))), true
	case strings.HasPrefix(q, "SELECT lower("):
		groups, err := w.collisions(q)
		if err != nil {
			return fakedb.CHResult{Err: err}, true
		}
		res := fakedb.CHResult{Cols: []fakedb.Col{{Column: "k", Type: "String"}, {Column: "stored", Type: "Array(String)"}}}
		for _, g := range groups {
			res.Rows = append(res.Rows, []any{g[0], g[1:]})
		}
		return res, true
	case strings.Contains(q, "system.tables"):
		if !strings.Contains(q, "database = 'logs'") {
			w.t.Errorf("orphan scan misses the database: %s", q)
		}
		var names []string
		for _, objs := range []map[string]bool{keysSet(w.tables), keysSet(w.dists), keysSet(w.dictObjs)} {
			for n := range objs {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		return fakedb.Strs("name", names...), true
	}

	m := reSelect.FindStringSubmatch(q)
	if m == nil {
		w.t.Errorf("unexpected ClickHouse query: %s", q)
		return fakedb.CHResult{}, true
	}
	proj, name, isFinal, where, order := m[1], unident(m[2]), m[3] != "", m[4], m[5]
	tbl, visible, _, err := w.resolve(name)
	if err != nil {
		return fakedb.CHResult{Err: err}, true
	}
	if w.cluster && !strings.HasSuffix(name, "_distributed") {
		w.t.Errorf("rows read from the local table on a cluster: %s", q)
	}
	keyCol := w.keyColumnOf(name)
	rows := tbl.rows
	if isFinal {
		rows = w.final(tbl, keyCol)
	}
	keep, err := w.predicate(where)
	if err != nil {
		return fakedb.CHResult{Err: err}, true
	}
	var matched []tblRow
	for _, r := range rows {
		if keep(r) {
			matched = append(matched, r)
		}
	}

	switch proj {
	case "count()":
		return fakedb.Count(uint64(len(matched))), true
	case "count() AS n":
		return fakedb.One("n", "UInt64", uint64(len(matched))), true
	case "max(`_bf_seq`)":
		var hi uint64
		for _, r := range matched {
			hi = max(hi, r.seq)
		}
		return fakedb.One("max(_bf_seq)", "UInt64", hi), true
	}

	distinct := strings.HasPrefix(proj, "DISTINCT ")
	proj = strings.TrimPrefix(proj, "DISTINCT ")
	var cols []fakedb.Col
	var names []string
	for _, p := range strings.Split(proj, ", ") {
		p = strings.TrimSpace(p)
		switch {
		case p == "_shard_num":
			cols, names = append(cols, fakedb.Col{Column: "_shard_num", Type: "UInt32"}), append(names, p)
		case strings.Contains(p, " AS "):
			parts := strings.SplitN(p, " AS ", 2)
			c := unident(strings.Trim(parts[0], "`"))
			cols, names = append(cols, fakedb.Col{Column: parts[1], Type: "String"}), append(names, c)
		default:
			c := unident(strings.Trim(p, "`"))
			if !slices.Contains(visible, c) {
				return fakedb.CHResult{Err: fakedb.Exception(47, "Missing column "+c)}, true
			}
			typ := "String"
			if c == seqColumn {
				typ = "UInt64"
			}
			cols, names = append(cols, fakedb.Col{Column: c, Type: typ}), append(names, c)
		}
	}
	if order != "" {
		sort.SliceStable(matched, func(i, j int) bool {
			if matched[i].seq != matched[j].seq {
				return matched[i].seq < matched[j].seq
			}
			return matched[i].vals[keyCol] < matched[j].vals[keyCol]
		})
	}
	if m[6] != "" {
		limit, _ := strconv.Atoi(m[6])
		offset, _ := strconv.Atoi(m[7])
		matched = matched[min(offset, len(matched)):]
		matched = matched[:min(limit, len(matched))]
	}
	res := fakedb.CHResult{Cols: cols}
	seen := map[string]bool{}
	for _, r := range matched {
		vals := make([]any, len(names))
		for i, n := range names {
			switch n {
			case seqColumn:
				vals[i] = r.seq
			case "_shard_num":
				vals[i] = r.shard
			default:
				vals[i] = r.vals[n]
			}
		}
		if distinct {
			k := fmt.Sprint(vals...)
			if seen[k] {
				continue
			}
			seen[k] = true
		}
		res.Rows = append(res.Rows, vals)
	}
	return res, true
}

// keyColumnOf finds the key column a table is sorted by, from Postgres.
func (w *world) keyColumnOf(table string) string {
	for _, d := range w.dicts {
		if chTableName(d.id) == table || chDistTableName(d.id) == table {
			return d.keyCol
		}
	}
	return ""
}

// collisions groups a column's FINAL values by lower(), keeping groups with more
// than one distinct spelling: [lowered, spellings...].
func (w *world) collisions(q string) ([][]string, error) {
	m := reLower.FindStringSubmatch(q)
	if m == nil {
		w.t.Errorf("unparsed collision query: %s", q)
		return nil, nil
	}
	name, col := unident(m[1]), unident(m[2])
	tbl, _, _, err := w.resolve(name)
	if err != nil {
		return nil, err
	}
	spellings := map[string][]string{}
	for _, r := range w.final(tbl, w.keyColumnOf(name)) {
		v := r.vals[col]
		if v == "" {
			continue
		}
		k := strings.ToLower(v)
		if !slices.Contains(spellings[k], v) {
			spellings[k] = append(spellings[k], v)
		}
	}
	var out [][]string
	for k, s := range spellings {
		if len(s) > 1 {
			sort.Strings(s)
			out = append(out, append([]string{k}, s...))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out, nil
}

func keysSet[V any](m map[string]V) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}
