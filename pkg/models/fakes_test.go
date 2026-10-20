package models

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// An in-memory analytics_models table behind the scripted Postgres driver, and a
// ClickHouse fake that knows which tables exist from the DDL it was sent. The
// Postgres side answers only the statement shapes this package issues, applying
// each one's WHERE and SET the way Postgres would; any other statement fails the
// test, so a query that drifts from what the fake models cannot pass by accident.

const (
	fracA = "aaaaaaaa-0000-0000-0000-000000000001"
	fracB = "bbbbbbbb-0000-0000-0000-000000000002"
)

// modelRow is one analytics_models row.
type modelRow struct {
	id, fractalID, prismID, name, description, modelType, definition         string
	chTable, chMV, status, alertMode, linkedAlertID, errorMessage, createdBy string
	createdAt, updatedAt                                                     time.Time

	bfStatus, bfWindow    string
	bfTotal, bfDone       int64
	bfStartedAt, bfAnchor *time.Time
	bfFrom, bfTo          *time.Time
	bfError               string

	watermark, lastScored *time.Time
}

// alertRow is the part of an alerts row the model columns subselect.
type alertRow struct {
	enabled       bool
	lastTriggered *time.Time
}

type modelDB struct {
	t      testing.TB
	pg     *fakedb.PG
	client *storage.PostgresClient

	mu      sync.Mutex
	rows    map[string]*modelRow
	order   []string // insertion order, the order an unordered scan returns
	alerts  map[string]*alertRow
	seq     int
	unknown []string

	// lockHeld makes pg_try_advisory_lock report another replica holding it.
	lockHeld bool
	// fail fails every statement containing the key.
	fail map[string]error
	// hook, set before any goroutine starts, sees every statement first.
	hook func(stmt string)
}

func newModelDB(t testing.TB) *modelDB {
	client, pg := storagetest.Postgres()
	db := &modelDB{t: t, pg: pg, client: client, rows: map[string]*modelRow{},
		alerts: map[string]*alertRow{}, fail: map[string]error{}}
	pg.Respond(db.respond)
	t.Cleanup(func() {
		db.mu.Lock()
		defer db.mu.Unlock()
		for _, q := range db.unknown {
			t.Errorf("statement the fake does not model: %s", q)
		}
	})
	return db
}

func defJSON(def ModelDefinition) string {
	b, err := json.Marshal(def)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// add stores a model, defaulting what a test leaves out to an active model
// with its table recorded.
func (db *modelDB) add(r modelRow) *modelRow {
	db.mu.Lock()
	defer db.mu.Unlock()
	if r.id == "" {
		db.seq++
		r.id = fmt.Sprintf("00000000-0000-0000-0000-%012d", db.seq)
	}
	if r.fractalID == "" {
		r.fractalID = fracA
	}
	if r.status == "" {
		r.status = "active"
	}
	if r.chTable == "" {
		r.chTable = chModelTableName(r.id)
	}
	if r.bfStatus == "" {
		r.bfStatus = "none"
	}
	if r.alertMode == "" {
		r.alertMode = "none"
	}
	if r.createdAt.IsZero() {
		r.createdAt = time.Date(2026, 9, 1, 0, 0, 0, int(db.seq), time.UTC)
	}
	if r.updatedAt.IsZero() {
		r.updatedAt = r.createdAt
	}
	if r.definition == "" {
		r.definition = "{}"
	}
	row := r
	db.rows[r.id] = &row
	db.order = append(db.order, r.id)
	return &row
}

// get returns a copy of a row.
func (db *modelDB) get(id string) (modelRow, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	r, ok := db.rows[id]
	if !ok {
		return modelRow{}, false
	}
	return *r, true
}

func (db *modelDB) setLockHeld(v bool) {
	db.mu.Lock()
	db.lockHeld = v
	db.mu.Unlock()
}

func (db *modelDB) failOn(sub string, err error) {
	db.mu.Lock()
	db.fail[sub] = err
	db.mu.Unlock()
}

func tval(p *time.Time) driver.Value {
	if p == nil {
		return nil
	}
	return *p
}

func tptr(t time.Time) *time.Time { return &t }

// sameTime is IS NOT DISTINCT FROM between a stored timestamp and an argument.
func sameTime(stored *time.Time, arg driver.Value) bool {
	t, ok := arg.(time.Time)
	if stored == nil || !ok {
		return stored == nil && arg == nil
	}
	return stored.Equal(t)
}

// columns renders a row in modelColumns order.
func (db *modelDB) columns(r *modelRow) []driver.Value {
	var enabled, last driver.Value
	if a, ok := db.alerts[r.linkedAlertID]; ok && r.linkedAlertID != "" {
		enabled, last = a.enabled, tval(a.lastTriggered)
	}
	return []driver.Value{
		r.id, r.fractalID, r.prismID, r.name, r.description, r.modelType, []byte(r.definition),
		r.chTable, r.chMV, r.status, r.alertMode, r.linkedAlertID, r.errorMessage, r.createdBy,
		r.createdAt, r.updatedAt, r.bfStatus, r.bfWindow, r.bfTotal, r.bfDone,
		tval(r.bfStartedAt), tval(r.bfFrom), tval(r.bfTo), r.bfError, tval(r.watermark), enabled, last,
	}
}

func norm(q string) string { return strings.Join(strings.Fields(q), " ") }

var modelCols = norm(modelColumns)

func str(v driver.Value) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	case nil:
		return ""
	}
	return fmt.Sprint(v)
}

// pqArray reads the text form pq.Array binds a string slice as.
func pqArray(v driver.Value) []string {
	s := strings.Trim(str(v), "{}")
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		out = append(out, strings.Trim(p, `"`))
	}
	return out
}

func (db *modelDB) sorted(less func(a, b *modelRow) bool) []*modelRow {
	out := make([]*modelRow, 0, len(db.order))
	for _, id := range db.order {
		if r, ok := db.rows[id]; ok {
			out = append(out, r)
		}
	}
	if less != nil {
		sort.SliceStable(out, func(i, j int) bool { return less(out[i], out[j]) })
	}
	return out
}

func byCreated(a, b *modelRow) bool {
	if !a.createdAt.Equal(b.createdAt) {
		return a.createdAt.Before(b.createdAt)
	}
	return a.id < b.id
}

func affected(n int) fakedb.PGResult { return fakedb.Affected(int64(n)) }

func (db *modelDB) respond(q string, args []driver.Value) (fakedb.PGResult, bool) {
	n := norm(q)
	if db.hook != nil {
		db.hook(n)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for sub, err := range db.fail {
		if strings.Contains(n, sub) {
			return fakedb.PGResult{Err: err}, true
		}
	}
	arg := func(i int) string { return str(args[i]) }
	row := func(i int) *modelRow { return db.rows[arg(i)] }
	now := time.Now().UTC()

	switch n {
	case "SELECT pg_try_advisory_lock($1)":
		return fakedb.Row(!db.lockHeld), true
	case "SELECT pg_advisory_unlock($1)":
		return fakedb.Row(true), true

	case "SELECT " + modelCols + " FROM analytics_models WHERE fractal_id = $1 ORDER BY name":
		var res fakedb.PGResult
		for _, r := range db.sorted(func(a, b *modelRow) bool { return a.name < b.name }) {
			if r.fractalID == arg(0) {
				res.Rows = append(res.Rows, db.columns(r))
			}
		}
		return res, true
	case "SELECT " + modelCols + " FROM analytics_models WHERE id = $1":
		if r := row(0); r != nil {
			return fakedb.PGResult{Rows: [][]driver.Value{db.columns(r)}}, true
		}
		return fakedb.PGResult{}, true

	case "SELECT id, name, model_type, definition FROM analytics_models WHERE fractal_id = $1 AND status = 'active'":
		var res fakedb.PGResult
		for _, r := range db.sorted(nil) {
			if r.fractalID == arg(0) && r.status == "active" {
				res.Rows = append(res.Rows, []driver.Value{r.id, r.name, r.modelType, []byte(r.definition)})
			}
		}
		return res, true
	case "SELECT id, name, model_type, definition, fractal_id FROM analytics_models WHERE fractal_id = ANY($1) AND status = 'active' ORDER BY created_at ASC, id ASC":
		in := setOf(pqArray(args[0]))
		var res fakedb.PGResult
		for _, r := range db.sorted(byCreated) {
			if in[r.fractalID] && r.status == "active" {
				res.Rows = append(res.Rows, []driver.Value{r.id, r.name, r.modelType, []byte(r.definition), r.fractalID})
			}
		}
		return res, true
	case "SELECT id, name, definition, COALESCE(fractal_id::text,'') FROM analytics_models WHERE fractal_id = ANY($1) AND status = 'active' AND model_type = $2 ORDER BY created_at ASC, id ASC":
		in := setOf(pqArray(args[0]))
		var res fakedb.PGResult
		for _, r := range db.sorted(byCreated) {
			if in[r.fractalID] && r.status == "active" && r.modelType == arg(1) {
				res.Rows = append(res.Rows, []driver.Value{r.id, r.name, []byte(r.definition), r.fractalID})
			}
		}
		return res, true

	case "INSERT INTO analytics_models (fractal_id, name, description, model_type, definition, ch_table_name, ch_mv_name, status, alert_mode, created_by) VALUES ($1, $2, $3, $4, $5, '', '', 'rebuilding', $6, $7) RETURNING id":
		db.seq++
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", db.seq)
		r := &modelRow{id: id, fractalID: arg(0), name: arg(1), description: arg(2), modelType: arg(3),
			definition: arg(4), status: "rebuilding", alertMode: arg(5), createdBy: arg(6),
			createdAt: now, updatedAt: now, bfStatus: "none"}
		db.rows[id] = r
		db.order = append(db.order, id)
		return fakedb.Row(id), true
	case "UPDATE analytics_models SET ch_table_name = $1, ch_mv_name = $2 WHERE id = $3":
		return db.update(row(2), func(r *modelRow) { r.chTable, r.chMV = arg(0), arg(1) }), true
	case "DELETE FROM analytics_models WHERE id = $1":
		if r := row(0); r != nil {
			delete(db.rows, r.id)
			return affected(1), true
		}
		return affected(0), true
	case "UPDATE analytics_models SET linked_alert_id = $1::uuid WHERE id = $2":
		return db.update(row(1), func(r *modelRow) { r.linkedAlertID = arg(0) }), true
	case "UPDATE analytics_models SET status='error', error_message=$1 WHERE id=$2":
		return db.update(row(1), func(r *modelRow) { r.status, r.errorMessage = "error", arg(0) }), true
	case "UPDATE analytics_models SET status='active', error_message='' WHERE id=$1":
		return db.update(row(0), func(r *modelRow) { r.status, r.errorMessage = "active", "" }), true
	case "UPDATE analytics_models SET name=$1, description=$2, definition=$3, updated_at=NOW() WHERE id=$4":
		return db.update(row(3), func(r *modelRow) {
			r.name, r.description, r.definition, r.updatedAt = arg(0), arg(1), arg(2), now
		}), true
	case "UPDATE analytics_models SET status='rebuilding' WHERE id=$1":
		return db.update(row(0), func(r *modelRow) { r.status = "rebuilding" }), true
	case "UPDATE analytics_models SET alert_mode=$1, updated_at=NOW() WHERE id=$2":
		return db.update(row(1), func(r *modelRow) { r.alertMode, r.updatedAt = arg(0), now }), true
	case "UPDATE analytics_models SET state_watermark = COALESCE(state_watermark, NOW()), backfill_anchor = COALESCE(state_watermark, NOW()), backfill_status = 'none', backfill_window = '', backfill_total = 0, backfill_done = 0, backfill_started_at = NULL, backfill_from = NULL, backfill_to = NULL, backfill_error = '' WHERE id = $1":
		return db.update(row(0), func(r *modelRow) {
			// Every SET reads the row as it stood before the statement.
			wm := now
			if r.watermark != nil {
				wm = *r.watermark
			}
			r.watermark, r.bfAnchor = tptr(wm), tptr(wm)
			r.bfStatus, r.bfWindow, r.bfTotal, r.bfDone, r.bfStartedAt, r.bfError = "none", "", 0, 0, nil, ""
			r.bfFrom, r.bfTo = nil, nil
		}), true
	case "UPDATE analytics_models SET state_watermark = COALESCE(state_watermark, NOW()) WHERE id = $1":
		return db.update(row(0), func(r *modelRow) {
			if r.watermark == nil {
				r.watermark = tptr(now)
			}
		}), true
	case "SELECT id, COALESCE(fractal_id::text,''), model_type, definition, ch_table_name, ch_mv_name FROM analytics_models":
		var res fakedb.PGResult
		for _, r := range db.sorted(nil) {
			res.Rows = append(res.Rows, []driver.Value{r.id, r.fractalID, r.modelType, r.definition, r.chTable, r.chMV})
		}
		return res, true
	case "UPDATE analytics_models SET ch_mv_name = '', state_watermark = CASE WHEN id::text = ANY($2) THEN NOW() ELSE COALESCE(state_watermark, NOW()) END WHERE (COALESCE(ch_mv_name, '') <> '' OR state_watermark IS NULL OR id::text = ANY($2)) AND NOT (COALESCE(ch_mv_name, '') = ANY($1))":
		survived, dropped := setOf(pqArray(args[0])), setOf(pqArray(args[1]))
		k := 0
		for _, r := range db.rows {
			if (r.chMV != "" || r.watermark == nil || dropped[r.id]) && !survived[r.chMV] {
				r.chMV = ""
				if r.watermark == nil || dropped[r.id] {
					r.watermark = tptr(now)
				}
				k++
			}
		}
		return affected(k), true

	case "UPDATE analytics_models SET backfill_status='running', backfill_window=$1, backfill_total=$2, backfill_done=0, backfill_anchor=COALESCE(backfill_anchor, created_at), backfill_started_at=$3, backfill_from=$4, backfill_to=$5, backfill_error='' WHERE id=$6 AND status='active' AND backfill_status IN ('none','')":
		r := row(5)
		if r == nil || r.status != "active" || (r.bfStatus != "none" && r.bfStatus != "") {
			return affected(0), true
		}
		r.bfStatus, r.bfWindow, r.bfTotal, r.bfDone, r.bfError = "running", arg(0), args[1].(int64), 0, ""
		if r.bfAnchor == nil {
			r.bfAnchor = tptr(r.createdAt)
		}
		r.bfStartedAt = tptr(args[2].(time.Time))
		r.bfFrom, r.bfTo = tptr(args[3].(time.Time)), tptr(args[4].(time.Time))
		return affected(1), true
	case "UPDATE analytics_models SET backfill_status='running', backfill_error='' WHERE id=$1 AND status='active' AND backfill_status IN ('failed','cancelled') RETURNING backfill_started_at":
		r := row(0)
		if r == nil || r.status != "active" || (r.bfStatus != "failed" && r.bfStatus != "cancelled") {
			return fakedb.PGResult{}, true
		}
		r.bfStatus, r.bfError = "running", ""
		return fakedb.Row(tval(r.bfStartedAt)), true
	case "SELECT backfill_from, backfill_to, backfill_anchor, backfill_started_at, backfill_done FROM analytics_models WHERE id=$1":
		if r := row(0); r != nil {
			return fakedb.Row(tval(r.bfFrom), tval(r.bfTo), tval(r.bfAnchor), tval(r.bfStartedAt), r.bfDone), true
		}
		return fakedb.PGResult{}, true
	case "UPDATE analytics_models SET backfill_done=$1 WHERE id=$2 AND backfill_status='running' AND backfill_started_at IS NOT DISTINCT FROM $3":
		if r := row(1); r != nil && r.bfStatus == "running" && sameTime(r.bfStartedAt, args[2]) {
			return db.update(r, func(r *modelRow) { r.bfDone = args[0].(int64) }), true
		}
		return affected(0), true
	case "UPDATE analytics_models SET backfill_status=$1, backfill_error=$2 WHERE id=$3 AND backfill_status='running' AND backfill_started_at IS NOT DISTINCT FROM $4":
		if r := row(2); r != nil && r.bfStatus == "running" && sameTime(r.bfStartedAt, args[3]) {
			return db.update(r, func(r *modelRow) { r.bfStatus, r.bfError = arg(0), arg(1) }), true
		}
		return affected(0), true
	case "SELECT id FROM analytics_models WHERE backfill_status='running'":
		var res fakedb.PGResult
		for _, r := range db.sorted(nil) {
			if r.bfStatus == "running" {
				res.Rows = append(res.Rows, []driver.Value{r.id})
			}
		}
		return res, true

	case norm(dueModelsQuery):
		var res fakedb.PGResult
		for _, r := range db.sorted(nil) {
			if r.status == "active" && r.chTable != "" && r.watermark != nil {
				res.Rows = append(res.Rows, []driver.Value{r.id, r.name, r.fractalID, r.modelType, []byte(r.definition), r.chTable, *r.watermark})
			}
		}
		return res, true
	case "UPDATE analytics_models SET error_message = $1 WHERE id = $2":
		return db.update(row(1), func(r *modelRow) { r.errorMessage = arg(0) }), true
	case "UPDATE analytics_models SET state_watermark = $1 WHERE id = $2":
		return db.update(row(1), func(r *modelRow) { r.watermark = tptr(args[0].(time.Time)) }), true

	case "SELECT id, COALESCE(fractal_id::text,''), model_type, definition, ch_table_name, last_scored_at FROM analytics_models WHERE status = 'active' AND model_type IN ('beacon','long_connection')":
		var res fakedb.PGResult
		for _, r := range db.sorted(nil) {
			if r.status == "active" && (r.modelType == string(ModelTypeBeacon) || r.modelType == string(ModelTypeLongConnection)) {
				res.Rows = append(res.Rows, []driver.Value{r.id, r.fractalID, r.modelType, []byte(r.definition), r.chTable, tval(r.lastScored)})
			}
		}
		return res, true
	case "UPDATE analytics_models SET last_scored_at = NOW() WHERE id = $1":
		return db.update(row(0), func(r *modelRow) { r.lastScored = tptr(now) }), true
	}
	db.unknown = append(db.unknown, n)
	return fakedb.PGResult{Err: fmt.Errorf("modelDB: unmodelled statement")}, true
}

func (db *modelDB) update(r *modelRow, set func(*modelRow)) fakedb.PGResult {
	if r == nil {
		return affected(0)
	}
	set(r)
	return affected(1)
}

func setOf(xs []string) map[string]bool {
	out := map[string]bool{}
	for _, x := range xs {
		out[x] = true
	}
	return out
}

// ---- ClickHouse ----

// chFake is the scripted ClickHouse connection plus the tables its DDL created.
type chFake struct {
	*fakedb.CH
	client *storage.ClickHouseClient

	mu sync.Mutex
	// views answers the sweep for leftover insert-time views.
	views []string
	// columns lists the columns system.columns reports, per table, beyond what an
	// ALTER added. A table not listed has every column.
	columns map[string][]string
	// blankRows lists the first_seen tables holding the blank entity.
	blankRows map[string]bool
	// existing are tables that were there before the test sent any DDL.
	existing map[string]bool
}

var (
	createTableRe = regexp.MustCompile("^CREATE TABLE IF NOT EXISTS `([^`]+)`")
	dropTableRe   = regexp.MustCompile("^DROP TABLE IF EXISTS `([^`]+)`")
	addColumnRe   = regexp.MustCompile("^ALTER TABLE `([^`]+)` ADD COLUMN IF NOT EXISTS `([^`]+)`")
)

func newCH(t testing.TB) *chFake {
	client, ch := storagetest.ClickHouse()
	return wireCH(client, ch)
}

// clusterTopo is a self-managed cluster: replicated engines, ON CLUSTER DDL and
// Distributed companions.
func clusterTopo(t testing.TB) storage.Topology {
	topo, err := storage.DeriveTopology(storage.TopologySpec{Cluster: "bf_cluster"})
	if err != nil {
		t.Fatal(err)
	}
	return topo
}

func newClusterCH(t testing.TB) *chFake {
	client, ch := storagetest.ClickHouseWith(clusterTopo(t))
	return wireCH(client, ch)
}

func wireCH(client *storage.ClickHouseClient, ch *fakedb.CH) *chFake {
	f := &chFake{CH: ch, client: client, columns: map[string][]string{}, existing: map[string]bool{}}
	ch.Respond(f.respond)
	return f
}

// tables replays the DDL sent so far into the set of tables that exist.
func (f *chFake) tables() map[string]bool {
	out := map[string]bool{}
	f.mu.Lock()
	for t := range f.existing {
		out[t] = true
	}
	f.mu.Unlock()
	for _, s := range f.Executed() {
		if m := createTableRe.FindStringSubmatch(s); m != nil {
			out[m[1]] = true
		}
		if m := dropTableRe.FindStringSubmatch(s); m != nil {
			delete(out, m[1])
		}
	}
	return out
}

func (f *chFake) hasColumn(table, col string) bool {
	for _, s := range f.Executed() {
		if m := addColumnRe.FindStringSubmatch(s); m != nil && m[1] == table && m[2] == col {
			return true
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cols, listed := f.columns[table]
	if !listed {
		return true
	}
	for _, c := range cols {
		if c == col {
			return true
		}
	}
	return false
}

func (f *chFake) respond(q string, args []any) (fakedb.CHResult, bool) {
	switch {
	case strings.Contains(q, "FROM system.tables WHERE database = currentDatabase() AND name = ?"):
		if f.tables()[fmt.Sprint(args[0])] {
			return fakedb.Count(1), true
		}
		return fakedb.Count(0), true
	case strings.Contains(q, "FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = ?"):
		if f.hasColumn(fmt.Sprint(args[0]), fmt.Sprint(args[1])) {
			return fakedb.Count(1), true
		}
		return fakedb.Count(0), true
	case strings.Contains(q, "SELECT count() FROM `") && strings.Contains(q, "WHERE entity_key = "):
		f.mu.Lock()
		defer f.mu.Unlock()
		for tbl := range f.blankRows {
			if strings.Contains(q, "`"+tbl+"`") {
				return fakedb.Count(1), true
			}
		}
		return fakedb.Count(0), true
	case strings.Contains(q, "engine = 'MaterializedView'"):
		f.mu.Lock()
		defer f.mu.Unlock()
		return fakedb.Strs("name", f.views...), true
	}
	return fakedb.CHResult{}, false
}

// cols builds result columns from name, type pairs.
func cols(spec ...string) []fakedb.Col {
	out := make([]fakedb.Col, 0, len(spec)/2)
	for i := 0; i+1 < len(spec); i += 2 {
		out = append(out, fakedb.Col{Column: spec[i], Type: spec[i+1]})
	}
	return out
}

// fakeEnv is a manager over both fakes.
type fakeEnv struct {
	db  *modelDB
	ch  *chFake
	mgr *Manager
}

func newEnv(t testing.TB) *fakeEnv { return envOver(t, newCH(t)) }

func newClusterEnv(t testing.TB) *fakeEnv { return envOver(t, newClusterCH(t)) }

func envOver(t testing.TB, ch *chFake) *fakeEnv {
	db := newModelDB(t)
	m := NewManager(db.client, ch.client)
	m.bfCfg.chunkSleepMs = 0
	return &fakeEnv{db: db, ch: ch, mgr: m}
}

// waitFor polls until cond holds, for the work this package hands to a goroutine.
func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// recordingAlerts is a LinkedAlertManager that remembers every call.
type recordingAlerts struct {
	mu        sync.Mutex
	created   []LinkedAlertSpec
	updated   map[string]LinkedAlertSpec
	deleted   []string
	enabled   map[string]bool
	createErr error
	setErr    error
	nextID    string
}

func newRecordingAlerts() *recordingAlerts {
	return &recordingAlerts{updated: map[string]LinkedAlertSpec{}, enabled: map[string]bool{}, nextID: "alert-1"}
}

func (a *recordingAlerts) CreateLinkedAlert(_ context.Context, spec LinkedAlertSpec) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.createErr != nil {
		return "", a.createErr
	}
	a.created = append(a.created, spec)
	return a.nextID, nil
}

func (a *recordingAlerts) UpdateLinkedAlert(_ context.Context, id string, spec LinkedAlertSpec) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.updated[id] = spec
	return nil
}

func (a *recordingAlerts) DeleteLinkedAlert(_ context.Context, id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deleted = append(a.deleted, id)
	return nil
}

func (a *recordingAlerts) SetLinkedAlertEnabled(_ context.Context, id string, on bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.setErr != nil {
		return a.setErr
	}
	a.enabled[id] = on
	return nil
}
