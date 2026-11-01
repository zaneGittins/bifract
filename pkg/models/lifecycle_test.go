package models

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// Model lifecycle through the fakes: what Create, Update, Delete and the startup
// reconcilers leave in Postgres and send to ClickHouse.

var rarityDef = ModelDefinition{PartitionKey: "host", ValueKey: "proc", Alert: &AlertConfig{Severity: "high", CoverageThreshold: 0.9}}

func createAndWait(t *testing.T, env *fakeEnv, req CreateRequest) modelRow {
	t.Helper()
	mo, err := env.mgr.Create(context.Background(), fracA, req, "ana")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	var r modelRow
	waitFor(t, "the async ClickHouse build", func() bool {
		r, _ = env.db.get(mo.ID)
		return r.status != "rebuilding"
	})
	return r
}

// stmtsFor returns the executed statements that name a table.
func stmtsFor(ch *chFake, table string) []string {
	return ch.ExecutedMatching("`" + table + "`")
}

func TestCreateSingleNode(t *testing.T) {
	env := newEnv(t)
	alerts := newRecordingAlerts()
	env.mgr.SetAlertManager(alerts)

	r := createAndWait(t, env, CreateRequest{Name: "rare procs", ModelType: ModelTypeRarity, Definition: rarityDef, AlertMode: "paused"})
	if r.status != "active" || r.errorMessage != "" {
		t.Fatalf("status %q %q", r.status, r.errorMessage)
	}
	if r.chTable != chModelTableName(r.id) || r.chMV != chModelMVName(r.id) || r.createdBy != "ana" {
		t.Errorf("names: %+v", r)
	}
	// Live state takes over at creation, and the backfill owns everything before it.
	if r.watermark == nil || r.bfAnchor == nil || !r.watermark.Equal(*r.bfAnchor) || r.bfStatus != "none" {
		t.Errorf("watermark %v anchor %v status %q", r.watermark, r.bfAnchor, r.bfStatus)
	}

	if r.linkedAlertID != "alert-1" || len(alerts.created) != 1 {
		t.Fatalf("linked alert: %q %+v", r.linkedAlertID, alerts.created)
	}
	spec := alerts.created[0]
	if spec.Enabled || spec.Severity != "high" || spec.FractalID != fracA || spec.CreatedBy != "ana" ||
		!strings.Contains(spec.Name, "rare procs (model ") || spec.QueryString == "" {
		t.Errorf("alert spec: %+v", spec)
	}

	execs := env.ch.Executed()
	if len(execs) != 2 {
		t.Fatalf("single node runs the table and the view sweep, got %d:\n%s", len(execs), strings.Join(execs, "\n"))
	}
	if !strings.HasPrefix(execs[0], "CREATE TABLE IF NOT EXISTS `"+r.chTable+"`") ||
		!strings.Contains(execs[0], "ENGINE = AggregatingMergeTree()") || strings.Contains(execs[0], "ON CLUSTER") {
		t.Errorf("table DDL: %s", execs[0])
	}
	if execs[1] != "DROP VIEW IF EXISTS `"+r.chMV+"`" {
		t.Errorf("a leftover insert-time view must be dropped: %s", execs[1])
	}
}

// On a cluster every object goes out ON CLUSTER with a replicated engine, and the
// Distributed companion reads and writes go through exists on every node too.
func TestCreateCluster(t *testing.T) {
	env := newClusterEnv(t)
	r := createAndWait(t, env, CreateRequest{Name: "rare", ModelType: ModelTypeRarity, Definition: rarityDef})
	if r.status != "active" {
		t.Fatalf("status %q %q", r.status, r.errorMessage)
	}
	execs := env.ch.Executed()
	if len(execs) != 3 {
		t.Fatalf("want table, view sweep, distributed companion; got:\n%s", strings.Join(execs, "\n"))
	}
	on := " ON CLUSTER 'bf_cluster'"
	if !strings.HasPrefix(execs[0], "CREATE TABLE IF NOT EXISTS `"+r.chTable+"`"+on) || !strings.Contains(execs[0], "ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')") {
		t.Errorf("table: %s", execs[0])
	}
	if execs[1] != "DROP VIEW IF EXISTS `"+r.chMV+"`"+on {
		t.Errorf("view sweep: %s", execs[1])
	}
	dist := chModelDistName(r.id)
	want := "CREATE TABLE IF NOT EXISTS `" + dist + "`" + on + " AS `" + r.chTable + "` ENGINE = Distributed('bf_cluster', currentDatabase(), '" + r.chTable + "', rand())"
	if execs[2] != want {
		t.Errorf("distributed companion:\n got %s\nwant %s", execs[2], want)
	}
}

// A network model owns a rolling-state table and a results table, each with a
// Distributed companion on a cluster. Its watermark is seeded without touching
// backfill state: it has none.
func TestCreateNetworkModel(t *testing.T) {
	env := newClusterEnv(t)
	r := createAndWait(t, env, CreateRequest{Name: "beacons", ModelType: ModelTypeBeacon, Definition: ModelDefinition{Window: "7d"}})
	if r.status != "active" || r.watermark == nil || r.bfAnchor != nil {
		t.Fatalf("row: %+v", r)
	}
	state, dist, distState := chModelStateName(r.id), chModelDistName(r.id), chModelStateDistName(r.id)
	created := env.ch.tables()
	for _, tbl := range []string{state, r.chTable, dist, distState} {
		if !created[tbl] {
			t.Errorf("%s not created; have %v", tbl, created)
		}
	}
	stateDDL := stmtsFor(env.ch, state)[0]
	if !strings.Contains(stateDDL, "TTL hour + INTERVAL 9 DAY") || !strings.Contains(stateDDL, "ReplicatedAggregatingMergeTree") {
		t.Errorf("a 7d window keeps 9 days of state: %s", stateDDL)
	}
	for _, s := range env.ch.Executed() {
		if !strings.Contains(s, "ON CLUSTER 'bf_cluster'") {
			t.Errorf("not ON CLUSTER: %s", s)
		}
	}
	if q := env.ch.ExecutedMatching("ENGINE = Distributed('bf_cluster', currentDatabase(), '" + state + "'"); len(q) != 1 || !strings.Contains(q[0], "`"+distState+"`") {
		t.Errorf("state companion: %v", q)
	}
}

// A results table that cannot be created takes the state table with it, and the
// model reports the error instead of sitting half-built.
func TestCreateNetworkModelFailureCleansUp(t *testing.T) {
	env := newEnv(t)
	env.ch.FailExecFunc(func(s string) error {
		if strings.Contains(s, "regularity_score") && strings.HasPrefix(s, "CREATE TABLE") {
			return errors.New("disk full")
		}
		return nil
	})
	r := createAndWait(t, env, CreateRequest{Name: "lc", ModelType: ModelTypeLongConnection})
	if r.status != "error" || !strings.Contains(r.errorMessage, "create results table") || !strings.Contains(r.errorMessage, "disk full") {
		t.Errorf("row: %q %q", r.status, r.errorMessage)
	}
	state := chModelStateName(r.id)
	if len(env.ch.ExecutedMatching("DROP TABLE IF EXISTS `"+state+"` SYNC")) != 1 {
		t.Errorf("state table left behind: %v", env.ch.Executed())
	}
	if r.watermark != nil {
		t.Error("a model that never built must not be handed to state maintenance")
	}
}

func TestCreateTableFailureMarksError(t *testing.T) {
	env := newEnv(t)
	env.ch.FailExec("CREATE TABLE", errors.New("no space"))
	r := createAndWait(t, env, CreateRequest{Name: "r", ModelType: ModelTypeRarity, Definition: rarityDef})
	if r.status != "error" || !strings.Contains(r.errorMessage, "no space") || r.watermark != nil {
		t.Errorf("row: %+v", r)
	}
}

// ON CLUSTER DDL that outlives the distributed DDL timeout (code 159) is still
// queued on every node, so it is a warning and the model goes active.
func TestCreateToleratesDDLTimeout(t *testing.T) {
	env := newClusterEnv(t)
	env.ch.FailExec("CREATE TABLE", fakedb.Exception(159, "Watching task is executing longer than distributed_ddl_task_timeout"))
	r := createAndWait(t, env, CreateRequest{Name: "r", ModelType: ModelTypeRarity, Definition: rarityDef})
	if r.status != "active" {
		t.Errorf("status %q %q", r.status, r.errorMessage)
	}
}

// A failure recording the ClickHouse names removes the row it just inserted.
func TestCreateRollsBackOnNameWriteFailure(t *testing.T) {
	env := newEnv(t)
	env.db.failOn("SET ch_table_name", errors.New("pg down"))
	if _, err := env.mgr.Create(context.Background(), fracA, CreateRequest{Name: "r", ModelType: ModelTypeRarity, Definition: rarityDef}, "ana"); err == nil {
		t.Fatal("expected an error")
	}
	if n := len(env.db.sorted(nil)); n != 0 {
		t.Errorf("%d rows left behind", n)
	}
	if len(env.ch.Executed()) != 0 {
		t.Error("ClickHouse objects built for a model that does not exist")
	}
}

// An alert that cannot be created does not fail the model, and an index model
// never gets one: its alert mode is stored as none whatever the editor sent.
func TestCreateAlertIsBestEffortAndTypeAware(t *testing.T) {
	env := newEnv(t)
	alerts := newRecordingAlerts()
	alerts.createErr = errors.New("alerts down")
	env.mgr.SetAlertManager(alerts)
	r := createAndWait(t, env, CreateRequest{Name: "r", ModelType: ModelTypeRarity, Definition: rarityDef})
	if r.status != "active" || r.linkedAlertID != "" {
		t.Errorf("row: %+v", r)
	}

	alerts.createErr = nil
	r = createAndWait(t, env, CreateRequest{Name: "idx", ModelType: ModelTypeTLSH, AlertMode: "paused",
		Definition: ModelDefinition{KeyFields: []string{"tlsh"}}})
	if r.alertMode != "none" || r.linkedAlertID != "" || len(alerts.created) != 0 {
		t.Errorf("tlsh: mode %q alert %q created %d", r.alertMode, r.linkedAlertID, len(alerts.created))
	}
}

func seedModel(env *fakeEnv, mt ModelType, def ModelDefinition, edit func(*modelRow)) *modelRow {
	r := modelRow{name: "m", modelType: string(mt), definition: defJSON(def)}
	if edit != nil {
		edit(&r)
	}
	row := env.db.add(r)
	if row.chMV == "" {
		row.chMV = chModelMVName(row.id)
	}
	return row
}

// Editing only metadata or thresholds keeps the model's data and its backfill: no
// DDL at all. The linked alert follows the new name and thresholds.
func TestUpdateWithoutDetectionChangeKeepsData(t *testing.T) {
	env := newEnv(t)
	alerts := newRecordingAlerts()
	env.mgr.SetAlertManager(alerts)
	wm := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	r := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) {
		r.linkedAlertID, r.bfStatus, r.bfDone, r.bfTotal, r.watermark = "al-9", "completed", 8, 8, &wm
	})

	def := rarityDef
	def.Alert = &AlertConfig{CoverageThreshold: 0.5, PercentThreshold: 2}
	mo, err := env.mgr.Update(context.Background(), r.id, UpdateRequest{Name: "renamed", Description: "d", Definition: def})
	if err != nil {
		t.Fatal(err)
	}
	if len(env.ch.Executed()) != 0 {
		t.Errorf("a threshold edit rebuilt the model: %v", env.ch.Executed())
	}
	got, _ := env.db.get(r.id)
	if got.name != "renamed" || got.bfStatus != "completed" || got.bfDone != 8 || !got.watermark.Equal(wm) {
		t.Errorf("row: %+v", got)
	}
	if mo.Definition.Alert.PercentThreshold != 2 {
		t.Errorf("returned model: %+v", mo.Definition.Alert)
	}
	spec, ok := alerts.updated["al-9"]
	if !ok || !strings.HasPrefix(spec.Name, "renamed (model ") || !strings.Contains(spec.QueryString, "renamed") {
		t.Errorf("linked alert not synced: %+v", alerts.updated)
	}
}

// A detection change drops and recreates the data, stops the backfill writing to
// the old table, and re-arms the backfill anchored where live state already is:
// rows before the watermark are the backfill's, rows after it the maintainer's.
func TestUpdateWithDetectionChangeRebuilds(t *testing.T) {
	env := newEnv(t)
	wm := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	r := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) {
		r.bfStatus, r.bfDone, r.bfTotal, r.bfWindow, r.watermark = "completed", 8, 8, "7d", &wm
	})
	running, cancel := context.WithCancel(context.Background())
	defer cancel()
	env.mgr.registerCancel(r.id, cancel)

	def := rarityDef
	def.ValueKey = "parent"
	if _, err := env.mgr.Update(context.Background(), r.id, UpdateRequest{Name: "m", Definition: def}); err != nil {
		t.Fatal(err)
	}
	if running.Err() == nil {
		t.Error("the in-flight backfill was not cancelled before its table was dropped")
	}
	got, _ := env.db.get(r.id)
	if got.status != "active" || got.bfStatus != "none" || got.bfDone != 0 || got.bfWindow != "" {
		t.Errorf("backfill not re-armed: %+v", got)
	}
	if !got.watermark.Equal(wm) || !got.bfAnchor.Equal(wm) {
		t.Errorf("watermark %v anchor %v, want both at %v", got.watermark, got.bfAnchor, wm)
	}
	execs := env.ch.Executed()
	want := []string{
		"DROP VIEW IF EXISTS `" + r.chMV + "`",
		"DROP TABLE IF EXISTS `" + r.chTable + "` SYNC SETTINGS max_table_size_to_drop = 0",
		"CREATE TABLE IF NOT EXISTS `" + r.chTable + "`",
	}
	if len(execs) < len(want) {
		t.Fatalf("execs: %v", execs)
	}
	for i, w := range want {
		if !strings.HasPrefix(execs[i], w) {
			t.Errorf("exec %d = %s, want prefix %s", i, execs[i], w)
		}
	}
	if got.definition != defJSON(def) {
		t.Errorf("definition not stored: %s", got.definition)
	}
}

func TestUpdateRebuildFailureMarksError(t *testing.T) {
	env := newEnv(t)
	r := seedModel(env, ModelTypeRarity, rarityDef, nil)
	env.ch.FailExec("CREATE TABLE", errors.New("keeper unavailable"))
	def := rarityDef
	def.PartitionKey = "user"
	if _, err := env.mgr.Update(context.Background(), r.id, UpdateRequest{Name: "m", Definition: def}); err == nil || !strings.Contains(err.Error(), "recreate clickhouse objects") {
		t.Fatalf("err = %v", err)
	}
	got, _ := env.db.get(r.id)
	if got.status != "error" || !strings.Contains(got.errorMessage, "keeper unavailable") {
		t.Errorf("row: %q %q", got.status, got.errorMessage)
	}
}

// A model whose alert was deleted on the Alerts page gets a new one on its next
// edit rather than staying alert-less.
func TestUpdateRecreatesMissingAlert(t *testing.T) {
	env := newEnv(t)
	alerts := newRecordingAlerts()
	alerts.nextID = "al-new"
	env.mgr.SetAlertManager(alerts)
	r := seedModel(env, ModelTypeRarity, rarityDef, nil)
	mo, err := env.mgr.Update(context.Background(), r.id, UpdateRequest{Name: "m", Definition: rarityDef})
	if err != nil {
		t.Fatal(err)
	}
	if mo.LinkedAlertID != "al-new" || len(alerts.created) != 1 {
		t.Errorf("linked %q created %d", mo.LinkedAlertID, len(alerts.created))
	}
}

func TestUpdateUnknownModel(t *testing.T) {
	env := newEnv(t)
	if _, err := env.mgr.Update(context.Background(), "missing", UpdateRequest{Definition: rarityDef}); err == nil || err.Error() != "model not found" {
		t.Errorf("err = %v", err)
	}
}

// Delete removes the row at once and every ClickHouse object after: for a network
// model on a cluster that is the view, both tables and both companions, each ON
// CLUSTER.
func TestDeleteNetworkModelOnCluster(t *testing.T) {
	env := newClusterEnv(t)
	alerts := newRecordingAlerts()
	env.mgr.SetAlertManager(alerts)
	r := seedModel(env, ModelTypeBeacon, ModelDefinition{}, func(r *modelRow) { r.linkedAlertID = "al-1" })
	running, cancel := context.WithCancel(context.Background())
	defer cancel()
	env.mgr.registerCancel(r.id, cancel)
	env.mgr.health.entries[r.id] = healthEntry{}

	if err := env.mgr.Delete(context.Background(), r.id); err != nil {
		t.Fatal(err)
	}
	if _, ok := env.db.get(r.id); ok {
		t.Error("row still present")
	}
	if running.Err() == nil || len(alerts.deleted) != 1 || alerts.deleted[0] != "al-1" {
		t.Errorf("backfill cancelled %v, alerts deleted %v", running.Err() != nil, alerts.deleted)
	}
	if _, ok := env.mgr.health.entries[r.id]; ok {
		t.Error("health summary of a deleted model kept")
	}
	waitFor(t, "the async drops", func() bool { return len(env.ch.Executed()) == 5 })
	dropped := map[string]bool{}
	for _, s := range env.ch.Executed() {
		if !strings.Contains(s, "ON CLUSTER 'bf_cluster'") {
			t.Errorf("not ON CLUSTER: %s", s)
		}
		if m := strings.SplitN(s, "`", 3); len(m) == 3 {
			dropped[m[1]] = true
		}
	}
	for _, name := range []string{r.chMV, r.chTable, chModelStateName(r.id), chModelDistName(r.id), chModelStateDistName(r.id)} {
		if !dropped[name] {
			t.Errorf("%s not dropped", name)
		}
	}
}

// A drop that fails does not stop the rest: a partial earlier failure still
// cleans up fully on the next attempt.
func TestDropCHObjectsContinuesPastFailures(t *testing.T) {
	env := newEnv(t)
	env.ch.FailExec("DROP VIEW", errors.New("boom"))
	if err := env.mgr.dropCHObjects(context.Background(), "x", "model_x", "model_mv_x", ModelTypeLongConnection); err != nil {
		t.Fatal(err)
	}
	if len(env.ch.ExecutedMatching("DROP TABLE")) != 2 {
		t.Errorf("drops: %v", env.ch.Executed())
	}
}

func TestDeleteUnknownModel(t *testing.T) {
	env := newEnv(t)
	if err := env.mgr.Delete(context.Background(), "missing"); err == nil {
		t.Error("deleted a model that does not exist")
	}
}

func TestSetAlertEnabled(t *testing.T) {
	env := newEnv(t)
	ctx := context.Background()

	// No alert manager wired: nothing to toggle.
	r := seedModel(env, ModelTypeRarity, rarityDef, nil)
	if err := env.mgr.SetAlertEnabled(ctx, r.id, true); err == nil || !strings.Contains(err.Error(), "no alert is linked") {
		t.Errorf("no manager: %v", err)
	}

	alerts := newRecordingAlerts()
	env.mgr.SetAlertManager(alerts)

	// Missing alert: created lazily, then enabled, and the mode mirrored.
	if err := env.mgr.SetAlertEnabled(ctx, r.id, true); err != nil {
		t.Fatal(err)
	}
	got, _ := env.db.get(r.id)
	if got.linkedAlertID != "alert-1" || !alerts.enabled["alert-1"] || got.alertMode != "active" {
		t.Errorf("enable: row %+v enabled %v", got, alerts.enabled)
	}
	if err := env.mgr.SetAlertEnabled(ctx, r.id, false); err != nil {
		t.Fatal(err)
	}
	got, _ = env.db.get(r.id)
	if alerts.enabled["alert-1"] || got.alertMode != "paused" || len(alerts.created) != 1 {
		t.Errorf("pause: row %+v enabled %v created %d", got, alerts.enabled, len(alerts.created))
	}

	// A toggle the alert store refuses leaves the mirrored mode alone.
	alerts.setErr = errors.New("nope")
	if err := env.mgr.SetAlertEnabled(ctx, r.id, true); err == nil {
		t.Error("refused toggle reported success")
	}
	if got, _ = env.db.get(r.id); got.alertMode != "paused" {
		t.Errorf("mode moved on a refused toggle: %q", got.alertMode)
	}

	idx := seedModel(env, ModelTypeTLSH, ModelDefinition{KeyFields: []string{"d"}}, nil)
	if err := env.mgr.SetAlertEnabled(ctx, idx.id, true); err == nil || !strings.Contains(err.Error(), "do not raise alerts") {
		t.Errorf("tlsh: %v", err)
	}
	if err := env.mgr.SetAlertEnabled(ctx, "missing", true); err == nil {
		t.Error("unknown model")
	}
}

// List orders by name and reports the alert mode its linked alert holds, whichever
// page toggled it; an empty fractal is an empty list, not null.
func TestListAndGet(t *testing.T) {
	env := newEnv(t)
	ctx := context.Background()
	fired := time.Date(2026, 10, 2, 3, 0, 0, 0, time.UTC)
	env.db.alerts["al-on"] = &alertRow{enabled: true, lastTriggered: &fired}
	env.db.alerts["al-off"] = &alertRow{enabled: false}
	seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.name, r.linkedAlertID = "zeta", "al-on" })
	seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.name, r.linkedAlertID = "alpha", "al-off" })
	seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.name, r.fractalID = "other", fracB })

	list, err := env.mgr.List(ctx, fracA)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Name != "alpha" || list[1].Name != "zeta" {
		t.Fatalf("list: %+v", list)
	}
	if list[0].AlertMode != "paused" || list[1].AlertMode != "active" || list[1].LastAlertAt == nil || !list[1].LastAlertAt.Equal(fired) {
		t.Errorf("alert modes: %q %q %v", list[0].AlertMode, list[1].AlertMode, list[1].LastAlertAt)
	}
	if list[0].Definition.PartitionKey != "host" {
		t.Errorf("definition: %+v", list[0].Definition)
	}

	empty, err := env.mgr.List(ctx, "no-such-fractal")
	if err != nil || empty == nil || len(empty) != 0 {
		t.Errorf("empty list: %v %v", empty, err)
	}

	env.db.failOn("ORDER BY name", errors.New("pg down"))
	if _, err := env.mgr.List(ctx, fracA); err == nil {
		t.Error("list error swallowed")
	}
}

// model_lookup() resolves a name to the table its reads must use: the
// Distributed companion on a cluster. Only active models resolve.
func TestListModelInfos(t *testing.T) {
	ctx := context.Background()
	for _, cluster := range []bool{false, true} {
		env := newEnv(t)
		if cluster {
			env = newClusterEnv(t)
		}
		a := seedModel(env, ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"u"}, TimeBucket: "hour", MinSample: 5}, func(r *modelRow) { r.name = "vol" })
		seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.name, r.status = "building", "rebuilding" })

		infos, err := env.mgr.ListModelInfos(ctx, fracA)
		if err != nil {
			t.Fatal(err)
		}
		if len(infos) != 1 {
			t.Fatalf("infos: %+v", infos)
		}
		info := infos["vol"]
		wantTable := a.chTable
		if cluster {
			wantTable = chModelDistName(a.id)
		}
		if info.ID != a.id || info.TableName != wantTable || info.Distributed != cluster ||
			info.TimeBucket != "hour" || info.MinSample != 5 || info.FractalID != fracA || info.ModelType != ModelTypeVolumeBaseline {
			t.Errorf("cluster=%v info: %+v", cluster, info)
		}
	}
}

// Across a prism's member fractals the oldest same-named model wins, every time,
// and carries the fractal that owns it.
func TestListModelInfosForFractalsOldestWins(t *testing.T) {
	env := newClusterEnv(t)
	ctx := context.Background()
	newer := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) {
		r.name, r.createdAt = "dup", time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	})
	older := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) {
		r.name, r.fractalID, r.createdAt = "dup", fracB, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	})
	_ = newer
	infos, err := env.mgr.ListModelInfosForFractals(ctx, []string{fracA, fracB})
	if err != nil {
		t.Fatal(err)
	}
	if got := infos["dup"]; got.ID != older.id || got.FractalID != fracB || got.TableName != chModelDistName(older.id) || !got.Distributed {
		t.Errorf("dup resolved to %+v, want the older model in %s", got, fracB)
	}
	if got, err := env.mgr.ListModelInfosForFractals(ctx, nil); err != nil || len(got) != 0 {
		t.Errorf("no fractals: %v %v", got, err)
	}
	if n := len(env.db.pg.Matching("ANY($1)")); n != 1 {
		t.Errorf("an empty fractal list must not query: %d queries", n)
	}
}

// tlsh() may only probe an index that holds a superset of what a query could see:
// a model with a source filter is excluded and named, one keyed on another field
// is ignored, and the oldest unfiltered index per fractal wins.
func TestListTLSHIndexes(t *testing.T) {
	env := newEnv(t)
	ctx := context.Background()
	key := ModelDefinition{KeyFields: []string{"tlsh"}}
	day := func(d int) time.Time { return time.Date(2026, 9, d, 0, 0, 0, 0, time.UTC) }
	filtered := key
	filtered.Filter = []FilterCondition{{Field: "event", Op: "=", Value: "x"}}

	seedModel(env, ModelTypeTLSH, filtered, func(r *modelRow) { r.name, r.createdAt = "filtered", day(1) })
	oldest := seedModel(env, ModelTypeTLSH, key, func(r *modelRow) { r.name, r.createdAt = "oldest", day(2) })
	seedModel(env, ModelTypeTLSH, key, func(r *modelRow) { r.name, r.createdAt = "newer", day(3) })
	seedModel(env, ModelTypeTLSH, ModelDefinition{KeyFields: []string{"other"}}, func(r *modelRow) { r.name, r.fractalID = "other-key", fracB })
	seedModel(env, ModelTypeTLSH, filtered, func(r *modelRow) { r.name, r.fractalID = "only-filtered", fracB })
	seedModel(env, ModelTypeTLSH, key, func(r *modelRow) { r.name, r.definition = "broken", "{" })

	idx, why, err := env.mgr.ListTLSHIndexes(ctx, []string{fracA, fracB}, "tlsh")
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != 1 || idx[fracA].ModelID != oldest.id || idx[fracA].TableName != oldest.chTable || idx[fracA].KeyField != "tlsh" {
		t.Errorf("indexes: %+v", idx)
	}
	if why[fracA] != "filtered" || why[fracB] != "only-filtered" {
		t.Errorf("excluded: %v", why)
	}
	if idx, why, err := env.mgr.ListTLSHIndexes(ctx, []string{fracA}, ""); err != nil || len(idx)+len(why) != 0 {
		t.Error("no key field must resolve nothing")
	}
}

// After a log reset the model tables are gone while Postgres still describes the
// models. Startup recreates what is missing, re-arms its backfill, and only
// patches columns on a table that survived.
func TestReconcileCHObjects(t *testing.T) {
	env := newEnv(t)
	wm := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	kept := seedModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user"}}, func(r *modelRow) { r.watermark = &wm })
	lost := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) {
		r.bfStatus, r.bfDone, r.watermark = "completed", 8, &wm
	})
	broken := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.definition = "not json" })

	env.ch.existing[kept.chTable] = true
	env.ch.columns[kept.chTable] = []string{"entity_key"} // predates first_recorded

	env.mgr.ReconcileCHObjects(context.Background())

	if alters := env.ch.ExecutedMatching("ALTER TABLE `" + kept.chTable + "` ADD COLUMN IF NOT EXISTS `" + FirstRecordedColumn + "`"); len(alters) != 1 {
		t.Errorf("surviving first_seen table not patched: %v", env.ch.Executed())
	}
	if len(env.ch.ExecutedMatching("CREATE TABLE IF NOT EXISTS `"+kept.chTable+"`")) != 0 {
		t.Error("a table that exists was recreated")
	}
	if len(env.ch.ExecutedMatching("CREATE TABLE IF NOT EXISTS `"+lost.chTable+"`")) != 1 {
		t.Error("missing table not recreated")
	}
	got, _ := env.db.get(lost.id)
	if got.bfStatus != "none" || got.bfDone != 0 || !got.bfAnchor.Equal(wm) {
		t.Errorf("recreated model's backfill not re-armed at its watermark: %+v", got)
	}
	if len(stmtsFor(env.ch, broken.chTable)) != 0 {
		t.Error("a model with an unreadable definition was touched")
	}

	// A second startup finds everything in place and changes nothing.
	before := len(env.ch.Executed())
	env.mgr.ReconcileCHObjects(context.Background())
	if after := env.ch.Executed(); len(after) != before {
		t.Errorf("not idempotent: %v", after[before:])
	}
}

// Startup rebuilds a network model's state table left in an older layout, with
// its Distributed companion on a cluster, and leaves a current one alone. The
// results table is never dropped.
func TestReconcileCHObjectsRebuildsOldNetState(t *testing.T) {
	for _, c := range []struct {
		name         string
		cluster, old bool
	}{
		{"old layout", false, true},
		{"current layout", false, false},
		{"old layout on a cluster", true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			env := newEnv(t)
			if c.cluster {
				env = newClusterEnv(t)
			}
			r := seedModel(env, ModelTypeBeacon, ModelDefinition{}, nil)
			state, dist := chModelStateName(r.id), chModelStateDistName(r.id)
			env.ch.existing[r.chTable] = true
			env.ch.oldNetState[state] = c.old
			env.ch.oldNetState[dist] = c.old && c.cluster

			env.mgr.ReconcileCHObjects(context.Background())

			drops := len(env.ch.ExecutedMatching("DROP TABLE IF EXISTS `" + state + "`"))
			creates := len(env.ch.ExecutedMatching("CREATE TABLE IF NOT EXISTS `" + state + "`"))
			if want := map[bool]int{true: 1, false: 0}[c.old]; drops != want || creates != want {
				t.Errorf("state table dropped %d and created %d times, want %d: %v", drops, creates, want, env.ch.Executed())
			}
			if c.cluster {
				ddl := env.ch.Executed()
				dropped := slices.IndexFunc(ddl, func(s string) bool { return strings.HasPrefix(s, "DROP TABLE IF EXISTS `"+dist+"`") })
				recreated := slices.IndexFunc(ddl, func(s string) bool { return strings.HasPrefix(s, "CREATE TABLE IF NOT EXISTS `"+dist+"`") })
				if dropped < 0 || recreated < dropped {
					t.Errorf("old Distributed state companion not replaced: %v", ddl)
				}
			}
			if len(env.ch.ExecutedMatching("DROP TABLE IF EXISTS `"+r.chTable+"`")) != 0 {
				t.Error("the results table was dropped")
			}

			// A second startup finds the current layout and changes nothing.
			before := len(env.ch.ExecutedMatching("DROP TABLE"))
			env.mgr.ReconcileCHObjects(context.Background())
			if len(env.ch.ExecutedMatching("DROP TABLE")) != before {
				t.Errorf("not idempotent: %v", env.ch.Executed())
			}
		})
	}
}

// On a cluster, startup reissues every model's Distributed companions ON CLUSTER,
// including those of a model whose table the probe found: an older release created
// them on one node only, and the probe goes through the load-balanced connection,
// so it describes one node. Each companion goes out once, with the statement the
// create path sends, and one that fails or times out does not stop the rest.
func TestReconcileCHObjectsRepairsCompanionsOnCluster(t *testing.T) {
	env := newClusterEnv(t)
	rarity := seedModel(env, ModelTypeRarity, rarityDef, nil)
	beacon := seedModel(env, ModelTypeBeacon, ModelDefinition{Window: "7d"}, nil)
	timesOut := seedModel(env, ModelTypeRarity, rarityDef, nil)
	lost := seedModel(env, ModelTypeRarity, rarityDef, nil)
	for _, tbl := range []string{rarity.chTable, beacon.chTable, chModelStateName(beacon.id), timesOut.chTable} {
		env.ch.existing[tbl] = true
	}

	on := " ON CLUSTER 'bf_cluster'"
	companion := func(dist, local string) string {
		return "CREATE TABLE IF NOT EXISTS `" + dist + "`" + on + " AS `" + local +
			"` ENGINE = Distributed('bf_cluster', currentDatabase(), '" + local + "', rand())"
	}
	env.ch.FailExecFunc(func(stmt string) error {
		switch stmt {
		case companion(chModelDistName(timesOut.id), timesOut.chTable):
			return fakedb.Exception(159, "distributed DDL timed out")
		case companion(chModelDistName(beacon.id), beacon.chTable):
			return errors.New("keeper unavailable")
		}
		return nil
	})

	env.mgr.ReconcileCHObjects(context.Background())

	want := []string{
		companion(chModelDistName(rarity.id), rarity.chTable),
		companion(chModelDistName(beacon.id), beacon.chTable),
		companion(chModelStateDistName(beacon.id), chModelStateName(beacon.id)),
		companion(chModelDistName(timesOut.id), timesOut.chTable),
		companion(chModelDistName(lost.id), lost.chTable),
	}
	for _, w := range want {
		if n := len(env.ch.ExecutedMatching(w)); n != 1 {
			t.Errorf("companion sent %d times, want once: %s\nexecuted:\n%s", n, w, strings.Join(env.ch.Executed(), "\n"))
		}
	}
	for _, tbl := range []string{rarity.chTable, beacon.chTable, timesOut.chTable} {
		if len(env.ch.ExecutedMatching("CREATE TABLE IF NOT EXISTS `"+tbl+"`")) != 0 {
			t.Errorf("a table that exists was recreated: %s", tbl)
		}
	}
	if len(env.ch.ExecutedMatching("CREATE TABLE IF NOT EXISTS `"+lost.chTable+"`"+on)) != 1 {
		t.Error("missing table not recreated")
	}
	for _, r := range []*modelRow{rarity, beacon, timesOut} {
		if got, _ := env.db.get(r.id); got.bfStatus != "none" || got.watermark != nil {
			t.Errorf("repairing companions touched the model's state: %+v", got)
		}
	}
}

// Before keyless logs were skipped, a first_seen model recorded them as one blank
// entity that every keyless log joined. Startup deletes that entity once: only
// when it is there, and only the blank key the model's key fields encode.
func TestReconcileCHObjectsDeletesBlankFirstSeenEntity(t *testing.T) {
	env := newEnv(t)
	one := seedModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user"}}, nil)
	two := seedModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user", "host"}}, nil)
	clean := seedModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user"}}, nil)
	for _, r := range []*modelRow{one, two, clean} {
		env.ch.existing[r.chTable] = true
	}
	env.ch.blankRows = map[string]bool{one.chTable: true, two.chTable: true}

	env.mgr.ReconcileCHObjects(context.Background())

	for tbl, blank := range map[string]string{one.chTable: "''", two.chTable: "repeat(char(30), 1)"} {
		want := "DELETE FROM `" + tbl + "` WHERE entity_key = " + blank
		if n := len(env.ch.ExecutedMatching(want)); n != 1 {
			t.Errorf("sent %d times, want once: %s\nexecuted:\n%s", n, want, strings.Join(env.ch.Executed(), "\n"))
		}
	}
	if n := len(env.ch.ExecutedMatching("DELETE FROM `" + clean.chTable + "`")); n != 0 {
		t.Errorf("deleted from a table with no blank entity")
	}
}

// Single node has no companions: a model whose table exists sends nothing.
func TestReconcileCHObjectsSingleNodeSendsNoCompanions(t *testing.T) {
	env := newEnv(t)
	r := seedModel(env, ModelTypeBeacon, ModelDefinition{Window: "7d"}, nil)
	env.ch.existing[r.chTable] = true
	env.mgr.ReconcileCHObjects(context.Background())
	if execs := env.ch.Executed(); len(execs) != 0 {
		t.Errorf("single node sent DDL for a model in place: %v", execs)
	}
}

func TestReconcileCHObjectsListFailure(t *testing.T) {
	env := newEnv(t)
	env.db.failOn("ch_mv_name FROM analytics_models", errors.New("pg down"))
	env.mgr.ReconcileCHObjects(context.Background())
	if len(env.ch.Executed()) != 0 {
		t.Error("acted without a model list")
	}
}

// Every leftover insert-time view is dropped from ClickHouse, including one whose
// model is gone, and every model is handed to the state maintainer.
func TestReconcileStateViews(t *testing.T) {
	env := newClusterEnv(t)
	wm := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	withView := seedModel(env, ModelTypeRarity, rarityDef, nil) // chMV set, no watermark
	handed := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.chMV, r.watermark = "", &wm })
	handed.chMV = ""
	env.ch.views = []string{withView.chMV, "model_mv_orphan"}

	env.mgr.ReconcileStateViews(context.Background())

	for _, v := range env.ch.views {
		if len(env.ch.ExecutedMatching("DROP VIEW IF EXISTS `"+v+"` ON CLUSTER 'bf_cluster'")) != 1 {
			t.Errorf("%s not dropped: %v", v, env.ch.Executed())
		}
	}
	a, _ := env.db.get(withView.id)
	b, _ := env.db.get(handed.id)
	if a.chMV != "" || a.watermark == nil {
		t.Errorf("not handed over: %+v", a)
	}
	if !b.watermark.Equal(wm) {
		t.Errorf("an existing watermark moved: %v", b.watermark)
	}
}

// A view that fails to drop keeps its model off the maintainer: the model is not
// handed over (it keeps its view name and its NULL watermark), and a model an
// older release already handed over despite a live view is skipped too, with the
// reason on the model. A later startup that drops the view hands it over.
func TestReconcileStateViewsKeepsModelsWhoseViewSurvived(t *testing.T) {
	env := newClusterEnv(t)
	s := NewStateMaintainer(env.db.client, env.ch.client, env.mgr)
	wm := ms(time.Now().Add(-time.Hour))
	stuck := seedModel(env, ModelTypeRarity, rarityDef, nil)   // view will not drop, no watermark
	dropped := seedModel(env, ModelTypeRarity, rarityDef, nil) // view drops, no watermark
	// Handed over by an older release although its view was still writing.
	early := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.chMV, r.watermark = "", &wm })
	early.chMV = ""
	env.ch.views = []string{stuck.chMV, dropped.chMV, chModelMVName(early.id)}

	var failing atomic.Bool
	failing.Store(true)
	env.ch.FailExecFunc(func(stmt string) error {
		if failing.Load() && strings.HasPrefix(stmt, "DROP VIEW") && !strings.Contains(stmt, dropped.chMV) {
			return errors.New("keeper unavailable")
		}
		return nil
	})
	// Ingest is visible only up to each handover, so no freed model has a window
	// however long the test takes between handover and cycle.
	var visible atomic.Int64
	s.SetIngestWatermark(func(context.Context) (time.Time, error) { return time.UnixMilli(visible.Load()), nil })

	visible.Store(time.Now().UnixMilli())
	env.mgr.ReconcileStateViews(context.Background())

	if got, _ := env.db.get(stuck.id); got.chMV != stuck.chMV || got.watermark != nil {
		t.Errorf("a model whose view survived was handed over: %+v", got)
	}
	if got, _ := env.db.get(dropped.id); got.chMV != "" || got.watermark == nil {
		t.Errorf("a model whose view dropped was not handed over: %+v", got)
	}

	// The freed models start at the handover instant, so the only one with a
	// window to read is the one whose view is still live.
	s.cycle(context.Background())
	if inserts := env.ch.ExecutedMatching("INSERT INTO"); len(inserts) != 0 {
		t.Fatalf("a model whose view is live was maintained:\n%s", strings.Join(inserts, "\n"))
	}
	if got, _ := env.db.get(early.id); !got.watermark.Equal(wm) || !strings.Contains(got.errorMessage, "could not be dropped") {
		t.Errorf("a model with a live view was maintained or not told why: %q %v", got.errorMessage, got.watermark)
	}

	// The next startup drops both and hands them over at that instant: each view
	// counted everything ingested until it dropped, so an older watermark would
	// count that stretch twice.
	failing.Store(false)
	handover := time.Now().UTC()
	visible.Store(handover.UnixMilli())
	env.mgr.ReconcileStateViews(context.Background())
	if got, _ := env.db.get(stuck.id); got.chMV != "" || got.watermark == nil {
		t.Errorf("not handed over once its view dropped: %+v", got)
	}
	if got, _ := env.db.get(early.id); got.watermark == nil || got.watermark.Before(handover) {
		t.Errorf("watermark %v kept from before the view dropped at %v", got.watermark, handover)
	}
	s.cycle(context.Background())
	if inserts := env.ch.ExecutedMatching("INSERT INTO"); len(inserts) != 0 {
		t.Errorf("state the views already wrote was read again:\n%s", strings.Join(inserts, "\n"))
	}
	if got, _ := env.db.get(early.id); got.errorMessage != "" {
		t.Errorf("stale reason kept: %q", got.errorMessage)
	}
}

func TestRowCount(t *testing.T) {
	env := newEnv(t)
	env.ch.On("SELECT count() FROM `model_x`", fakedb.Count(42))
	if n, err := env.mgr.RowCount(context.Background(), "model_x"); n != 42 || err != nil {
		t.Errorf("%d %v", n, err)
	}
	if n, err := env.mgr.RowCount(context.Background(), "model_empty"); n != 0 || err != nil {
		t.Errorf("no rows: %d %v", n, err)
	}
	env.ch.On("`model_bad`", fakedb.CHResult{Err: errors.New("unknown table")})
	if _, err := env.mgr.RowCount(context.Background(), "model_bad"); err == nil {
		t.Error("error swallowed")
	}
}
