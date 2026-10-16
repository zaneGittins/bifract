package setup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bifract/pkg/storage"
)

func TestClickHouseNeedsReset(t *testing.T) {
	tests := []struct {
		name    string
		rule    fakeRule
		want    bool
		wantErr bool
	}{
		{"event-time partitioning", fakeRule{Stdout: []byte("toYYYYMMDD(timestamp)\n")}, true, false},
		{"ingest-time partitioning", fakeRule{Stdout: []byte("toDate(ingest_timestamp)\n")}, false, false},
		{"fresh install has no logs table", fakeRule{Stdout: []byte("\n")}, false, false},
		{"query failure", fakeRule{Stdout: []byte("Code: 516. Authentication failed"), Exit: 1}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeExec(t, tt.rule)
			got, err := clickHouseNeedsReset(&DockerOps{Dir: t.TempDir()}, "pw")
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("clickHouseNeedsReset = %v, %v; want %v, err %v", got, err, tt.want, tt.wantErr)
			}
			if c := fake.calls(); len(c) != 1 || !strings.Contains(c[0].joined(), "--user default --password pw") {
				t.Errorf("calls = %+v", c)
			}
		})
	}
}

func TestResetPostgresState(t *testing.T) {
	sql := postgresResetSQL()
	if !strings.HasPrefix(sql, "BEGIN;\n") || !strings.HasSuffix(sql, ";\nCOMMIT;") {
		t.Errorf("reset is not one transaction:\n%s", sql)
	}
	for _, stmt := range storage.ResetPostgresStateStatements() {
		if !strings.Contains(sql, stmt+";") {
			t.Errorf("reset SQL lacks %q", stmt)
		}
	}

	newFakeExec(t)
	if err := resetPostgresState(&DockerOps{Dir: t.TempDir()}, "u", "d"); err != nil {
		t.Errorf("clean run: %v", err)
	}
	// psql can report a failed statement in its output; that is a failure too.
	newFakeExec(t, fakeRule{Stdout: []byte("ERROR:  relation \"archive_completeness\" does not exist\nROLLBACK\n")})
	if err := resetPostgresState(&DockerOps{Dir: t.TempDir()}, "u", "d"); err == nil {
		t.Error("an ERROR in psql output was treated as success")
	}
	newFakeExec(t, fakeRule{Exit: 2})
	if err := resetPostgresState(&DockerOps{Dir: t.TempDir()}, "u", "d"); err == nil {
		t.Error("a psql failure was treated as success")
	}
}

func TestModelCHObjects(t *testing.T) {
	views, tables := modelCHObjects("\n m1 \n\nm2\n")
	var wantViews, wantTables []string
	for _, id := range []string{"m1", "m2"} {
		v, tb := storage.ModelCHObjectNames(id)
		wantViews, wantTables = append(wantViews, v...), append(wantTables, tb...)
	}
	if fmt.Sprint(views) != fmt.Sprint(wantViews) || fmt.Sprint(tables) != fmt.Sprint(wantTables) {
		t.Errorf("modelCHObjects = %v %v, want %v %v", views, tables, wantViews, wantTables)
	}
	if v, tb := modelCHObjects(""); v != nil || tb != nil {
		t.Errorf("no models gave %v %v", v, tb)
	}
}

func TestPostgresCreds(t *testing.T) {
	if u, d := postgresCreds(map[string]string{}); u != "bifract" || d != "bifract" {
		t.Errorf("defaults = %q %q", u, d)
	}
	if u, d := postgresCreds(map[string]string{"POSTGRES_USER": "u", "POSTGRES_DB": "d"}); u != "u" || d != "d" {
		t.Errorf("overrides = %q %q", u, d)
	}
}

func resetInstall(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, ".env"), []byte("POSTGRES_PASSWORD=pg\nCLICKHOUSE_PASSWORD=chpw\n"), 0600)
	return dir
}

func runResetLogs(t *testing.T, dir string, nonInteractive bool) (string, error) {
	t.Helper()
	var err error
	out := captureStdout(t, func() { err = RunResetLogs(dir, nonInteractive) })
	return out, err
}

// The writers stop before anything is dropped (ingest would otherwise recreate
// tables mid-reset), every derived object goes in dependency order, and the
// Postgres rows describing the dropped data are cleared before the app restarts.
func TestRunResetLogsOrder(t *testing.T) {
	dir := resetInstall(t)
	fake := newFakeExec(t,
		runningStack,
		fakeRule{Match: []string{storage.ModelIDsQuery}, Stdout: []byte("m1\n")},
		// Fail the restart: the health check after it polls in real time.
		composeUpFails,
	)
	_, err := runResetLogs(t, dir, true)
	if err == nil || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("err = %v, want the restart failure", err)
	}

	views, tables := storage.ModelCHObjectNames("m1")
	var want []string
	want = append(want, "compose ps --status running -q", "compose stop bifract bifract-ingest",
		"compose exec -T postgres psql -U bifract -d bifract -tAc "+storage.ModelIDsQuery)
	for _, stmt := range storage.ResetLogDataStatements(views, tables) {
		want = append(want, "compose exec -T clickhouse clickhouse-client --user default --password chpw --database logs "+
			"--receive_timeout 3600 --send_timeout 3600 --query "+stmt)
	}
	want = append(want, "compose exec -T postgres psql -U bifract -d bifract -c "+postgresResetSQL(), "compose up -d")

	var got []string
	for _, c := range fake.calls() {
		got = append(got, c.joined())
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("calls:\n%s\n\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestRunResetLogsStopsEarly(t *testing.T) {
	t.Run("missing credentials", func(t *testing.T) {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, ".env"), []byte("POSTGRES_PASSWORD=pg\n"), 0600)
		fake := newFakeExec(t, runningStack)
		if _, err := runResetLogs(t, dir, true); err == nil || !strings.Contains(err.Error(), "CLICKHOUSE_PASSWORD") {
			t.Errorf("err = %v", err)
		}
		if len(fake.calls()) != 0 {
			t.Error("ran docker without credentials")
		}
	})
	t.Run("no install", func(t *testing.T) {
		withoutExternalCommands(t)
		if _, err := runResetLogs(t, t.TempDir(), true); err == nil || !strings.Contains(err.Error(), "read .env") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("stack stopped", func(t *testing.T) {
		fake := newFakeExec(t)
		if _, err := runResetLogs(t, resetInstall(t), true); err == nil || !strings.Contains(err.Error(), "not running") {
			t.Errorf("err = %v", err)
		}
		if n := len(fake.calls()); n != 1 {
			t.Errorf("made %d calls, want only the running check", n)
		}
	})
	t.Run("declined", func(t *testing.T) {
		fake := newFakeExec(t, runningStack)
		withStdin(t, "no\n")
		out, err := runResetLogs(t, resetInstall(t), false)
		if err != nil || !strings.Contains(out, "Reset cancelled") {
			t.Errorf("err = %v, out:\n%s", err, out)
		}
		if n := len(fake.calls()); n != 1 {
			t.Errorf("made %d calls after the operator declined", n)
		}
	})
	t.Run("drop failure stops before Postgres", func(t *testing.T) {
		fake := newFakeExec(t, runningStack,
			fakeRule{Match: []string{"DROP VIEW IF EXISTS"}, Stdout: []byte("Code: 999"), Exit: 1})
		if _, err := runResetLogs(t, resetInstall(t), true); err == nil || !strings.Contains(err.Error(), "clickhouse reset") {
			t.Errorf("err = %v", err)
		}
		for _, c := range fake.calls() {
			if strings.Contains(c.joined(), "BEGIN;") || strings.Contains(c.joined(), "compose up") {
				t.Errorf("continued after a failed drop: %s", c.joined())
			}
		}
	})
}

// k8sResetRules answers RunResetLogsK8s against two ClickHouse shards with one
// orphaned Keeper replica left by an earlier pod incarnation.
func k8sResetRules(stalePath string) []fakeRule {
	return []fakeRule{
		{Cmd: "kubectl", Match: []string{"get pods", "-l " + k8sCHPodSelector}, Stdout: []byte("bifract-ch-clickhouse-0-0-0\nbifract-ch-clickhouse-1-0-0\n")},
		{Cmd: "kubectl", Match: []string{"get secret bifract-secrets"}, Stdout: []byte(`{"CLICKHOUSE_PASSWORD":"` + b64("chpw") + `"}`)},
		{Cmd: "kubectl", Match: []string{"get deployment bifract "}, Stdout: []byte("2")},
		{Cmd: "kubectl", Match: []string{"get statefulset bifract-ingest "}, Stdout: []byte("3")},
		{Cmd: "kubectl", Match: []string{"get pods", "--no-headers"}, Stdout: []byte("bifract-ch-clickhouse-0-0-0\nbifract-postgres-0\n")},
		{Cmd: "kubectl", Match: []string{storage.ModelIDsQuery}, Stdout: []byte("m1\n")},
		{Cmd: "kubectl", Match: []string{storage.ShardMacroQuery}, Stdout: []byte("01\n")},
		{Cmd: "kubectl", Match: []string{storage.KeeperReplicasQuery(stalePath)}, Stdout: []byte("chi-old-0\n")},
		// Every other Keeper path is absent, which is the healthy case.
		{Cmd: "kubectl", Match: []string{"system.zookeeper"}, Stdout: []byte("Code: 999. No node"), Exit: 1},
	}
}

func runResetLogsK8s(t *testing.T, nonInteractive bool) (string, error) {
	t.Helper()
	var err error
	out := captureStdout(t, func() { err = RunResetLogsK8s(nonInteractive) })
	return out, err
}

func TestRunResetLogsK8s(t *testing.T) {
	stale := storage.KeeperTablePath("01", "logs", "logs")
	fake := newFakeExec(t, k8sResetRules(stale)...)
	if _, err := runResetLogsK8s(t, true); err != nil {
		t.Fatal(err)
	}

	var calls []string
	for _, c := range fake.calls() {
		calls = append(calls, c.joined())
	}
	index := func(s string) int {
		for i, c := range calls {
			if strings.Contains(c, s) {
				return i
			}
		}
		return -1
	}
	lastIndex := func(s string) int {
		for i := len(calls) - 1; i >= 0; i-- {
			if strings.Contains(calls[i], s) {
				return i
			}
		}
		return -1
	}

	firstDrop, lastDrop := index("--query DROP "), lastIndex("--query DROP ")
	for _, scale := range []string{"scale deployment bifract -n bifract --replicas=0", "scale statefulset bifract-ingest -n bifract --replicas=0"} {
		if i := index(scale); i < 0 || i > firstDrop {
			t.Errorf("%q must run before the first drop", scale)
		}
	}
	reset := index("BEGIN;")
	if reset < lastDrop {
		t.Error("Postgres was cleared before the drops finished")
	}
	for _, restore := range []string{"scale deployment bifract -n bifract --replicas=2", "scale statefulset bifract-ingest -n bifract --replicas=3"} {
		if i := index(restore); i < reset {
			t.Errorf("%q must run after the reset completes (at %d, reset at %d)", restore, i, reset)
		}
	}

	// Every shard gets every statement: one connection would leave the others
	// half-reset.
	views, tables := storage.ModelCHObjectNames("m1")
	stmts := storage.ResetLogDataStatements(views, tables)
	for _, pod := range []string{"bifract-ch-clickhouse-0-0-0", "bifract-ch-clickhouse-1-0-0"} {
		var got []string
		for _, c := range fake.calls() {
			if len(c.Args) > 3 && c.Args[3] == pod && strings.HasPrefix(c.lastArg(), "DROP ") {
				got = append(got, c.lastArg())
			}
		}
		if strings.Join(got, "\n") != strings.Join(stmts, "\n") {
			t.Errorf("%s ran %d drop statements, want all %d in order", pod, len(got), len(stmts))
		}
	}

	var dropped []string
	for _, c := range fake.calls() {
		if strings.HasPrefix(c.lastArg(), "SYSTEM DROP REPLICA") {
			dropped = append(dropped, c.lastArg())
		}
	}
	want := storage.DropKeeperReplicaStatement("chi-old-0", stale)
	if len(dropped) != 2 || dropped[0] != want || dropped[1] != want {
		t.Errorf("keeper cleanup = %q, want %q once per shard", dropped, want)
	}
	if !strings.Contains(calls[firstDrop], "--password chpw --database logs --receive_timeout 3600") {
		t.Errorf("drop call = %s", calls[firstDrop])
	}
}

// An abort part-way must not strand the cluster with no app and no ingest.
func TestRunResetLogsK8sRestoresWritersOnFailure(t *testing.T) {
	rules := append([]fakeRule{{
		Cmd: "kubectl", Match: []string{"bifract-ch-clickhouse-1-0-0", "DROP TABLE"}, Stdout: []byte("Code: 999"), Exit: 1,
	}}, k8sResetRules("/none")...)
	fake := newFakeExec(t, rules...)

	_, err := runResetLogsK8s(t, true)
	if err == nil || !strings.Contains(err.Error(), "bifract-ch-clickhouse-1-0-0") {
		t.Fatalf("err = %v, want the failure on the second shard", err)
	}
	var restored []string
	for _, c := range fake.calls() {
		if strings.HasPrefix(c.joined(), "scale ") && !strings.HasSuffix(c.joined(), "--replicas=0") {
			restored = append(restored, c.joined())
		}
		if strings.Contains(c.joined(), "BEGIN;") {
			t.Error("cleared Postgres although the ClickHouse reset failed")
		}
	}
	if len(restored) != 2 {
		t.Errorf("restored %v, want both writers scaled back", restored)
	}
}

func TestRunResetLogsK8sStopsEarly(t *testing.T) {
	t.Run("no kubectl", func(t *testing.T) {
		withoutExternalCommands(t)
		if _, err := runResetLogsK8s(t, true); err == nil || !strings.Contains(err.Error(), "kubectl not found") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("no ClickHouse pods", func(t *testing.T) {
		newFakeExec(t,
			fakeRule{Cmd: "kubectl", Match: []string{"-l " + k8sCHPodSelector}},
			fakeRule{Cmd: "kubectl", Match: []string{"get pods"}, Stdout: []byte("bifract-postgres-0\n")})
		if _, err := runResetLogsK8s(t, true); err == nil || !strings.Contains(err.Error(), "no ClickHouse pods") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("no password", func(t *testing.T) {
		fake := newFakeExec(t,
			fakeRule{Cmd: "kubectl", Match: []string{"get pods"}, Stdout: []byte("bifract-ch-clickhouse-0-0-0\n")},
			fakeRule{Cmd: "kubectl", Match: []string{"get secret"}, Stdout: []byte(`{"POSTGRES_PASSWORD":"` + b64("x") + `"}`)})
		if _, err := runResetLogsK8s(t, true); err == nil || !strings.Contains(err.Error(), "CLICKHOUSE_PASSWORD is not set") {
			t.Errorf("err = %v", err)
		}
		for _, c := range fake.calls() {
			if strings.HasPrefix(c.joined(), "scale") {
				t.Error("scaled workloads before the reset could run")
			}
		}
	})
	t.Run("declined", func(t *testing.T) {
		fake := newFakeExec(t, k8sResetRules("/none")...)
		withStdin(t, "\n")
		out, err := runResetLogsK8s(t, false)
		if err != nil || !strings.Contains(out, "Reset cancelled") {
			t.Errorf("err = %v, out:\n%s", err, out)
		}
		for _, c := range fake.calls() {
			if strings.HasPrefix(c.joined(), "scale") || strings.HasPrefix(c.joined(), "exec") {
				t.Errorf("acted after the operator declined: %s", c.joined())
			}
		}
	})
}

func TestClickHousePodsFallsBackToNamePrefix(t *testing.T) {
	newFakeExec(t,
		// The operator label matches nothing (an older operator version).
		fakeRule{Cmd: "kubectl", Match: []string{"-l " + k8sCHPodSelector}, Stdout: []byte("")},
		fakeRule{Cmd: "kubectl", Match: []string{"get pods"}, Stdout: []byte("bifract-0\nbifract-ch-clickhouse-0-0-0\nbifract-postgres-0\n")},
	)
	pods, err := clickHousePods()
	if err != nil || fmt.Sprint(pods) != "[bifract-ch-clickhouse-0-0-0]" {
		t.Errorf("clickHousePods = %v, %v", pods, err)
	}
}

func TestCurrentReplicasSkipsAbsentAndIdle(t *testing.T) {
	newFakeExec(t,
		fakeRule{Cmd: "kubectl", Match: []string{"deployment bifract "}, Stdout: []byte("0")},
		fakeRule{Cmd: "kubectl", Match: []string{"statefulset bifract-ingest"}, Stdout: []byte("NotFound"), Exit: 1},
	)
	if got := currentReplicas(writerWorkloads); len(got) != 0 {
		t.Errorf("currentReplicas = %+v; an idle or absent workload must not be scaled back up", got)
	}
}
