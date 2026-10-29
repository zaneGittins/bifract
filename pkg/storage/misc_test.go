package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// newFakeClient returns a single-node client wired to a fresh scripted connection.
// Storage's own tests cannot use storagetest, which imports this package.
func newFakeClient() (*ClickHouseClient, *fakedb.CH) {
	f := fakedb.NewCH()
	return NewClickHouseClientFromConn(f, "logs", Topology{Kind: DeploymentSingleNode}), f
}

// newFakePostgres returns a client over a fresh scripted driver.
func newFakePostgres() (*PostgresClient, *fakedb.PG) {
	p := fakedb.NewPG()
	return NewPostgresClientFromDB(p.DB()), p
}

func TestValidTimezone(t *testing.T) {
	for tz, want := range map[string]bool{
		"UTC":                         true,
		"America/New_York":            true,
		"":                            false,
		"Local":                       false, // the server's zone is never an answer
		"Not/AZone":                   false,
		strings.Repeat("A", 65):       false,
		"../../../../etc/passwd":      false,
		"Europe/London" + "\x00extra": false,
	} {
		if got := ValidTimezone(tz); got != want {
			t.Errorf("ValidTimezone(%q) = %v, want %v", tz, got, want)
		}
	}
	if SafeTimezone("Not/AZone") != "UTC" || SafeTimezone("Asia/Tokyo") != "Asia/Tokyo" {
		t.Error("SafeTimezone must fall back to UTC only for an unusable zone")
	}
	if dashboardTimezone("") != "UTC" || dashboardTimezone("Asia/Tokyo") != "Asia/Tokyo" {
		t.Error("an empty dashboard zone means UTC")
	}
}

// Comment permalinks and dedup depend on the id being a pure function of the
// event time (to the nanosecond) and the raw text.
func TestGenerateLogID(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 1, time.UTC)
	id := GenerateLogID(ts, "raw")
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(id) {
		t.Fatalf("id = %q, want 32 lowercase hex chars", id)
	}
	if GenerateLogID(ts.In(time.FixedZone("x", 3600)), "raw") != id {
		t.Error("the id must not depend on the time's zone")
	}
	if GenerateLogID(ts.Add(time.Nanosecond), "raw") == id || GenerateLogID(ts, "raw2") == id {
		t.Error("the id must change with the time or the text")
	}
}

func TestAttributionAndDisplayTimezone(t *testing.T) {
	ctx := context.Background()
	if AttributionUser(ctx) != "" || DisplayTimezone(ctx) != "" {
		t.Error("an empty context credits nobody and has no zone")
	}
	ctx = context.WithValue(ctx, AttributionUserKey, "alice")
	ctx = context.WithValue(ctx, "user", &User{DisplayTimezone: "Asia/Tokyo"})
	if AttributionUser(ctx) != "alice" || DisplayTimezone(ctx) != "Asia/Tokyo" {
		t.Errorf("got %q, %q", AttributionUser(ctx), DisplayTimezone(ctx))
	}
	// A machine principal stores no *User; it must read as "no zone", not panic.
	ctx = context.WithValue(context.Background(), "user", (*User)(nil))
	if DisplayTimezone(ctx) != "" {
		t.Error("a nil user has no zone")
	}
}

func TestNullableUser(t *testing.T) {
	if NullableUser("") != nil {
		t.Error("an empty username must bind as NULL, not violate the users FK")
	}
	if NullableUser("bob") != "bob" {
		t.Error("a username binds as itself")
	}
}

// Every table accessor resolves to the _distributed twin exactly when the topology
// has distributed tables; a read through the local name on a cluster sees one shard.
func TestTableAccessorsFollowTopology(t *testing.T) {
	accessors := map[string]func(*ClickHouseClient) string{
		"logs":             (*ClickHouseClient).ReadTable,
		"logs ":            (*ClickHouseClient).WriteTable,
		"logs_raw":         (*ClickHouseClient).RawWriteTable,
		"logs_raw ":        (*ClickHouseClient).RawReadTable,
		"logs_histogram":   (*ClickHouseClient).HistogramReadTable,
		"logs_field_paths": (*ClickHouseClient).FieldPathsReadTable,
		"logs_hot":         (*ClickHouseClient).HotReadTable,
		"proc_lineage":     (*ClickHouseClient).ProcLineageReadTable,
		"process_edges":    (*ClickHouseClient).ProcEdgesReadTable,
		"proc_freq":        (*ClickHouseClient).ProcFreqReadTable,
	}
	single := &ClickHouseClient{}
	cluster := &ClickHouseClient{topo: Topology{DistributedTables: true}}
	for base, get := range accessors {
		base = strings.TrimSpace(base)
		if got := get(single); got != base {
			t.Errorf("single node %s = %q", base, got)
		}
		if got := get(cluster); got != base+"_distributed" {
			t.Errorf("cluster %s = %q", base, got)
		}
	}
}

func TestClientAccessors(t *testing.T) {
	c := &ClickHouseClient{addrs: []string{"a:1"}, topo: Topology{Kind: DeploymentCloud}}
	if c.LogsDatabase() != "logs" {
		t.Error("an unset database means logs")
	}
	c.Database = "bf"
	if c.LogsDatabase() != "bf" || !reflect.DeepEqual(c.Addrs(), []string{"a:1"}) || c.Topology().Kind != DeploymentCloud {
		t.Error("accessors must report the client's configuration")
	}
	if EscCHIdent("a`b\\") != "a``b\\\\" {
		t.Errorf("EscCHIdent = %q", EscCHIdent("a`b\\"))
	}
	if EscCHStr(`it's \x`) != `it\'s \\x` {
		t.Errorf("EscCHStr = %q", EscCHStr(`it's \x`))
	}
	f := fakedb.NewCH()
	c.conn = f
	if c.Conn() != f || c.HealthCheck(context.Background()) != nil {
		t.Error("Conn/HealthCheck must use the pooled connection")
	}
	if err := c.Close(); err != nil || f.Closed() != 1 {
		t.Errorf("Close = %v, closed %d", err, f.Closed())
	}
	c.Exec(context.Background(), "SELECT 1")
	c.ExecWithID(context.Background(), "id", "SELECT 2")
	if got := f.Executed(); !reflect.DeepEqual(got, []string{"SELECT 1", "SELECT 2"}) {
		t.Errorf("executed %q", got)
	}
	f.FailExec("BAD", errors.New("x"))
	if err := c.Exec(context.Background(), "BAD"); err == nil || !strings.Contains(err.Error(), "failed to execute statement") {
		t.Errorf("err = %v", err)
	}
}

func TestWaitForSchemaReady(t *testing.T) {
	(&ClickHouseClient{}).WaitForSchemaReady(context.Background()) // never wired: returns at once
	(&ClickHouseClient{}).markSchemaReady()                        // nil channel: no panic

	c := &ClickHouseClient{schemaReady: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.WaitForSchemaReady(ctx) // cancelled: returns rather than blocking forever
	c.markSchemaReady()
	c.markSchemaReady() // idempotent: a second close would panic
	c.WaitForSchemaReady(context.Background())
}

func TestStringers(t *testing.T) {
	topo := Topology{Kind: DeploymentSelfManagedCluster, DDLCluster: "d", FanoutCluster: "f", DistributedTables: true, ManagedStorage: true}
	if got := topo.String(); got != "cluster ddl_cluster=d fanout_cluster=f distributed_tables managed_storage" {
		t.Errorf("Topology.String = %q", got)
	}
	if got := (Topology{Kind: DeploymentSingleNode}).String(); got != "single-node" {
		t.Errorf("Topology.String = %q", got)
	}
	if got := (ServerVersion{Major: 26, Minor: 6, Raw: "26.6.2.81"}).String(); got != "26.6.2.81" {
		t.Errorf("ServerVersion.String = %q", got)
	}
	if got := (ServerVersion{Major: 25, Minor: 6}).String(); got != "25.6" {
		t.Errorf("ServerVersion.String = %q", got)
	}
	env := ClickHouseEnv{Deployment: DeploymentCloud, Host: "h", Port: 9440, Database: "logs", User: "u", Password: "s3cret", Secure: true, TLSInsecure: true, WriteHost: "w"}
	s := env.String()
	if strings.Contains(s, "s3cret") {
		t.Fatalf("String leaked the password: %q", s)
	}
	for _, want := range []string{"write_host=w", "tls(INSECURE"} {
		if !strings.Contains(s, want) {
			t.Errorf("String = %q, want %q", s, want)
		}
	}
}

func TestNotebookLockMessages(t *testing.T) {
	var nilNotebook *Notebook
	if nilNotebook.IsLocked() || (&Notebook{}).IsLocked() {
		t.Error("only a notebook with locked_at is locked")
	}
	at := time.Date(2026, 3, 4, 5, 6, 0, 0, time.FixedZone("x", 3600))
	n := &Notebook{LockedAt: &at, LockedBy: "carol"}
	if !n.IsLocked() {
		t.Fatal("expected locked")
	}
	if got := n.LockedMessage(); got != "This notebook was locked by carol on 4 Mar 2026 04:06 UTC. Unlock it to make changes." {
		t.Errorf("message = %q", got)
	}
	n.LockedBy = ""
	if !strings.Contains(n.LockedMessage(), "locked by another user") {
		t.Error("a lock with no recorded user must still read sensibly")
	}
	if (&UnrunQueriesError{Count: 2}).Error() != "2 query sections have never been run" {
		t.Error("unexpected UnrunQueriesError text")
	}
}

// The schema identity is a privilege boundary for generated DDL. When it cannot be
// provisioned, generated DDL keeps running as the privileged user, which is the
// pre-identity behaviour, and the capability says so.
func TestEnsureSchemaIdentity(t *testing.T) {
	c, f := newWorkloadClient()
	c.topo.DDLCluster = "c1"
	c.EnsureSchemaIdentity(context.Background())
	execs := f.Executed()
	if len(execs) < 4 || !strings.HasPrefix(execs[0], "CREATE USER IF NOT EXISTS bifract_schema ON CLUSTER 'c1'") ||
		execs[2] != "GRANT ON CLUSTER 'c1' CLUSTER ON *.* TO bifract_schema" {
		t.Errorf("executed:\n%s", strings.Join(execs, "\n"))
	}
	for _, s := range execs {
		if strings.Contains(s, "GRANT") && strings.Contains(s, "ON *.*") &&
			!strings.Contains(s, "CLUSTER ON") && !strings.Contains(s, "REMOTE") &&
			!strings.Contains(s, "SET DEFINER") && !strings.Contains(s, "RELOAD DICTIONARY") {
			t.Errorf("unexpected instance-wide grant: %q", s)
		}
	}
	if c.schemaConn() != f {
		t.Error("an identity that failed its probe must not be used")
	}

	c, f = newWorkloadClient()
	f.FailExec("CREATE USER", fakedb.Exception(497, "denied"))
	c.EnsureSchemaIdentity(context.Background())
	if c.Capabilities()[CapSchemaIdentity].State != CapUnavailable {
		t.Error("a refused identity must be reported")
	}
	if len(f.ExecutedMatching("GRANT")) != 0 {
		t.Error("nothing may be granted to a user that was not created")
	}

	c, f = newWorkloadClient()
	f.FailExec("CREATE TABLE, CREATE VIEW", fakedb.Exception(497, "denied"))
	c.EnsureSchemaIdentity(context.Background())
	if c.Capabilities()[CapSchemaIdentity].State != CapUnavailable {
		t.Error("a refused required grant must be reported")
	}
}

func TestSchemaExecRoutesThroughIdentity(t *testing.T) {
	c, def := newFakeClient()
	if err := c.ExecSchema(context.Background(), "CREATE X"); err != nil || len(def.Executed()) != 1 {
		t.Fatal("without an identity, schema DDL runs on the default connection")
	}

	schema := fakedb.NewCH()
	var ident driver.Conn = schema
	c.schema.conn.Store(&ident)
	c.ExecSchema(context.Background(), "CREATE Y")
	c.ExecSchemaArgs(context.Background(), "INSERT ?", 1)
	schema.On("SELECT", fakedb.Strs("k", "v"))
	rows, err := c.QuerySchema(context.Background(), "SELECT k")
	if err != nil || !reflect.DeepEqual(rows, []map[string]interface{}{{"k": "v"}}) {
		t.Errorf("QuerySchema = %v, %v", rows, err)
	}
	if !reflect.DeepEqual(schema.Executed(), []string{"CREATE Y", "INSERT ?"}) || len(def.Executed()) != 1 {
		t.Errorf("schema conn ran %q, default ran %q", schema.Executed(), def.Executed())
	}

	schema.FailExec("BAD", errors.New("x"))
	schema.On("FAIL", fakedb.CHResult{Err: errors.New("x")})
	if c.ExecSchema(context.Background(), "BAD") == nil || c.ExecSchemaArgs(context.Background(), "BAD") == nil {
		t.Error("schema exec errors must surface")
	}
	if _, err := c.QuerySchema(context.Background(), "FAIL"); err == nil {
		t.Error("schema query errors must surface")
	}

	c.closeSchemaIdentity()
	if schema.Closed() != 1 || c.schemaConn() != def {
		t.Error("closing the identity returns schema DDL to the default connection")
	}
}

// The alert hydration lookup is chunked to the IN-list ceiling, prunes to the
// fractals actually present in the matched rows, and drops rows whose norm_log is
// empty (the caller treats a missing key as "no hydration").
func TestHydrateLogFields(t *testing.T) {
	c, f := newFakeClient()
	f.On("SELECT log_id, norm_log FROM logs_hot", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "log_id", Type: "String"}, {Column: "norm_log", Type: "String"}},
		Rows: [][]any{{"L1", `{"user":"a"}`}, {"L2", ""}},
	})
	keys := make([]LogKey, 0, 501)
	for i := range 501 {
		keys = append(keys, LogKey{LogID: fmt.Sprintf("L%d", i), FractalID: "F1"})
	}
	from, to := time.Unix(100, 0), time.Unix(200, 0)
	out, err := c.HydrateLogFields(context.Background(), "logs_hot", keys, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out["L1"]["user"] != "a" {
		t.Errorf("out = %v", out)
	}
	var lookups int
	for _, q := range f.Queried() {
		if strings.Contains(q, "FROM logs_hot") {
			lookups++
			if !strings.Contains(q, "ingest_timestamp >=") || !strings.Contains(q, "fractal_id IN (?)") {
				t.Errorf("lookup %q must be bounded and scoped", q)
			}
		}
	}
	if lookups != 2 {
		t.Errorf("%d lookups for 501 ids, want 2 chunks", lookups)
	}

	c, f = newFakeClient()
	f.On("FROM logs", fakedb.CHResult{Err: errors.New("down")})
	if _, err := c.HydrateLogFields(context.Background(), "logs", keys[:1], time.Time{}, time.Time{}); err == nil {
		t.Error("expected the query error")
	}
	if got, err := c.HydrateLogFields(context.Background(), "logs", nil, from, to); got != nil || err != nil {
		t.Error("no keys, no work")
	}
}

// The event-time window is an optimisation: ids it misses are retried unbounded,
// and only those ids.
func TestGetLogDisplayRowsByIDsRetriesMissesUnbounded(t *testing.T) {
	c, f := newFakeClient()
	cols := []fakedb.Col{{Column: "timestamp", Type: "DateTime64(3, 'UTC')"}, {Column: "log_id", Type: "String"}, {Column: "fields", Type: "String"}}
	ts := time.Unix(150, 0).UTC()
	f.OnOnce("AND timestamp >=", fakedb.CHResult{Cols: cols, Rows: [][]any{{ts, "L1", `{"a":1}`}}})
	f.OnOnce("LIMIT 1 BY log_id", fakedb.CHResult{Cols: cols, Rows: [][]any{{ts, "L2", `{}`}}})

	out, err := c.GetLogDisplayRowsByIDs(context.Background(), "F1", []string{"L1", "L2", "L1", ""}, time.Unix(100, 0), time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out["L1"]["log_id"] != "L1" || out["L2"]["log_id"] != "L2" {
		t.Errorf("out = %v", out)
	}
	qs := f.Queried()
	if len(qs) != 2 || !strings.Contains(qs[0], "timestamp >=") || strings.Contains(qs[1], "timestamp >=") {
		t.Errorf("queries = %q, want a bounded pass then an unbounded retry", qs)
	}

	// Everything found in the window: no second pass.
	c, f = newFakeClient()
	f.On("SELECT", fakedb.CHResult{Cols: cols, Rows: [][]any{{ts, "L1", `{}`}}})
	c.GetLogDisplayRowsByIDs(context.Background(), "", []string{"L1"}, time.Unix(100, 0), time.Unix(200, 0))
	if n := len(f.Queried()); n != 1 {
		t.Errorf("%d queries, want 1", n)
	}

	if out, err := c.GetLogDisplayRowsByIDs(context.Background(), "", []string{""}, time.Time{}, time.Time{}); out != nil || err != nil {
		t.Error("no usable ids, no work")
	}
	c, f = newFakeClient()
	f.On("SELECT", fakedb.CHResult{Err: errors.New("down")})
	if _, err := c.GetLogDisplayRowsByIDs(context.Background(), "", []string{"L1"}, time.Time{}, time.Time{}); err == nil {
		t.Error("expected the query error")
	}
}
