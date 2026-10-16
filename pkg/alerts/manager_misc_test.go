package alerts

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"bifract/internal/fakedb"
)

func revisionRow(t *testing.T, num int64, summary string, c RevisionContent) []driver.Value {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return []driver.Value{num, summary, "alice", "Alice", mustHash(t, c), fixedTime, raw}
}

func TestListAndGetRevisions(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)
	c := RevisionContent{Name: "n", QueryString: `a=1`, AlertType: "event", Severity: "medium"}
	pg.answer("FROM alert_revisions r", fakedb.RowsOf(revisionRow(t, 2, "query", c), revisionRow(t, 1, "created", c)))

	revs, err := m.ListRevisions(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if len(revs) != 2 || !revs[0].IsHead || revs[1].IsHead || revs[0].Content.References == nil {
		t.Errorf("revisions = %+v", revs)
	}

	bad := revisionRow(t, 3, "x", c)
	bad[6] = []byte("{")
	pg.answer("FROM alert_revisions r", fakedb.RowsOf(bad))
	if _, err := m.ListRevisions(ctx, "a1"); err == nil || !strings.Contains(err.Error(), "decode revision 3") {
		t.Errorf("err = %v", err)
	}

	if _, err := m.GetRevision(ctx, "a1", 9); !errors.Is(err, ErrRevisionNotFound) {
		t.Errorf("err = %v", err)
	}
	pg.answer("FROM alert_revisions WHERE alert_id = $1 AND revision = $2", fakedb.RowsOf(revisionRow(t, 1, "created", c)))
	rev, err := m.GetRevision(ctx, "a1", 1)
	if err != nil || rev.Revision != 1 || rev.Content.Name != "n" {
		t.Errorf("revision = %+v %v", rev, err)
	}
}

// A restore that would silently unwire an action is refused with the action named;
// asked to drop them, it re-applies the rest through the normal update path and
// leaves the alert's enabled state alone.
func TestRestoreRevision(t *testing.T) {
	ctx := context.Background()
	stored := RevisionContent{Name: "v1", QueryString: `a=1`, AlertType: "event", Severity: "medium",
		WebhookActionIDs: []string{"w-gone"}, EmailActionIDs: []string{"e1"}}
	head := RevisionContent{Name: "v3", QueryString: `a=1`, AlertType: "event", Severity: "medium"}
	head.canonicalize()

	setup := func(t *testing.T) (*Manager, *pgFake) {
		m, pg := updateFixture(t, head, false)
		pg.answer("FROM alert_revisions WHERE alert_id = $1 AND revision = $2", fakedb.RowsOf(revisionRow(t, 1, "created", stored)))
		pg.answer("SELECT enabled, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '') FROM alerts", fakedb.Row(false, "f1", ""))
		pg.answer("SELECT id::text, name FROM webhook_actions WHERE id = ANY($1)", fakedb.RowsOf([]driver.Value{"w-gone", "notify-soc"}))
		pg.answer("FROM webhook_actions WHERE id = ANY($1) AND enabled = true", fakedb.PGResult{})
		pg.answer("FROM email_actions WHERE id = ANY($1) AND enabled = true", fakedb.RowsOf([]driver.Value{"e1", "mail"}))
		pg.answer("SELECT COUNT(*) FROM email_actions", fakedb.Row(int64(1)))
		return m, pg
	}

	t.Run("refused with the missing action named", func(t *testing.T) {
		m, pg := setup(t)
		_, err := m.RestoreRevision(ctx, "a1", 1, "bob", false)
		var blocked *RestoreBlockedError
		if !errors.As(err, &blocked) || len(blocked.Missing) != 1 {
			t.Fatalf("err = %v", err)
		}
		if got := blocked.Missing[0]; got.Kind != "webhook" || got.ID != "w-gone" || got.Name != "notify-soc" {
			t.Errorf("missing = %+v", got)
		}
		if !strings.Contains(err.Error(), "webhook action notify-soc") {
			t.Errorf("message = %v", err)
		}
		if len(pg.Matching(sqlLockAlert)) != 0 {
			t.Error("a refused restore opened a transaction")
		}
	})
	t.Run("dropping the missing actions", func(t *testing.T) {
		m, pg := setup(t)
		if _, err := m.RestoreRevision(ctx, "a1", 1, "bob", true); err != nil {
			t.Fatal(err)
		}
		upd := pg.args("UPDATE alerts\n\t\tSET name = $2")[0]
		if upd[1] != "v1" || upd[4] != false {
			t.Errorf("restored name %v enabled %v: the live enabled state must carry over", upd[1], upd[4])
		}
		if len(pg.Matching("INSERT INTO alert_webhook_actions")) != 0 {
			t.Error("the dropped webhook was re-linked")
		}
		if args := pg.args("INSERT INTO alert_email_actions"); len(args) != 1 || args[0][1] != "e1" {
			t.Errorf("email links = %v", args)
		}
	})
	t.Run("a deleted alert", func(t *testing.T) {
		m, pg := setup(t)
		pg.answer("SELECT enabled, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '') FROM alerts", fakedb.PGResult{})
		if _, err := m.RestoreRevision(ctx, "a1", 1, "bob", false); !errors.Is(err, ErrAlertNotFound) {
			t.Errorf("err = %v", err)
		}
	})
}

func TestListTests(t *testing.T) {
	m, pg := newTestManager(t)
	pg.answer("FROM alert_tests WHERE alert_id", fakedb.RowsOf([]driver.Value{"t1", "fires", "match", []byte(`[{"status":"500"}]`), int64(0)}))
	tests, err := m.ListTests(context.Background(), "a1")
	if err != nil || len(tests) != 1 || tests[0].Events[0]["status"] != "500" {
		t.Errorf("tests = %+v %v", tests, err)
	}
	pg.answer("FROM alert_tests WHERE alert_id", fakedb.RowsOf([]driver.Value{"t1", "fires", "match", []byte(`nope`), int64(0)}))
	if _, err := m.ListTests(context.Background(), "a1"); err == nil || !strings.Contains(err.Error(), `decode events for test "fires"`) {
		t.Errorf("err = %v", err)
	}
}

func readZip(t *testing.T, data []byte) map[string]string {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(rc)
		rc.Close()
		out[f.Name] = string(b)
	}
	return out
}

// An export carries every manual alert with its tests and its actions by name, of
// every kind, so the bundle re-wires on import.
func TestExportBundle(t *testing.T) {
	m, pg := newTestManager(t)
	pg.answer(sqlListAlerts, fakedb.RowsOf(
		alertSpec{ID: "a1", Name: "Brute force", FractalID: "f1", Webhooks: `[{"id":"w1"}]`, Dicts: `[{"id":"d1","name":"x"}]`}.listRow(),
		alertSpec{ID: "a2", Name: "Brute/force", FractalID: "f1"}.listRow(),
	))
	pg.answer("SELECT id::text, name FROM webhook_actions", fakedb.RowsOf([]driver.Value{"w1", "notify"}))
	pg.answer("SELECT id::text, name FROM dictionary_actions", fakedb.RowsOf([]driver.Value{"d1", "blocklist"}))
	pg.handle("FROM alert_tests WHERE alert_id", func(args []driver.Value) fakedb.PGResult {
		if args[0] != "a1" {
			return fakedb.PGResult{}
		}
		return fakedb.RowsOf([]driver.Value{"t1", "fires", "match", []byte(`[{"status":"500"}]`), int64(4)})
	})

	data, err := m.ExportBundle(context.Background(), "f1", "", "fractal:f1")
	if err != nil {
		t.Fatal(err)
	}
	files := readZip(t, data)

	var manifest BundleManifest
	if err := json.Unmarshal([]byte(files["manifest.json"]), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Alerts != 2 || manifest.Tests != 1 || manifest.Origin != "fractal:f1" || manifest.Format != BundleFormat {
		t.Errorf("manifest = %+v", manifest)
	}
	var first YAMLAlert
	if err := yaml.Unmarshal([]byte(files["alerts/Brute-force.yaml"]), &first); err != nil {
		t.Fatalf("files = %v: %v", keys(files), err)
	}
	if strings.Join(first.ActionNames, ",") != "blocklist,notify" {
		t.Errorf("actions travel by name, every kind included: %v", first.ActionNames)
	}
	if _, ok := files["alerts/Brute-force-2.yaml"]; !ok {
		t.Errorf("two alerts sharing a slug get distinct files: %v", keys(files))
	}
	var tests bundleTests
	if err := json.Unmarshal([]byte(files["tests/Brute-force.json"]), &tests); err != nil {
		t.Fatal(err)
	}
	if tests.Alert != "Brute force" || tests.Tests[0].ID != "" || tests.Tests[0].Position != 0 {
		t.Errorf("tests carry no instance identity: %+v", tests)
	}

	pg.answer("SELECT id::text, name FROM webhook_actions", fakedb.PGResult{Err: errors.New("down")})
	if _, err := m.ExportBundle(context.Background(), "f1", "", ""); err == nil {
		t.Error("a failed name lookup must fail the export rather than drop actions")
	}
}

func keys(m map[string]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The feed alert table pages, filters and sorts in Postgres; every filter binds its
// value rather than splicing it into the SQL.
func TestListFeedAlertsPage(t *testing.T) {
	m, pg := newTestManager(t)
	pg.answer("COUNT(*) OVER () AS total", fakedb.RowsOf(
		[]driver.Value{"a1", "Rule", "d", true, pgArray([]string{"sigma:high"}), "feed-1", "Sigma", "", nil, int64(12), int64(41)},
	))
	page, err := m.ListFeedAlertsPage(context.Background(), FeedAlertQuery{
		PrismID: "p1", Search: "50%_off", Status: "enabled", FeedID: "feed-1", Severity: "HIGH", Label: "attack.t1078",
		Sort: "severity", Dir: "desc", Limit: 9999, Offset: -5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 41 || len(page.Alerts) != 1 || *page.Alerts[0].LastExecutionTimeMs != 12 {
		t.Errorf("page = %+v", page)
	}
	c := pg.Matching("COUNT(*) OVER () AS total")[0]
	for _, want := range []string{"a.prism_id = $1", "a.enabled = true", "a.feed_id = $3", "$4 = ANY(a.labels)", "$5 = ANY(a.labels)",
		"ORDER BY CASE", "DESC, a.name, a.id", "LIMIT $6 OFFSET $7"} {
		if !strings.Contains(c.Query, want) {
			t.Errorf("query lacks %q", want)
		}
	}
	if c.Args[1] != `%50\%\_off%` || c.Args[3] != "sigma:high" || c.Args[5] != int64(25) || c.Args[6] != int64(0) {
		t.Errorf("args = %v", c.Args)
	}

	if _, err := m.ListFeedAlertsPage(context.Background(), FeedAlertQuery{FractalID: "f1", Status: "disabled"}); err != nil {
		t.Fatal(err)
	}
	c = pg.Matching("COUNT(*) OVER () AS total")[1]
	if !strings.Contains(c.Query, "a.fractal_id = $1") || !strings.Contains(c.Query, "a.enabled = false") || !strings.Contains(c.Query, "ORDER BY COALESCE(f.name,''), a.name, a.id") {
		t.Errorf("default query = %s", c.Query)
	}
}

func TestFeedAlertFacetsAndFilteredToggle(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)
	pg.answer("SELECT DISTINCT unnest(a.labels)", fakedb.RowsOf(
		[]driver.Value{"attack.t1078"}, []driver.Value{"feed:legacy"}, []driver.Value{"sigma:high"}, []driver.Value{"windows"},
	))
	pg.answer("GROUP BY 1, 2", fakedb.RowsOf([]driver.Value{"feed-1", "Sigma", int64(30)}, []driver.Value{"feed-2", "Other", int64(5)}))
	facets, err := m.FeedAlertFacetsFor(ctx, "f1", "")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(facets.Labels, ",") != "attack.t1078,windows" {
		t.Errorf("severity and legacy feed labels have their own filters: %v", facets.Labels)
	}
	if facets.Unfiltered != 35 || len(facets.Feeds) != 2 {
		t.Errorf("facets = %+v", facets)
	}
	pg.answer("SELECT DISTINCT unnest(a.labels)", fakedb.PGResult{Err: errors.New("down")})
	if _, err := m.FeedAlertFacetsFor(ctx, "f1", ""); err == nil {
		t.Error("want the label error")
	}

	pg.answer("WHERE id IN (SELECT a.id FROM alerts a WHERE", fakedb.Affected(7))
	n, err := m.BatchToggleFeedAlertsFiltered(ctx, FeedAlertQuery{FractalID: "f1", Label: "windows"}, true, "bob")
	if err != nil || n != 7 {
		t.Errorf("toggled %d %v", n, err)
	}
	c := pg.Matching("WHERE id IN (SELECT a.id FROM alerts a WHERE")[0]
	if c.Args[0] != true || c.Args[1] != "bob" || c.Args[2] != "f1" || c.Args[3] != "windows" || !strings.Contains(c.Query, "$4 = ANY(a.labels)") {
		t.Errorf("the toggle shares the table's predicate: %s %v", c.Query, c.Args)
	}
	if len(pg.Matching(sqlCacheRefresh)) != 1 {
		t.Error("a bulk toggle refreshes the engine")
	}
}

func TestListCandidateRules(t *testing.T) {
	m, pg := newTestManager(t)
	pg.answer("FROM feed_rule_catalog c", fakedb.RowsOf([]driver.Value{
		"feed-1", "Sigma", "rules/x.yml", "X", "high", "stable", "", "", pgArray([]string{"attack.t1003"}),
	}))
	got, err := m.listCandidateRules(context.Background(), "", "p1")
	if err != nil || len(got) != 1 || got[0].Labels[0] != "attack.t1003" {
		t.Errorf("candidates = %+v %v", got, err)
	}
	if c := pg.Matching("FROM feed_rule_catalog")[0]; !strings.Contains(c.Query, "f.prism_id = $1") || c.Args[0] != "p1" {
		t.Errorf("query = %s", c.Query)
	}
}
