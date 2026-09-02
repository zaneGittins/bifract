//go:build alertsdb

// Manual harness for alert action wiring. Needs a running Bifract Postgres:
//
//	go test -tags alertsdb ./pkg/alerts/ -run TestAlertActions -v
//
// Connection comes from BIFRACT_PG_HOST/PORT/DB/USER/PASSWORD (defaults suit
// docker-compose.dev.yml). Rows it creates are named "ZZ ..." and removed after.
package alerts

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"bifract/pkg/storage"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// liveManager connects to a running Bifract Postgres and picks any existing
// fractal to scope the probe rows to.
func liveManager(t *testing.T) (*Manager, string, context.Context) {
	t.Helper()
	port, err := strconv.Atoi(env("BIFRACT_PG_PORT", "5432"))
	if err != nil {
		t.Fatalf("BIFRACT_PG_PORT: %v", err)
	}
	pg, err := storage.NewPostgresClient(
		env("BIFRACT_PG_HOST", "localhost"), port,
		env("BIFRACT_PG_DB", "bifract"),
		env("BIFRACT_PG_USER", "bifract"),
		env("BIFRACT_PG_PASSWORD", "bifract"),
	)
	if err != nil {
		t.Skipf("no live postgres: %v", err)
	}
	ctx := context.Background()

	var fractalID, user string
	if err := pg.QueryRow(ctx, `SELECT id::text FROM fractals ORDER BY created_at LIMIT 1`).Scan(&fractalID); err != nil {
		t.Skipf("no fractal to scope the probe to: %v", err)
	}
	if err := pg.QueryRow(ctx, `SELECT username FROM users LIMIT 1`).Scan(&user); err != nil {
		t.Skipf("no user to attribute the probe to: %v", err)
	}
	t.Setenv("ZZ_PROBE_USER", user)

	return NewManager(pg, NewEngineWithDicts(pg, nil, nil, "http://localhost"), nil), fractalID, ctx
}

func probeUser() string { return os.Getenv("ZZ_PROBE_USER") }

func mustExec(t *testing.T, m *Manager, ctx context.Context, sql string, args ...interface{}) {
	t.Helper()
	if _, err := m.pg.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// seed creates two webhooks, three fractal actions, one dictionary action and one
// email action. The uneven counts are the point: a join fan-out returns 2x3 of each.
func seed(t *testing.T, m *Manager, ctx context.Context, fractalID string) {
	t.Helper()
	cleanup(t, m, ctx)
	t.Cleanup(func() { cleanup(t, m, ctx) })
	mustExec(t, m, ctx, `INSERT INTO webhook_actions (name, url, fractal_id, enabled) VALUES ('ZZ hook A','http://x/1',$1,true),('ZZ hook B','http://x/2',$1,true)`, fractalID)
	mustExec(t, m, ctx, `INSERT INTO fractal_actions (name, target_fractal_id, fractal_id, enabled) VALUES ('ZZ frac A',$1,$1,true),('ZZ frac B',$1,$1,true),('ZZ frac C',$1,$1,true)`, fractalID)
	mustExec(t, m, ctx, `INSERT INTO dictionary_actions (name, dictionary_name, fractal_id, enabled) VALUES ('ZZ dict A','zzdict',$1,true)`, fractalID)
	mustExec(t, m, ctx, `INSERT INTO email_actions (name, recipients, fractal_id, enabled) VALUES ('ZZ mail A',ARRAY['a@b.c'],$1,true)`, fractalID)
}

func cleanup(t *testing.T, m *Manager, ctx context.Context) {
	t.Helper()
	mustExec(t, m, ctx, `DELETE FROM alerts WHERE name LIKE 'ZZ %'`)
	for _, tbl := range []string{"webhook_actions", "fractal_actions", "dictionary_actions", "email_actions"} {
		mustExec(t, m, ctx, `DELETE FROM `+tbl+` WHERE name LIKE 'ZZ %'`)
	}
}

const probeYAML = `name: ZZ probe alert
description: |2-
  Multi line.
  Second line.
queryString: |2-
  level=error
  | table(host)
alertType: event
severity: high
actionNames:
- "ZZ hook A"
- "ZZ hook B"
- "ZZ frac A"
- "ZZ frac B"
- "ZZ frac C"
- "ZZ dict A"
- "ZZ mail A"
labels: []
enabled: true
throttleTimeSeconds: 0
`

func countsOf(a *Alert) (int, int, int, int) {
	return len(a.WebhookActions), len(a.FractalActions), len(a.DictionaryActionRefs), len(a.EmailActions)
}

// TestAlertActionsSurviveYAMLRoundTrip covers both halves of the action wiring:
// every kind is carried by the YAML format, and no read path multiplies the
// kinds against each other.
func TestAlertActionsSurviveYAMLRoundTrip(t *testing.T) {
	m, fractalID, ctx := liveManager(t)
	seed(t, m, ctx, fractalID)

	created, err := m.ImportFromYAML(ctx, probeYAML, probeUser(), fractalID, "", "")
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	got, err := m.GetAlert(ctx, created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if w, f, d, e := countsOf(got); w != 2 || f != 3 || d != 1 || e != 1 {
		t.Errorf("GetAlert counts: w=%d f=%d d=%d e=%d, want 2/3/1/1", w, f, d, e)
	}
	if got.Description != "Multi line.\nSecond line." {
		t.Errorf("description not preserved: %q", got.Description)
	}
	if got.QueryString != "level=error\n| table(host)" {
		t.Errorf("query string not preserved: %q", got.QueryString)
	}

	// Re-importing the same file must not strip the kinds the update request
	// used to leave empty.
	if _, err := m.ImportFromYAML(ctx, probeYAML, probeUser(), fractalID, "", ""); err != nil {
		t.Fatalf("re-import: %v", err)
	}
	again, err := m.GetAlert(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if w, f, d, e := countsOf(again); w != 2 || f != 3 || d != 1 || e != 1 {
		t.Errorf("re-import counts: w=%d f=%d d=%d e=%d, want 2/3/1/1", w, f, d, e)
	}

	// The engine cache feeds delivery: a fan-out here sends each webhook once
	// per fractal action.
	cached, err := m.engine.refreshAlertsCache(ctx)
	if err != nil {
		t.Fatalf("refresh engine cache: %v", err)
	}
	for _, a := range cached {
		if a.Name != "ZZ probe alert" {
			continue
		}
		if len(a.WebhookActions) != 2 || len(a.FractalActions) != 3 || len(a.EmailActions) != 1 {
			t.Errorf("engine cache counts: w=%d f=%d e=%d, want 2/3/1",
				len(a.WebhookActions), len(a.FractalActions), len(a.EmailActions))
		}
	}

	list, err := m.ListAlerts(ctx, false, fractalID, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, a := range list {
		if a.Name != "ZZ probe alert" {
			continue
		}
		if w, f, d, _ := countsOf(a); w != 2 || f != 3 || d != 1 {
			t.Errorf("ListAlerts counts: w=%d f=%d d=%d, want 2/3/1", w, f, d)
		}
	}
}

func TestAlertActionsRejectUnknownName(t *testing.T) {
	m, fractalID, ctx := liveManager(t)
	seed(t, m, ctx, fractalID)

	bad := strings.Replace(probeYAML, `- "ZZ frac B"`, `- "ZZ nope"`, 1)
	_, err := m.ImportFromYAML(ctx, bad, probeUser(), fractalID, "", "")
	if err == nil {
		t.Fatal("expected an error for an unknown action name")
	}
	if !strings.Contains(err.Error(), "actions not found: ZZ nope") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAlertActionsRejectAmbiguousName covers the one cost of a single flat list:
// the action tables are unique per kind, so the same name can exist twice.
func TestAlertActionsRejectAmbiguousName(t *testing.T) {
	m, fractalID, ctx := liveManager(t)
	seed(t, m, ctx, fractalID)

	// A fractal action sharing a webhook's name.
	mustExec(t, m, ctx, `INSERT INTO fractal_actions (name, target_fractal_id, fractal_id, enabled) VALUES ('ZZ hook A',$1,$1,true)`, fractalID)

	_, err := m.ImportFromYAML(ctx, probeYAML, probeUser(), fractalID, "", "")
	if err == nil {
		t.Fatal("expected an error for a name matching two action kinds")
	}
	if !strings.Contains(err.Error(), `"ZZ hook A" is ambiguous`) ||
		!strings.Contains(err.Error(), "fractal and webhook") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAlertActionsLegacyYAML pins the compatibility contract: a rule file written
// before the other action kinds existed still imports, and gains nothing.
func TestAlertActionsLegacyYAML(t *testing.T) {
	m, fractalID, ctx := liveManager(t)
	seed(t, m, ctx, fractalID)

	legacy := `name: ZZ legacy alert
description: old format
queryString: level=error
alertType: event
actionNames:
- ZZ hook A
labels: []
enabled: true
throttleTimeSeconds: 0
`
	created, err := m.ImportFromYAML(ctx, legacy, probeUser(), fractalID, "", "")
	if err != nil {
		t.Fatalf("legacy import: %v", err)
	}
	got, err := m.GetAlert(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if w, f, d, e := countsOf(got); w != 1 || f != 0 || d != 0 || e != 0 {
		t.Errorf("legacy counts: w=%d f=%d d=%d e=%d, want 1/0/0/0", w, f, d, e)
	}
	if got.WebhookActions[0].Name != "ZZ hook A" {
		t.Errorf("legacy webhook wiring lost: %q", got.WebhookActions[0].Name)
	}
}
