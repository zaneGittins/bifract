package alerts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/parser"
)

// A retried delivery is the same firing, so a receiver deduplicating on
// firing_id opens one case however many attempts it took.
func TestSendRetryKeepsFiringID(t *testing.T) {
	var mu sync.Mutex
	var ids []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p WebhookPayload
		_ = json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		ids = append(ids, p.FiringID)
		n := len(ids)
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	webhook := WebhookAction{Name: "hook", URL: srv.URL, RetryCount: 2}
	firing := Firing{ID: "5b0d3f0e-0000-4000-8000-000000000001"}
	result := NewWebhookClient("", allowPrivate).Send(context.Background(), webhook, &Alert{ID: "a1", Name: "T"}, "T", firing, nil)

	if !result.Success || result.AttemptCount != 2 {
		t.Fatalf("want success on the second attempt: %+v", result)
	}
	if len(ids) != 2 || ids[0] != firing.ID || ids[1] != firing.ID {
		t.Errorf("firing_id across attempts = %v, want %s twice", ids, firing.ID)
	}
}

func TestFiringGroup(t *testing.T) {
	cases := []struct {
		name, field, want string
		rows              []map[string]interface{}
	}{
		{"no throttle field", "", "", []map[string]interface{}{{"host": "a"}}},
		{"one value", "host", "a", []map[string]interface{}{{"host": "a"}, {"host": "a"}}},
		{"non-string value", "pid", "42", []map[string]interface{}{{"pid": 42}}},
		{"several values", "host", "", []map[string]interface{}{{"host": "a"}, {"host": "b"}}},
		{"value missing on a row", "host", "", []map[string]interface{}{{"host": "a"}, {"user": "x"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFiring(&Alert{ThrottleField: c.field}, c.rows)
			if f.GroupField != c.field || f.GroupKey != c.want {
				t.Errorf("group = %q/%q, want %q/%q", f.GroupField, f.GroupKey, c.field, c.want)
			}
			if len(f.ID) != 36 {
				t.Errorf("firing ID %q is not a UUID", f.ID)
			}
		})
	}
	if newFiring(&Alert{}, nil).ID == newFiring(&Alert{}, nil).ID {
		t.Error("two firings share an ID")
	}
}

// Labels hold Sigma's attack.* tags verbatim; the payload resolves them against
// the matrix so receivers get IDs and names, not slugs.
func TestAttackTagsFromLabels(t *testing.T) {
	got := attackTags([]string{
		"attack.execution", "attack.t1059.001", "attack.t1086", "attack.g0016",
		"attack.s0002", "attack.c0001", "attack.ta0002", "sigma:high", "product:windows",
	})
	want := AttackTags{
		Tactics:    []AttackRef{{ID: "TA0002", Name: "Execution"}},
		Techniques: []AttackRef{{ID: "T1059.001", Name: "PowerShell"}},
		Groups:     []string{"G0016"},
		Software:   []string{"S0002"},
		Campaigns:  []string{"C0001"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("attack = %+v\nwant     %+v", got, want)
	}

	empty := attackTags([]string{"sigma:high"})
	b, _ := json.Marshal(empty)
	if string(b) != `{"tactics":[],"techniques":[],"groups":[],"software":[],"campaigns":[]}` {
		t.Errorf("an alert without ATT&CK tags keeps the shape: %s", b)
	}
}

func TestTemplateSeesFiringFields(t *testing.T) {
	p := testPayload()
	p.FiringID = "fid"
	p.GroupField = "host.name"
	p.GroupKey = "web-01"
	p.Attack = attackTags([]string{"attack.t1059.001"})
	p.References = []string{"https://example.com/rule"}
	webhook := WebhookAction{BodyMode: BodyModeTemplate,
		BodyTemplate: `{{.FiringID}} {{.GroupField}}={{.GroupKey}} {{range .Attack.Techniques}}{{.ID}}{{end}} {{.References | join ","}}`}

	body, _, err := RenderWebhookBody(webhook, p)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "fid host.name=web-01 T1059.001 https://example.com/rule" {
		t.Errorf("body = %q", body)
	}
}

// One evaluation is one firing: every webhook gets the same firing_id, it is the
// ID of the execution row in the alert's history, and group_key is the delivered
// throttle field value.
func TestProcessResultsCarriesFiring(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	hookA, hookB := newHookServer(t), newHookServer(t)
	alert := &Alert{ID: "a1", Name: "n", FractalID: "f1", ThrottleTimeSeconds: 300, ThrottleField: "host",
		Labels:     []string{"attack.t1059.001"},
		References: []string{"https://example.com/rule"},
		WebhookActions: []WebhookAction{
			{ID: "w1", URL: hookA.URL, Method: "POST", RetryCount: 1, Enabled: true},
			{ID: "w2", URL: hookB.URL, Method: "POST", RetryCount: 1, Enabled: true},
		}}

	if err := e.processAlertResults(context.Background(), alert, []map[string]interface{}{row("l1", "b"), row("l2", "b")}, parser.QueryOptions{}, 1); err != nil {
		t.Fatal(err)
	}

	var a, b WebhookPayload
	if bodies := hookA.received(); len(bodies) != 1 || json.Unmarshal([]byte(bodies[0]), &a) != nil {
		t.Fatalf("hook A bodies = %v", bodies)
	}
	if bodies := hookB.received(); len(bodies) != 1 || json.Unmarshal([]byte(bodies[0]), &b) != nil {
		t.Fatalf("hook B bodies = %v", bodies)
	}
	if a.FiringID == "" || a.FiringID != b.FiringID {
		t.Errorf("firing IDs = %q, %q, want one shared ID", a.FiringID, b.FiringID)
	}
	if exec := pg.args(sqlExecution); len(exec) != 1 || exec[0][11] != a.FiringID {
		t.Errorf("execution row ID = %v, want the firing ID %q", exec, a.FiringID)
	}
	if a.GroupField != "host" || a.GroupKey != "b" {
		t.Errorf("group = %q/%q, want host/b", a.GroupField, a.GroupKey)
	}
	if len(a.Attack.Techniques) != 1 || a.Attack.Techniques[0].ID != "T1059.001" {
		t.Errorf("attack = %+v", a.Attack)
	}
	if !reflect.DeepEqual(a.References, []string{"https://example.com/rule"}) {
		t.Errorf("references = %v", a.References)
	}
}

// A fully throttled evaluation still records its execution under a fresh ID.
func TestThrottledExecutionHasID(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	pg.answer(sqlThrottleRead, fakedb.RowsOf([]driver.Value{globalThrottleKey}))
	alert := &Alert{ID: "a1", Name: "n", FractalID: "f1", ThrottleTimeSeconds: 60}
	if err := e.processAlertResults(context.Background(), alert, []map[string]interface{}{row("l1", "a")}, parser.QueryOptions{}, 1); err != nil {
		t.Fatal(err)
	}
	exec := pg.args(sqlExecution)
	if len(exec) != 1 {
		t.Fatalf("want one execution row, got %d", len(exec))
	}
	if id, _ := exec[0][11].(string); len(id) != 36 {
		t.Errorf("execution ID = %v", exec[0][11])
	}
}

// The engine cache carries references, which the payload sends.
func TestCacheLoadsReferences(t *testing.T) {
	e, pg, _ := newTestEngine(t)
	pg.answer(sqlCacheRefresh, fakedb.RowsOf(alertSpec{ID: "a1", Name: "a1", Enabled: true, FractalID: "f1",
		References: []string{"https://example.com/a", "https://example.com/b"}}.cacheRow()))
	enabled, err := e.refreshAlertsCache(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(enabled) != 1 || strings.Join(enabled[0].References, " ") != "https://example.com/a https://example.com/b" {
		t.Fatalf("references = %+v", enabled)
	}
}

// The editor's preview shows the new fields filled, so template authors see
// their shape before the first real firing.
func TestPreviewShowsFiringFields(t *testing.T) {
	out := (&Manager{}).TestWebhookConfig(context.Background(), WebhookAction{}, false)
	var p WebhookPayload
	if err := json.Unmarshal([]byte(out.RenderedBody), &p); err != nil {
		t.Fatal(err)
	}
	if len(p.FiringID) != 36 || len(p.Attack.Techniques) != 1 || len(p.Attack.Tactics) != 1 || len(p.References) != 1 {
		t.Errorf("preview payload = %+v", p)
	}
	if !strings.Contains(out.RenderedBody, `"group_field":""`) || !strings.Contains(out.RenderedBody, `"group_key":""`) {
		t.Errorf("group fields are always present: %s", out.RenderedBody)
	}
}
