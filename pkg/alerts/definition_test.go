package alerts

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/models"
)

// Everything about a definition that can be judged without the database, for every
// way a definition reaches it. Each refusal is ErrInvalidAlert so a handler answers
// 400 without matching on wording, except a query that does not parse, whose
// wording the editor already keys on.
func TestValidateDefinition(t *testing.T) {
	cases := []struct {
		name    string
		content RevisionContent
		wantErr string // substring; "" means valid
		invalid bool   // whether the error is ErrInvalidAlert
	}{
		{"plain event", RevisionContent{QueryString: `status=500`}, "", false},
		{"explicit event", RevisionContent{AlertType: "event", QueryString: `status=500`}, "", false},
		{"unknown type", RevisionContent{AlertType: "streaming", QueryString: `status=500`}, `unknown alert_type "streaming"`, true},
		{"known severity", RevisionContent{Severity: "high", QueryString: `status=500`}, "", false},
		{"unknown severity", RevisionContent{Severity: `"onmouseover=alert()`, QueryString: `status=500`}, "unknown severity", true},
		{"legacy info severity", RevisionContent{Severity: "info", QueryString: `status=500`}, "unknown severity", true},
		{"unparseable query", RevisionContent{QueryString: `status=500 | groupby(`}, "invalid query syntax", false},
		{"bad throttle field", RevisionContent{QueryString: `a=1`, ThrottleField: "user name"}, "throttle field", true},
		{"negative lag", RevisionContent{QueryString: `a=1`, MaxEventLagSeconds: -1}, "max event lag", true},
		{"unbounded correlation", RevisionContent{QueryString: `* | chain(user) { a="x"; b="y" }`}, "needs a bound", true},

		{"event with aggregation", RevisionContent{QueryString: `* | groupby(user) | count()`}, "cannot use aggregate", true},
		{"event with top", RevisionContent{QueryString: `* | top(image)`}, "cannot use aggregate", true},
		{"compound may aggregate", RevisionContent{AlertType: "compound", QueryString: `* | groupby(user) | count()`, WindowDuration: intPtr(300)}, "", false},
		{"compound without a window", RevisionContent{AlertType: "compound", QueryString: `* | groupby(user)`}, "positive window_duration", true},
		{"compound with a zero window", RevisionContent{AlertType: "compound", QueryString: `a=1`, WindowDuration: intPtr(0)}, "positive window_duration", true},

		{"scheduled", RevisionContent{AlertType: "scheduled", QueryString: `* | count()`, ScheduleCron: strPtr("*/5 * * * *"), QueryWindowSeconds: intPtr(300)}, "", false},
		{"scheduled without cron", RevisionContent{AlertType: "scheduled", QueryString: `a=1`, QueryWindowSeconds: intPtr(300)}, "cron expression", true},
		{"scheduled with blank cron", RevisionContent{AlertType: "scheduled", QueryString: `a=1`, ScheduleCron: strPtr("  "), QueryWindowSeconds: intPtr(300)}, "cron expression", true},
		{"scheduled without window", RevisionContent{AlertType: "scheduled", QueryString: `a=1`, ScheduleCron: strPtr("@hourly")}, "query_window_seconds", true},
		{"scheduled with a bad cron", RevisionContent{AlertType: "scheduled", QueryString: `a=1`, ScheduleCron: strPtr("every tuesday"), QueryWindowSeconds: intPtr(60)}, "invalid cron expression", true},
		{"scheduled rejects seconds-field cron", RevisionContent{AlertType: "scheduled", QueryString: `a=1`, ScheduleCron: strPtr("0 */5 * * * *"), QueryWindowSeconds: intPtr(60)}, "invalid cron expression", true},

		{"http references", RevisionContent{QueryString: `a=1`, References: []string{"https://attack.mitre.org/techniques/T1059/", "http://example.com"}}, "", false},
		{"javascript reference", RevisionContent{QueryString: `a=1`, References: []string{"https://ok.example", "javascript:alert(1)"}}, "reference 2", true},
		{"data reference", RevisionContent{QueryString: `a=1`, References: []string{"data:text/html,x"}}, "only http and https", true},
		{"schemeless reference", RevisionContent{QueryString: `a=1`, References: []string{"example.com/rule"}}, "only http and https", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := validateDefinition(tc.content)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if parsed == nil {
					t.Fatal("a valid definition must return its parsed query")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.wantErr)
			}
			if got := errors.Is(err, ErrInvalidAlert); got != tc.invalid {
				t.Errorf("errors.Is(ErrInvalidAlert) = %v, want %v", got, tc.invalid)
			}
		})
	}
}

func TestQueryHasAggregation(t *testing.T) {
	cases := map[string]bool{
		`status=500`:                             false,
		`* | groupby(user)`:                      true,
		`* | groupby(user) | count()`:            true,
		`* | table(user, host)`:                  false,
		`* | table(lower(user))`:                 false,
		`status=500 | sort(timestamp)`:           false,
		`* | stddev(bytes)`:                      true,
		`* | percentile(field=bytes)`:            true,
		`* | groupby(user, function=sum(bytes))`: true,
	}
	for q, want := range cases {
		if got := queryHasAggregation(mustParse(t, q)); got != want {
			t.Errorf("%s: got %v, want %v", q, got, want)
		}
	}
	if queryHasAggregation(nil) {
		t.Error("a nil pipeline has no aggregation")
	}
}

// A write must be refused for what is wrong with the definition before anything
// touches the database. These managers have no database at all: reaching one
// would panic rather than return.
func TestCreateAndUpdateRejectBeforeTouchingTheDatabase(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()

	bad := []AlertUpdateRequest{
		{Name: "n", QueryString: `a=1`, AlertType: "bogus"},
		{Name: "n", QueryString: `* | groupby(user)`},
		{Name: "n", QueryString: `a=1`, AlertType: "compound"},
		{Name: "n", QueryString: `a=1`, AlertType: "scheduled", ScheduleCron: strPtr("@daily")},
		{Name: "n", QueryString: `a=(`},
		{Name: "n", QueryString: `a=1`, ThrottleField: "a b"},
	}
	for _, req := range bad {
		if _, err := m.CreateAlert(ctx, AlertCreateRequest(req), "u", "f1", ""); err == nil {
			t.Errorf("create accepted %+v", req)
		}
		// Update used to skip the type check and store an alert no engine evaluates.
		if _, err := m.UpdateAlert(ctx, "a1", req, "u"); err == nil {
			t.Errorf("update accepted %+v", req)
		}
	}
}

// A proposal is judged when submitted, so a reviewer is never asked to approve a
// definition merge would then fail to write.
func TestProposalValidatesTheDefinition(t *testing.T) {
	in := ChangeRequestInput{Kind: ChangeCreate, Title: "t", Content: &RevisionContent{
		Name: "n", QueryString: `* | groupby(user)`, AlertType: "event",
	}}
	err := in.Validate()
	if !errors.Is(err, ErrInvalidAlert) {
		t.Fatalf("an event proposal that aggregates must be refused as invalid, got %v", err)
	}

	rec := httptest.NewRecorder()
	(&Handler{}).changeRequestError(rec, err)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("an invalid proposal is the caller's to fix: status %d, want 400", rec.Code)
	}

	rec = httptest.NewRecorder()
	(&Handler{}).changeRequestError(rec, errors.New("invalid query syntax: unexpected EOF"))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("an unparseable proposal query: status %d, want 400", rec.Code)
	}

	// A delete carries no definition to judge.
	del := ChangeRequestInput{Kind: ChangeDelete, AlertID: "a1", Summary: "obsolete"}
	if err := del.Validate(); err != nil {
		t.Errorf("a delete proposal has no definition: %v", err)
	}
}

// Merge claims a proposal before applying it, so a definition that cannot be written
// has to stop it beforehand, or the proposal is left marked merged with nothing
// applied. Proposals opened before a rule was enforced exist in the table.
func TestMergeBlockerRefusesAnInapplicableDefinition(t *testing.T) {
	cr := crWith("hash-1", Review{Decision: ReviewApprove, Reviewer: "r", ContentHash: "hash-1"})
	cr.Kind = ChangeCreate
	cr.Content = &RevisionContent{Name: "n", QueryString: `a=1`, AlertType: "compound"}

	blocker := cr.MergeBlocker(gateCfg(), "")
	if !strings.Contains(blocker, "cannot be applied") || !strings.Contains(blocker, "window_duration") {
		t.Errorf("blocker = %q, want it to name the definition problem", blocker)
	}

	cr.Content.WindowDuration = intPtr(60)
	if blocker := cr.MergeBlocker(gateCfg(), ""); blocker != "" {
		t.Errorf("a valid, approved create should merge, got %q", blocker)
	}

	// A closed proposal reports its status first; the definition no longer matters.
	cr.Status = ChangeMerged
	cr.Content.WindowDuration = nil
	if blocker := cr.MergeBlocker(gateCfg(), ""); blocker != "this proposal is merged" {
		t.Errorf("got %q", blocker)
	}
}

// The import endpoints answer 400 for a definition problem raised after parsing, such
// as a compound alert document with no window, rather than a 500.
func TestImportRefusalCoversInvalidDefinitions(t *testing.T) {
	_, err := validateDefinition(RevisionContent{QueryString: `a=1`, AlertType: "compound"})
	if !importRefusal(err) {
		t.Errorf("%v should be refused as the document's fault", err)
	}
}

func TestScopedCountQuery(t *testing.T) {
	q, args := scopedCountQuery("webhook_actions", []string{"a"}, "f1", "")
	if q != "SELECT COUNT(*) FROM webhook_actions WHERE id = ANY($1) AND fractal_id = $2" || len(args) != 2 || args[1] != "f1" {
		t.Errorf("fractal scope: %q %v", q, args)
	}
	q, args = scopedCountQuery("email_actions", []string{"a"}, "f1", "p1")
	if !strings.HasSuffix(q, "AND prism_id = $2") || args[1] != "p1" {
		t.Errorf("a prism scope wins: %q %v", q, args)
	}
	// No scope must match nothing, never everything.
	q, args = scopedCountQuery("fractal_actions", []string{"a"}, "", "")
	if !strings.HasSuffix(q, "AND FALSE") || len(args) != 1 {
		t.Errorf("unscoped: %q %v", q, args)
	}
}

func TestNullableID(t *testing.T) {
	if nullableID("") != nil {
		t.Error("an empty scope id must be stored as NULL")
	}
	if nullableID("f1") != "f1" {
		t.Error("a set scope id is stored as is")
	}
}

func TestResolvedActionsGroupsByKind(t *testing.T) {
	var r resolvedActions
	r.add("webhook", "w1")
	r.add("fractal", "f1")
	r.add("dictionary", "d1")
	r.add("email", "e1")
	r.add("email", "e2")
	r.add("sms", "x") // unknown kinds are dropped, not misfiled
	if len(r.Webhook) != 1 || len(r.Fractal) != 1 || len(r.Dictionary) != 1 || strings.Join(r.Email, ",") != "e1,e2" {
		t.Errorf("got %+v", r)
	}
}

// A document the import cannot use is refused before any lookup, and the refusal
// reads as the document's fault.
func TestResolveYAMLImportRefusesBadDocuments(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()
	cases := map[string]string{
		"name: [unclosed":                      "failed to parse YAML",
		"queryString: status=500":              "alert name is required",
		"name: n":                              "query string is required",
		"name: n\nqueryString: 'status=500 |'": "invalid query syntax",
	}
	for doc, want := range cases {
		_, err := m.resolveYAMLImport(ctx, doc, "f1", "", "")
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: got %v, want %q", doc, err, want)
			continue
		}
		if !importRefusal(err) {
			t.Errorf("%q: %v should answer 400", doc, err)
		}
	}
}

func TestSigmaDescriptionKeepsProvenance(t *testing.T) {
	req, err := sigmaImportRequest(sigmaFixture, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Detects a download cradle", "Sigma ID: 3b6ab547-8ec2-4991-b9d2-2b06702a48d7", "Author: Test Author"} {
		if !strings.Contains(req.Description, want) {
			t.Errorf("description %q is missing %q", req.Description, want)
		}
	}
	if len(req.References) != 1 || req.AlertType != "event" {
		t.Errorf("references %v, type %q", req.References, req.AlertType)
	}
}

func TestSigmaImportReportsTranslationFailures(t *testing.T) {
	if _, err := sigmaImportRequest("title: x\nlogsource: {}\ndetection: [", nil); err == nil || !strings.Contains(err.Error(), "Sigma") {
		t.Errorf("an unparseable rule: got %v", err)
	}
}

// The scope-level checks a manager makes before reaching its tables.
func TestManagerScopeGuards(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()

	if p, err := m.ListPolicies(ctx, "", ""); err != nil || len(p) != 0 {
		t.Errorf("an unscoped rule set is empty, not an error: %v %v", p, err)
	}
	if _, err := m.ReplacePolicies(ctx, "", "", nil, "u"); err == nil {
		t.Error("policies cannot be saved without a scope")
	}
	if _, err := m.ReplacePolicies(ctx, "f1", "", []Policy{{Field: "nope"}}, "u"); err == nil || !strings.Contains(err.Error(), "rule 1") {
		t.Errorf("an invalid rule set must be refused before writing, got %v", err)
	}

	if cfg, err := m.GateConfigFor(ctx, "", ""); err != nil || cfg != DefaultGateConfig() {
		t.Errorf("an unscoped gate is the default: %+v %v", cfg, err)
	}
	if _, err := m.SaveGateConfig(ctx, "f1", "", GateConfig{MinApprovals: 0}, "u"); err == nil {
		t.Error("an invalid gate config must be refused before writing")
	}
	if _, err := m.SaveGateConfig(ctx, "", "", DefaultGateConfig(), "u"); err == nil {
		t.Error("a gate config cannot be saved without a scope")
	}
	if h, err := m.headHashFor(ctx, ""); err != nil || h != "" {
		t.Errorf("a create has no head: %q %v", h, err)
	}

	// The gate: an approved merge, a feed alert and an ungated scope all pass.
	if err := m.requireGate(withGateBypass(ctx), "f1", "", ChangeUpdate, false); err != nil {
		t.Errorf("a merge is the approval: %v", err)
	}
	if err := m.requireGate(ctx, "f1", "", ChangeUpdate, true); err != nil {
		t.Errorf("feed alerts are exempt: %v", err)
	}
	if err := m.requireGate(ctx, "", "", ChangeCreate, false); err != nil {
		t.Errorf("an unscoped write has no gate config: %v", err)
	}
	if err := m.enforcePolicies(ctx, "", "", PolicySubject{}); err != nil {
		t.Errorf("no scope means no rules: %v", err)
	}

	proposed := []AlertTest{{Name: "t"}}
	if got, err := m.testsForPolicy(ctx, "a1", &proposed); err != nil || len(got) != 1 {
		t.Errorf("tests being saved are the ones judged: %v %v", got, err)
	}
}

func TestActionCreatesRequireExactlyOneScope(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()
	for _, scope := range [][2]string{{"", ""}, {"f1", "p1"}} {
		if _, err := m.CreateWebhookAction(ctx, WebhookCreateRequest{Name: "n", URL: "https://x"}, "u", scope[0], scope[1]); err == nil {
			t.Errorf("webhook with scope %v accepted", scope)
		}
		if _, err := m.CreateFractalAction(ctx, FractalActionCreateRequest{Name: "n", TargetFractalID: "t"}, "u", scope[0], scope[1]); err == nil {
			t.Errorf("fractal action with scope %v accepted", scope)
		}
	}
}

func TestActionWritesValidateBeforeTheDatabase(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()

	if _, err := m.CreateWebhookAction(ctx, WebhookCreateRequest{Name: "n", URL: "  "}, "u", "f1", ""); err == nil {
		t.Error("a webhook needs a URL")
	}
	if _, err := m.CreateWebhookAction(ctx, WebhookCreateRequest{Name: "n", URL: "https://x", BodyMode: "template"}, "u", "f1", ""); err == nil {
		t.Error("template mode needs a template")
	}
	for _, u := range []string{"ftp://x/hook", "https://user:pw@x/hook"} {
		if _, err := m.CreateWebhookAction(ctx, WebhookCreateRequest{Name: "n", URL: u}, "u", "f1", ""); err == nil {
			t.Errorf("create accepted webhook URL %q", u)
		}
		if _, err := m.UpdateWebhookAction(ctx, "w1", WebhookUpdateRequest{Name: "n", URL: u}); err == nil {
			t.Errorf("update accepted webhook URL %q", u)
		}
	}
	if _, err := m.UpdateWebhookAction(ctx, "w1", WebhookUpdateRequest{URL: "https://x", BodyMode: "template", BodyTemplate: "{{ .Bad"}); err == nil ||
		!strings.Contains(err.Error(), "template syntax error") {
		t.Errorf("a broken template must be refused on update, got %v", err)
	}

	for _, req := range []FractalActionCreateRequest{{TargetFractalID: "t"}, {Name: "n"}} {
		if _, err := m.CreateFractalAction(ctx, req, "u", "f1", ""); err == nil {
			t.Errorf("create accepted %+v", req)
		}
		if _, err := m.UpdateFractalAction(ctx, "fa1", FractalActionUpdateRequest(req)); err == nil {
			t.Errorf("update accepted %+v", req)
		}
	}
}

func TestProposalWritesValidateBeforeTheDatabase(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()

	if _, err := m.SubmitChangeRequest(ctx, "", "f1", "", ChangeRequestInput{Kind: "rename"}, "u"); err == nil {
		t.Error("an unknown kind must be refused")
	}
	if _, err := m.SaveDraft(ctx, "", "f1", "", ChangeRequestInput{Kind: ChangeDelete, AlertID: "a1"}, "u"); err == nil {
		t.Error("a draft is a create or an update")
	}
	if _, err := m.SaveDraft(ctx, "", "f1", "", ChangeRequestInput{}, "u"); err == nil {
		t.Error("a draft needs a definition")
	}
}

// A model regenerating its detection query must never write one the engine cannot
// parse: the next cache refresh would drop the alert.
func TestUpdateLinkedAlertRefusesAnUnparseableQuery(t *testing.T) {
	err := (&Manager{}).UpdateLinkedAlert(context.Background(), "a1", models.LinkedAlertSpec{QueryString: "a=("})
	if err == nil || !strings.Contains(err.Error(), "generated query is invalid") {
		t.Errorf("got %v", err)
	}
}

// Feed rules come from repos we do not control, so a bad reference is dropped
// rather than refusing an otherwise good rule.
func TestHTTPReferencesDropsUnsafeSchemes(t *testing.T) {
	got := httpReferences([]string{"https://a.example/x", "javascript:alert(1)", " JavaScript:alert(1)", "vbscript:x", "http://b.example", "notaurl"})
	want := []string{"https://a.example/x", "http://b.example"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("httpReferences = %q, want %q", got, want)
	}
	if got := httpReferences(nil); got == nil || len(got) != 0 {
		t.Fatalf("httpReferences(nil) = %#v, want an empty slice", got)
	}
}
