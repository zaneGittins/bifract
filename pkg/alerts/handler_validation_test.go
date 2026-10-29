package alerts

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/storage"
)

// caller describes who is asking and in which scope, as the auth middleware leaves it
// in the request context.
type caller struct {
	user        *storage.User
	fractalRole string
	prismRole   string
	fractal     string
	prism       string
	apiKey      bool
}

var (
	nobody  = caller{}
	viewer  = caller{user: &storage.User{Username: "vic"}, fractalRole: "viewer", fractal: "f1"}
	analyst = caller{user: &storage.User{Username: "ana"}, fractalRole: "analyst", fractal: "f1"}
	// unscoped has a role but declared no fractal or prism.
	unscoped = caller{user: &storage.User{Username: "ana"}, fractalRole: "analyst"}
	admin    = caller{user: &storage.User{Username: "root", IsAdmin: true}, fractal: "f1"}
)

func (c caller) request(method, body string, params map[string]string) *http.Request {
	r := httptest.NewRequest(method, "/x", strings.NewReader(body))
	ctx := r.Context()
	if c.user != nil {
		ctx = context.WithValue(ctx, "user", c.user)
	}
	if c.fractalRole != "" {
		ctx = context.WithValue(ctx, "fractal_role", c.fractalRole)
	}
	if c.prismRole != "" {
		ctx = context.WithValue(ctx, "prism_role", c.prismRole)
	}
	if c.fractal != "" {
		ctx = context.WithValue(ctx, "selected_fractal", c.fractal)
	}
	if c.prism != "" {
		ctx = context.WithValue(ctx, "selected_prism", c.prism)
	}
	if c.apiKey {
		ctx = context.WithValue(ctx, "auth_type", "api_key")
	}
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	ctx = context.WithValue(ctx, chi.RouteCtxKey, rctx)
	return r.WithContext(ctx)
}

type handlerCase struct {
	name    string
	handler func(h *Handler) http.HandlerFunc
	as      caller
	body    string
	params  map[string]string
	status  int
	message string // substring of the error, when set
}

// newTestHandler has a manager with no database. Every case below must be answered
// before the handler reaches storage, or it panics.
func newTestHandler() *Handler { return &Handler{manager: &Manager{}} }

func runHandlerCases(t *testing.T, cases []handlerCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.handler(newTestHandler())(rec, tc.as.request(http.MethodPost, tc.body, tc.params))
			if rec.Code != tc.status {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.message != "" && !strings.Contains(rec.Body.String(), tc.message) {
				t.Errorf("body %s does not mention %q", rec.Body.String(), tc.message)
			}
		})
	}
}

func TestAlertHandlersRejectBadRequests(t *testing.T) {
	createAlert := func(h *Handler) http.HandlerFunc { return h.HandleCreateAlert }
	importYAML := func(h *Handler) http.HandlerFunc { return h.HandleImportYAML }
	noID := map[string]string{}

	runHandlerCases(t, []handlerCase{
		{"list needs a role", func(h *Handler) http.HandlerFunc { return h.HandleListAlerts }, nobody, "", nil, 403, ""},
		// An empty scope reaching the manager would list every alert in every fractal.
		{"list needs a scope", func(h *Handler) http.HandlerFunc { return h.HandleListAlerts }, caller{user: viewer.user, fractalRole: "viewer"}, "", nil, 400, "No fractal or prism selected"},

		{"create needs a user", createAlert, nobody, `{}`, nil, 401, ""},
		{"create needs analyst", createAlert, viewer, `{}`, nil, 403, ""},
		{"create needs JSON", createAlert, analyst, `{`, nil, 400, "Invalid request body"},
		{"create needs a name", createAlert, analyst, `{"name":" ","query_string":"a=1"}`, nil, 400, "name is required"},
		{"create needs a query", createAlert, analyst, `{"name":"n"}`, nil, 400, "Query string is required"},
		{"create needs a scope", createAlert, unscoped, `{"name":"n","query_string":"a=1"}`, nil, 400, "No fractal or prism selected"},
		{"create refuses an invalid type", createAlert, analyst, `{"name":"n","query_string":"a=1","alert_type":"bogus"}`, nil, 400, "unknown alert_type"},
		{"create refuses aggregation in an event alert", createAlert, analyst, `{"name":"n","query_string":"* | groupby(user)"}`, nil, 400, "cannot use aggregate"},
		{"create refuses a compound alert with no window", createAlert, analyst, `{"name":"n","query_string":"a=1","alert_type":"compound"}`, nil, 400, "window_duration"},
		{"create refuses a bad query", createAlert, analyst, `{"name":"n","query_string":"a=("}`, nil, 400, "invalid query syntax"},

		{"get needs an id", func(h *Handler) http.HandlerFunc { return h.HandleGetAlert }, viewer, "", noID, 400, "Alert ID is required"},
		{"update needs an id", func(h *Handler) http.HandlerFunc { return h.HandleUpdateAlert }, analyst, "", noID, 400, "Alert ID is required"},
		{"delete needs an id", func(h *Handler) http.HandlerFunc { return h.HandleDeleteAlert }, analyst, "", noID, 400, "Alert ID is required"},
		{"executions need an id", func(h *Handler) http.HandlerFunc { return h.HandleGetExecutions }, viewer, "", noID, 400, "Alert ID is required"},
		{"activity needs an id", func(h *Handler) http.HandlerFunc { return h.HandleGetActivity }, viewer, "", noID, 400, "Alert ID is required"},
		{"duplicate needs a user", func(h *Handler) http.HandlerFunc { return h.HandleDuplicateAlert }, nobody, "", nil, 401, ""},
		{"toggle feed alert needs a user", func(h *Handler) http.HandlerFunc { return h.HandleToggleFeedAlert }, nobody, "", nil, 401, ""},

		{"import needs a user", importYAML, nobody, "x", nil, 401, ""},
		{"import needs analyst", importYAML, viewer, "x", nil, 403, ""},
		{"import needs content", importYAML, analyst, "   ", nil, 400, "YAML content is required"},
		{"import needs a scope", importYAML, unscoped, "name: n", nil, 400, "No fractal or prism selected"},
	})
}

func TestImportYAMLAcceptsJSONEnvelope(t *testing.T) {
	h := newTestHandler()
	for body, want := range map[string]string{
		`{`:                     "Invalid request body",
		`{"yaml_content":"  "}`: "YAML content is required",
		// Parsed and refused for its content before any lookup.
		`{"yaml_content":"name: n"}`: "query string is required",
	} {
		r := analyst.request(http.MethodPost, body, nil)
		r.Header.Set("Content-Type", "application/json; charset=utf-8")
		rec := httptest.NewRecorder()
		h.HandleImportYAML(rec, r)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), want) {
			t.Errorf("%s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestBatchToggleHandlersBoundTheRequest(t *testing.T) {
	feed := func(h *Handler) http.HandlerFunc { return h.HandleBatchToggleFeedAlerts }
	plain := func(h *Handler) http.HandlerFunc { return h.HandleBatchToggleAlerts }
	tooMany := `{"alert_ids":[` + strings.TrimSuffix(strings.Repeat(`"x",`, 5001), ",") + `]}`

	runHandlerCases(t, []handlerCase{
		{"feed toggle needs analyst", feed, viewer, `{}`, nil, 403, ""},
		{"feed toggle needs JSON", feed, analyst, `{`, nil, 400, ""},
		{"feed toggle needs ids or a filter", feed, analyst, `{"enabled":true}`, nil, 400, "alert_ids or filter required"},
		{"feed toggle caps ids", feed, analyst, tooMany, nil, 400, "max 5000"},
		// A filter is resolved against the caller's scope; without one it would touch
		// every feed alert everywhere.
		{"feed toggle by filter needs a scope", feed, unscoped, `{"filter":{"status":"all"}}`, nil, 400, "No fractal or prism selected"},

		{"toggle needs analyst", plain, viewer, `{}`, nil, 403, ""},
		{"toggle needs JSON", plain, analyst, `{`, nil, 400, ""},
		{"toggle needs ids", plain, analyst, `{"alert_ids":[]}`, nil, 400, "alert_ids required"},
		{"toggle caps ids", plain, analyst, tooMany, nil, 400, "max 5000"},
		{"feed toggle by ids needs a scope", feed, unscoped, `{"alert_ids":["a1"]}`, nil, 400, "No fractal or prism selected"},
		{"toggle needs a scope", plain, unscoped, `{"alert_ids":["a1"]}`, nil, 400, "No fractal or prism selected"},
	})
}

func TestActionHandlersRejectBadRequests(t *testing.T) {
	noID := map[string]string{}
	runHandlerCases(t, []handlerCase{
		{"list webhooks needs a scope", func(h *Handler) http.HandlerFunc { return h.HandleListWebhooks }, unscoped, "", nil, 400, "No fractal or prism selected"},
		{"create webhook needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleCreateWebhook }, admin, `{`, nil, 400, ""},
		{"create webhook needs a name", func(h *Handler) http.HandlerFunc { return h.HandleCreateWebhook }, admin, `{"url":"https://x"}`, nil, 400, "name is required"},
		{"create webhook needs a URL", func(h *Handler) http.HandlerFunc { return h.HandleCreateWebhook }, admin, `{"name":"n"}`, nil, 400, "URL is required"},
		{"create webhook needs a scope", func(h *Handler) http.HandlerFunc { return h.HandleCreateWebhook }, unscoped, `{"name":"n","url":"https://x"}`, nil, 400, "No fractal or prism selected"},
		{"get webhook needs an id", func(h *Handler) http.HandlerFunc { return h.HandleGetWebhook }, admin, "", noID, 400, "Webhook ID is required"},
		{"update webhook needs an id", func(h *Handler) http.HandlerFunc { return h.HandleUpdateWebhook }, admin, "", noID, 400, "Webhook ID is required"},
		{"delete webhook needs an id", func(h *Handler) http.HandlerFunc { return h.HandleDeleteWebhook }, admin, "", noID, 400, "Webhook ID is required"},
		{"test webhook needs an id", func(h *Handler) http.HandlerFunc { return h.HandleTestWebhook }, admin, "", noID, 400, "Webhook ID is required"},
		{"test config needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleTestWebhookConfig }, admin, `{`, nil, 400, ""},
		{"test send needs a URL", func(h *Handler) http.HandlerFunc { return h.HandleTestWebhookConfig }, admin, `{"send":true}`, nil, 400, "URL is required to send"},
		{"test config needs a scope", func(h *Handler) http.HandlerFunc { return h.HandleTestWebhookConfig }, unscoped, `{}`, nil, 400, "No fractal or prism selected"},

		{"list fractal actions needs a scope", func(h *Handler) http.HandlerFunc { return h.HandleListFractalActions }, unscoped, "", nil, 400, ""},
		{"create fractal action needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleCreateFractalAction }, admin, `{`, nil, 400, ""},
		{"create fractal action needs a name", func(h *Handler) http.HandlerFunc { return h.HandleCreateFractalAction }, admin, `{"target_fractal_id":"t"}`, nil, 400, "name is required"},
		{"create fractal action needs a target", func(h *Handler) http.HandlerFunc { return h.HandleCreateFractalAction }, admin, `{"name":"n"}`, nil, 400, "Target fractal ID is required"},
		{"create fractal action needs a scope", func(h *Handler) http.HandlerFunc { return h.HandleCreateFractalAction }, unscoped, `{"name":"n","target_fractal_id":"t"}`, nil, 400, "No fractal or prism selected"},
		{"get fractal action needs an id", func(h *Handler) http.HandlerFunc { return h.HandleGetFractalAction }, admin, "", noID, 400, ""},
		{"update fractal action needs an id", func(h *Handler) http.HandlerFunc { return h.HandleUpdateFractalAction }, admin, "", noID, 400, ""},
		{"delete fractal action needs an id", func(h *Handler) http.HandlerFunc { return h.HandleDeleteFractalAction }, admin, "", noID, 400, ""},

		{"list email actions needs a scope", func(h *Handler) http.HandlerFunc { return h.HandleListEmailActions }, unscoped, "", nil, 400, ""},
		{"create email action needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleCreateEmailAction }, admin, `{`, nil, 400, ""},
		{"create email action needs a scope", func(h *Handler) http.HandlerFunc { return h.HandleCreateEmailAction }, unscoped, `{}`, nil, 400, ""},

		{"smtp needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleUpdateSMTPSettings }, admin, `{`, nil, 400, ""},
		{"smtp needs a host", func(h *Handler) http.HandlerFunc { return h.HandleUpdateSMTPSettings }, admin, `{"from_address":"a@b"}`, nil, 400, "SMTP host is required"},
		{"smtp needs a from address", func(h *Handler) http.HandlerFunc { return h.HandleUpdateSMTPSettings }, admin, `{"host":"smtp"}`, nil, 400, "From address is required"},
	})
}

// The editor's Test button previews an unsaved template, defaulting to an alert link
// when the request does not say otherwise.
func TestHandleTestWebhookConfigRendersWithoutSending(t *testing.T) {
	body := `{"name":"n","body_mode":"TEMPLATE","body_template":"{{.AlertName}}|{{.MatchCount}}","content_type":"text/plain"}`
	rec := httptest.NewRecorder()
	newTestHandler().HandleTestWebhookConfig(rec, admin.request(http.MethodPost, body, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Data WebhookTestResult `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Data.RenderedBody != "Test Alert|2" || resp.Data.Sent || resp.Data.ContentType != "text/plain" {
		t.Errorf("preview = %+v", resp.Data)
	}
}

func TestTestRunHandlers(t *testing.T) {
	// No runner wired: the endpoint says so instead of failing obscurely.
	rec := httptest.NewRecorder()
	newTestHandler().HandleRunTests(rec, analyst.request(http.MethodPost, `{}`, nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("no runner: status %d", rec.Code)
	}

	withRunner := func(h *Handler) http.HandlerFunc {
		h.testRunner = &TestRunner{ch: new(storage.ClickHouseClient), sessions: map[string]*testSession{}}
		return h.HandleRunTests
	}
	runHandlerCases(t, []handlerCase{
		{"run needs analyst", withRunner, viewer, `{}`, nil, 403, ""},
		{"run needs JSON", withRunner, analyst, `{`, nil, 400, ""},
		{"run needs a query", withRunner, analyst, `{"session_id":"s"}`, nil, 400, "Query string is required"},
		{"run needs a session", withRunner, analyst, `{"query_string":"a=1"}`, nil, 400, "Session ID is required"},
		{"run refuses a bad corpus", withRunner, analyst, `{"session_id":"s","query_string":"a=1","tests":[{"name":"","expectation":"match"}]}`, nil, 400, "test name is required"},
		{"run refuses a bad query", withRunner, analyst, `{"session_id":"s","query_string":"a=(","tests":[{"name":"t","expectation":"match","events":[{"a":1}]}]}`, nil, 400, "invalid query syntax"},
		{"release needs analyst", func(h *Handler) http.HandlerFunc { return h.HandleReleaseTestSession }, viewer, "", nil, 403, ""},
		{"release needs a session", func(h *Handler) http.HandlerFunc { return h.HandleReleaseTestSession }, analyst, "", nil, 400, "Session ID is required"},
		{"release of an unknown session succeeds", func(h *Handler) http.HandlerFunc { return h.HandleReleaseTestSession }, analyst, "", map[string]string{"sessionId": "s"}, 200, ""},
		{"list tests needs an id", func(h *Handler) http.HandlerFunc { return h.HandleListTests }, viewer, "", map[string]string{}, 400, ""},
	})

	// The runner keys sessions by principal, but the client only ever sees its own id.
	rec = httptest.NewRecorder()
	withRunner(newTestHandler())(rec, analyst.request(http.MethodPost, `{"session_id":"editor-1","query_string":"a=1","tests":[]}`, nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"session_id":"editor-1"`) {
		t.Errorf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRevisionHandlersNeedAnAlert(t *testing.T) {
	noID := map[string]string{}
	runHandlerCases(t, []handlerCase{
		{"list revisions", func(h *Handler) http.HandlerFunc { return h.HandleListRevisions }, viewer, "", noID, 400, "Alert ID is required"},
		{"get revision", func(h *Handler) http.HandlerFunc { return h.HandleGetRevision }, viewer, "", noID, 400, "Alert ID is required"},
		{"restore revision", func(h *Handler) http.HandlerFunc { return h.HandleRestoreRevision }, analyst, "", noID, 400, "Alert ID is required"},
		{"draft for alert", func(h *Handler) http.HandlerFunc { return h.HandleDraftForAlert }, analyst, "", noID, 400, "Alert ID is required"},
	})
}

func TestPolicyAndGateHandlersRejectBadRequests(t *testing.T) {
	analystKey := caller{user: &storage.User{Username: "apikey_1"}, fractalRole: "analyst", fractal: "f1", apiKey: true}

	runHandlerCases(t, []handlerCase{
		{"policies need a scope", func(h *Handler) http.HandlerFunc { return h.HandleListPolicies }, unscoped, "", nil, 400, "No fractal or prism selected"},
		{"policies need a user", func(h *Handler) http.HandlerFunc { return h.HandleListPolicies }, caller{fractal: "f1", fractalRole: "admin"}, "", nil, 403, ""},
		// An API key's role applies to its own scope, and a rule set is admin-only.
		{"saving policies needs admin", func(h *Handler) http.HandlerFunc { return h.HandleSavePolicies }, analystKey, `{}`, nil, 403, ""},
		{"saving policies on a prism checks the prism role", func(h *Handler) http.HandlerFunc { return h.HandleSavePolicies },
			caller{user: &storage.User{Username: "p"}, prismRole: "analyst", prism: "p1"}, `{}`, nil, 403, ""},
		{"saving policies needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleSavePolicies }, admin, `{`, nil, 400, ""},
		{"saving an invalid rule set", func(h *Handler) http.HandlerFunc { return h.HandleSavePolicies }, admin,
			`{"policies":[{"field":"nope","operator":"not_empty","severity":"warn","message":"m"}]}`, nil, 400, "rule 1"},
		{"evaluating needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleEvaluatePolicies }, admin, `{`, nil, 400, ""},
		{"importing needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleImportPolicies }, admin, `{`, nil, 400, ""},
		{"importing an empty document", func(h *Handler) http.HandlerFunc { return h.HandleImportPolicies }, admin, `{"content":"rules: []"}`, nil, 400, "no rules"},

		{"gate config needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleSaveGateConfig }, admin, `{`, nil, 400, ""},
		{"gate config is bounded", func(h *Handler) http.HandlerFunc { return h.HandleSaveGateConfig }, admin, `{"enabled":true,"min_approvals":11}`, nil, 400, "at most 10"},
		{"proposal needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleSubmitChangeRequest }, admin, `{`, nil, 400, ""},
		{"proposal needs a kind", func(h *Handler) http.HandlerFunc { return h.HandleSubmitChangeRequest }, admin, `{"kind":"rename"}`, nil, 400, "kind must be"},
		{"proposal is judged on submit", func(h *Handler) http.HandlerFunc { return h.HandleSubmitChangeRequest }, admin,
			`{"kind":"create","title":"t","content":{"name":"n","query_string":"* | groupby(user)","alert_type":"event"}}`, nil, 400, "cannot use aggregate"},
		{"proposal by id needs an id", func(h *Handler) http.HandlerFunc { return h.HandleGetChangeRequest }, admin, "", map[string]string{}, 400, "Proposal ID is required"},
		{"propose from YAML needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleProposeFromYAML }, admin, `{`, nil, 400, ""},
		{"propose from YAML needs content", func(h *Handler) http.HandlerFunc { return h.HandleProposeFromYAML }, admin, `{"summary":"s"}`, nil, 400, "A document is required"},
		{"propose from YAML needs a summary", func(h *Handler) http.HandlerFunc { return h.HandleProposeFromYAML }, admin, `{"content":"name: n"}`, nil, 400, "Describe the change"},
		{"propose from YAML refuses a bad document", func(h *Handler) http.HandlerFunc { return h.HandleProposeFromYAML }, admin, `{"content":"name: n","summary":"s"}`, nil, 400, "query string is required"},
		{"draft from YAML needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleDraftFromYAML }, admin, `{`, nil, 400, ""},
		{"draft from YAML needs content", func(h *Handler) http.HandlerFunc { return h.HandleDraftFromYAML }, admin, `{}`, nil, 400, "A document is required"},
		{"draft from YAML refuses a bad document", func(h *Handler) http.HandlerFunc { return h.HandleDraftFromYAML }, admin, `{"content":"name: [x"}`, nil, 400, "failed to parse YAML"},
		{"draft needs JSON", func(h *Handler) http.HandlerFunc { return h.HandleSaveDraft }, admin, `{`, nil, 400, ""},
		{"draft needs a definition", func(h *Handler) http.HandlerFunc { return h.HandleSaveDraft }, admin, `{}`, nil, 400, "needs a definition"},

		{"bundle import needs a body", func(h *Handler) http.HandlerFunc { return h.HandleImportBundle }, admin, "", nil, 400, "The archive is empty"},
		{"bundle import needs analyst", func(h *Handler) http.HandlerFunc { return h.HandleImportBundle }, viewer, "x", nil, 403, ""},
	})
}

func TestPolicyCatalogEndpoint(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestHandler().HandlePolicyCatalog(rec, viewer.request(http.MethodGet, "", nil))
	var resp struct {
		Data PolicyCatalog `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || rec.Code != 200 {
		t.Fatalf("%d %v", rec.Code, err)
	}
	if len(resp.Data.Fields) != len(policyFields) || len(resp.Data.Operators["list"]) == 0 {
		t.Errorf("catalog = %+v", resp.Data)
	}
}

// Role checks against a specific resource's scope, which is what stops a caller from
// acting on another fractal's alert by guessing its id.
func TestRequireRoleOnScope(t *testing.T) {
	h := newTestHandler()
	check := func(c caller, fractal string) int {
		rec := httptest.NewRecorder()
		if h.requireObjectRole(rec, c.request(http.MethodGet, "", nil), fractal, "", "analyst") {
			return 200
		}
		return rec.Code
	}

	if got := check(nobody, "f1"); got != 403 {
		t.Errorf("no user: %d", got)
	}
	if got := check(admin, "anything"); got != 200 {
		t.Errorf("a tenant admin passes everywhere: %d", got)
	}
	// Without a resolver the session role applies.
	if got := check(analyst, "f1"); got != 200 {
		t.Errorf("an analyst: %d", got)
	}
	if got := check(viewer, "f1"); got != 403 {
		t.Errorf("a viewer: %d", got)
	}

	// An API key's pre-resolved role is a role on the key's own fractal only.
	key := caller{user: &storage.User{Username: "apikey_1"}, fractalRole: "admin", fractal: "f1", apiKey: true}
	if got := check(key, "f1"); got != 200 {
		t.Errorf("a key on its own fractal: %d", got)
	}
	if got := check(key, "f2"); got != 403 {
		t.Errorf("a key scoped to f1 must not act on f2: %d", got)
	}
	weakKey := key
	weakKey.fractalRole = "viewer"
	if got := check(weakKey, "f1"); got != 403 {
		t.Errorf("a viewer key cannot act as analyst: %d", got)
	}

	prismCheck := func(c caller) int {
		rec := httptest.NewRecorder()
		if h.requireObjectRole(rec, c.request(http.MethodGet, "", nil), "", "p1", "analyst") {
			return 200
		}
		return rec.Code
	}
	if got := prismCheck(caller{user: &storage.User{Username: "p"}, prismRole: "analyst", prism: "p1"}); got != 200 {
		t.Errorf("prism analyst: %d", got)
	}
	if got := prismCheck(caller{user: &storage.User{Username: "p"}, prismRole: "analyst", prism: "p2"}); got != 403 {
		t.Errorf("an analyst of another prism: %d", got)
	}
	if got := prismCheck(caller{user: &storage.User{Username: "p"}, prismRole: "viewer", prism: "p1"}); got != 403 {
		t.Errorf("prism viewer: %d", got)
	}
	if got := prismCheck(nobody); got != 403 {
		t.Errorf("no user on a prism: %d", got)
	}
	if got := prismCheck(admin); got != 200 {
		t.Errorf("admin on a prism: %d", got)
	}
}

// An action is addressed by id, so it must also sit in the caller's scope; one that
// does not is reported as missing rather than forbidden, which leaks nothing.
func TestRequireActionInScope(t *testing.T) {
	h := newTestHandler()
	cases := []struct {
		c                caller
		actionF, actionP string
		wantOK           bool
		wantStatus       int
	}{
		{analyst, "f1", "", true, 0},
		{analyst, "f2", "", false, 404},
		{analyst, "", "p1", false, 404},
		{caller{user: analyst.user, prism: "p1"}, "", "p1", true, 0},
		{caller{user: analyst.user, prism: "p1"}, "p1", "", false, 404},
		{unscoped, "f1", "", false, 400},
	}
	for i, tc := range cases {
		rec := httptest.NewRecorder()
		ok := h.requireActionInScope(rec, tc.c.request(http.MethodGet, "", nil), tc.actionF, tc.actionP, "Webhook")
		if ok != tc.wantOK || (!ok && rec.Code != tc.wantStatus) {
			t.Errorf("case %d: ok=%v status=%d", i, ok, rec.Code)
		}
	}
}

// A selected prism wins over a selected fractal: the prism is the narrower view.
func TestGetScopePrefersThePrism(t *testing.T) {
	h := newTestHandler()
	f, p, err := h.getScope(caller{fractal: "f1", prism: "p1"}.request(http.MethodGet, "", nil))
	if err != nil || f != "" || p != "p1" {
		t.Errorf("got (%q, %q, %v)", f, p, err)
	}
	f, p, err = h.getScope(caller{fractal: "f1"}.request(http.MethodGet, "", nil))
	if err != nil || f != "f1" || p != "" {
		t.Errorf("got (%q, %q, %v)", f, p, err)
	}
	// No fractal manager to supply a default: no scope at all.
	f, p, err = h.getScope(nobody.request(http.MethodGet, "", nil))
	if err != nil || f != "" || p != "" {
		t.Errorf("got (%q, %q, %v)", f, p, err)
	}
}

func TestParseIntParam(t *testing.T) {
	if n, err := parseIntParam("42"); err != nil || n != 42 {
		t.Errorf("got %d %v", n, err)
	}
	if _, err := parseIntParam("ten"); err == nil {
		t.Error("a non-number must fail")
	}
}
