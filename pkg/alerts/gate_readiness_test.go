package alerts

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGateConfigBounds(t *testing.T) {
	def := DefaultGateConfig()
	if def.Enabled || def.MinApprovals != 1 || !def.AllowSelfApproval {
		t.Errorf("a scope that never configured the gate is open: %+v", def)
	}
	for n, ok := range map[int]bool{0: false, 1: true, 10: true, 11: false, -3: false} {
		cfg := GateConfig{MinApprovals: n}
		if err := cfg.Validate(); (err == nil) != ok {
			t.Errorf("min approvals %d: err = %v", n, err)
		}
	}
}

// A stored test run is evidence only for exactly what it ran: an edit to the
// definition or to the tests makes it stale.
func TestStoredTestResultIsBoundToContentAndTests(t *testing.T) {
	cr := &ChangeRequest{ContentHash: "c1", Tests: []AlertTest{{Name: "t", Expectation: "match"}}}
	run := &TestRunResult{Passed: 1}
	cr.testResult, cr.testResultHash = run, cr.testsHash()

	if cr.storedTestResult() != run {
		t.Fatal("a run for the current content and tests must be reused")
	}

	cr.ContentHash = "c2"
	if cr.storedTestResult() != nil {
		t.Error("a definition edit invalidates the run")
	}

	cr.ContentHash = "c1"
	cr.Tests[0].Expectation = "no_match"
	if cr.storedTestResult() != nil {
		t.Error("a test edit invalidates the run")
	}

	if (&ChangeRequest{ContentHash: "c1"}).storedTestResult() != nil {
		t.Error("no run stored, nothing to reuse")
	}
}

func TestTestRunResultOK(t *testing.T) {
	if !(&TestRunResult{Passed: 2}).OK() {
		t.Error("all passing is OK")
	}
	if (&TestRunResult{Passed: 2, Failed: 1}).OK() {
		t.Error("a failure is not OK")
	}
	if (&TestRunResult{Error: "no ClickHouse"}).OK() {
		t.Error("a run that could not execute is not a pass")
	}
}

func TestReadinessAndGateErrors(t *testing.T) {
	if !(&MergeReadiness{}).OK() || (&MergeReadiness{Blocker: "x"}).OK() {
		t.Error("readiness is OK exactly when nothing blocks")
	}
	if got := (&MergeBlockedError{Reason: "1 of 2 approvals"}).Error(); got != "cannot merge: 1 of 2 approvals" {
		t.Errorf("got %q", got)
	}
	if got := (&GateRequiredError{Kind: ChangeCreate}).Error(); !strings.Contains(got, "open a proposal") {
		t.Errorf("got %q", got)
	}
}

func TestDraftIsPrivateUntilSubmitted(t *testing.T) {
	cr := &ChangeRequest{Status: ChangeDraft}
	if !cr.Draft() || cr.Open() {
		t.Error("a draft is not open for review")
	}
	if err := cr.CanApprove("reviewer", false, gateCfg()); err == nil {
		t.Error("a draft cannot be approved")
	}
	if blocker := cr.MergeBlocker(gateCfg(), ""); blocker != "this proposal is draft" {
		t.Errorf("got %q", blocker)
	}
}

// Every proposal failure answers with a status the client can act on.
func TestChangeRequestErrorStatuses(t *testing.T) {
	cases := map[error]int{
		ErrChangeRequestNotFound: 404,
		ErrAlertNotFound:         404,
		errors.New("only the author or an admin can withdraw a proposal"): 403,
		errors.New("only the author can revise a proposal"):               403,
		errors.New("a proposal cannot be approved by its author"):         403,
		errors.New("self approval is turned off for this scope"):          403,
		ErrDraftExists:                                        409,
		errors.New("this proposal is merged"):                 409,
		errors.New("this proposal is discarded"):              409,
		errors.New("kind must be create, update or delete"):   400,
		errors.New("a delete proposal needs an alert"):        400,
		errors.New("a create proposal has no alert yet"):      400,
		errors.New("say why the alert should be deleted"):     400,
		errors.New(`duplicate test name "x"`):                 400,
		errors.New("at most 25 tests per alert"):              400,
		errors.New("load proposal scope: connection refused"): 500,
	}
	for err, want := range cases {
		rec := httptest.NewRecorder()
		(&Handler{}).changeRequestError(rec, err)
		if rec.Code != want {
			t.Errorf("%v: status %d, want %d", err, rec.Code, want)
		}
	}
}

// A refused direct write tells the client to open a proposal instead, in a shape it
// can branch on without reading the message.
func TestGateRequiredResponse(t *testing.T) {
	rec := httptest.NewRecorder()
	if (&Handler{}).gateRequiredResponse(rec, errors.New("other")) {
		t.Fatal("only a gate refusal is rendered")
	}
	rec = httptest.NewRecorder()
	if !(&Handler{}).gateRequiredResponse(rec, &GateRequiredError{Kind: ChangeUpdate}) {
		t.Fatal("a gate refusal must be rendered")
	}
	var resp struct {
		Success bool              `json:"success"`
		Data    map[string]string `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusConflict || resp.Success || resp.Data["gate"] != "required" || resp.Data["kind"] != ChangeUpdate {
		t.Errorf("%d %+v", rec.Code, resp)
	}
}

// A policy refusal carries only the blocking violations, so the editor can put each
// against its field without warnings getting in the way.
func TestPolicyBlockedResponse(t *testing.T) {
	rec := httptest.NewRecorder()
	if (&Handler{}).policyBlockedResponse(rec, errors.New("other")) {
		t.Fatal("only a policy refusal is rendered")
	}
	blocked := &PolicyBlockedError{Result: PolicyResult{Blocking: 1, Violations: []Violation{
		{Field: "labels", Severity: PolicyBlock, Message: "tag it"},
		{Field: "description", Severity: PolicyWarn, Message: "describe it"},
	}}}
	rec = httptest.NewRecorder()
	if !(&Handler{}).policyBlockedResponse(rec, blocked) {
		t.Fatal("a policy refusal must be rendered")
	}
	var resp struct {
		Data []Violation `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusUnprocessableEntity || len(resp.Data) != 1 || resp.Data[0].Field != "labels" {
		t.Errorf("%d %+v", rec.Code, resp.Data)
	}
}

// Every endpoint addressed by scope refuses a caller that declared none: an empty
// scope reaching the manager would read or write across every fractal.
func TestScopedEndpointsRefuseAnUnscopedCaller(t *testing.T) {
	endpoints := map[string]func(h *Handler) http.HandlerFunc{
		"list drafts":       func(h *Handler) http.HandlerFunc { return h.HandleListDrafts },
		"submit draft":      func(h *Handler) http.HandlerFunc { return h.HandleSubmitDraft },
		"delete draft":      func(h *Handler) http.HandlerFunc { return h.HandleDeleteDraft },
		"gate config":       func(h *Handler) http.HandlerFunc { return h.HandleGetGateConfig },
		"list proposals":    func(h *Handler) http.HandlerFunc { return h.HandleListChangeRequests },
		"policy compliance": func(h *Handler) http.HandlerFunc { return h.HandlePolicyCompliance },
		"export policies":   func(h *Handler) http.HandlerFunc { return h.HandleExportPolicies },
		"export bundle":     func(h *Handler) http.HandlerFunc { return h.HandleExportBundle },
		"evaluate policies": func(h *Handler) http.HandlerFunc { return h.HandleEvaluatePolicies },
		"import policies":   func(h *Handler) http.HandlerFunc { return h.HandleImportPolicies },
		"submit proposal":   func(h *Handler) http.HandlerFunc { return h.HandleSubmitChangeRequest },
		"propose from YAML": func(h *Handler) http.HandlerFunc { return h.HandleProposeFromYAML },
		"draft from YAML":   func(h *Handler) http.HandlerFunc { return h.HandleDraftFromYAML },
		"save draft":        func(h *Handler) http.HandlerFunc { return h.HandleSaveDraft },
		"import bundle":     func(h *Handler) http.HandlerFunc { return h.HandleImportBundle },
		"save gate config":  func(h *Handler) http.HandlerFunc { return h.HandleSaveGateConfig },
		"save policies":     func(h *Handler) http.HandlerFunc { return h.HandleSavePolicies },
	}
	for name, endpoint := range endpoints {
		rec := httptest.NewRecorder()
		endpoint(newTestHandler())(rec, unscoped.request(http.MethodPost, `{}`, nil))
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "No fractal or prism selected") {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
}

// Proposals are addressed by id, and every action on one starts by loading it.
func TestProposalEndpointsNeedAnID(t *testing.T) {
	for name, endpoint := range map[string]func(h *Handler) http.HandlerFunc{
		"review":   func(h *Handler) http.HandlerFunc { return h.HandleReviewChangeRequest },
		"merge":    func(h *Handler) http.HandlerFunc { return h.HandleMergeChangeRequest },
		"withdraw": func(h *Handler) http.HandlerFunc { return h.HandleWithdrawChangeRequest },
		"delete":   func(h *Handler) http.HandlerFunc { return h.HandleDeleteChangeRequest },
	} {
		rec := httptest.NewRecorder()
		endpoint(newTestHandler())(rec, admin.request(http.MethodPost, `{}`, map[string]string{}))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
}
