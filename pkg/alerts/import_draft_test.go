package alerts

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

const sigmaFixture = `
title: Suspicious PowerShell Download
id: 3b6ab547-8ec2-4991-b9d2-2b06702a48d7
status: test
level: high
description: Detects a download cradle
author: Test Author
tags:
  - attack.execution
  - attack.t1059.001
references:
  - https://example.com/ref
logsource:
  category: process_creation
  product: windows
detection:
  selection:
    CommandLine|contains: DownloadString
  condition: selection
`

func proposalWith(kind, author string) *ChangeRequest {
	cr := &ChangeRequest{Kind: kind, Status: ChangeOpen, Author: author, Title: "t", Summary: "why"}
	if kind != ChangeDelete {
		cr.Content = &RevisionContent{Name: "n", QueryString: `a="b"`, AlertType: "event", Severity: "low"}
	}
	if kind != ChangeCreate {
		cr.AlertID = "11111111-1111-1111-1111-111111111111"
	}
	return cr
}

// Withdrawing used to close a proposal for good, so the work vanished from the queue
// with no way back. It must return to the author's drafts, for creates and edits alike.
func TestWithdrawReturnsProposalToDrafts(t *testing.T) {
	for _, kind := range []string{ChangeCreate, ChangeUpdate} {
		cr := proposalWith(kind, "alice")
		if got := cr.WithdrawStatus(); got != ChangeDraft {
			t.Errorf("%s: withdrawn to %q, want %q", kind, got, ChangeDraft)
		}

		// What SubmitDraft rebuilds from the row must still be a valid proposal, so the
		// draft can be resubmitted without retyping anything.
		in := ChangeRequestInput{Kind: cr.Kind, AlertID: cr.AlertID, Title: cr.Title,
			Summary: cr.Summary, Content: cr.Content, Tests: cr.Tests}
		if err := in.Validate(); err != nil {
			t.Errorf("%s: a withdrawn draft no longer validates: %v", kind, err)
		}
	}
}

func TestWithdrawClosesWhatCannotBeADraft(t *testing.T) {
	cases := map[string]*ChangeRequest{
		"a delete has no definition to edit": proposalWith(ChangeDelete, "alice"),
		"a proposal whose author is gone":    proposalWith(ChangeCreate, ""),
	}
	noContent := proposalWith(ChangeUpdate, "alice")
	noContent.Content = nil
	cases["an update with no definition"] = noContent

	for name, cr := range cases {
		if got := cr.WithdrawStatus(); got != ChangeDiscarded {
			t.Errorf("%s: withdrawn to %q, want %q", name, got, ChangeDiscarded)
		}
	}
}

func TestWithdrawPermissions(t *testing.T) {
	cr := proposalWith(ChangeCreate, "alice")
	if err := cr.CanWithdraw("alice", false); err != nil {
		t.Errorf("the author must be able to withdraw: %v", err)
	}
	if err := cr.CanWithdraw("mallory", false); err == nil {
		t.Error("another analyst must not withdraw someone else's proposal")
	}
	if err := cr.CanWithdraw("admin", true); err != nil {
		t.Errorf("an admin may withdraw any proposal: %v", err)
	}

	orphan := proposalWith(ChangeCreate, "")
	if err := orphan.CanWithdraw("", false); err == nil {
		t.Error("an unattributed caller must not match an authorless proposal")
	}

	for _, status := range []string{ChangeDraft, ChangeMerged, ChangeDiscarded} {
		closed := proposalWith(ChangeCreate, "alice")
		closed.Status = status
		if err := closed.CanWithdraw("alice", false); err == nil {
			t.Errorf("a %s proposal must not be withdrawable", status)
		}
	}
	rejected := proposalWith(ChangeUpdate, "alice")
	rejected.Status = ChangeRejected
	if err := rejected.CanWithdraw("alice", false); err != nil {
		t.Errorf("a proposal sent back for changes is still withdrawable: %v", err)
	}
}

// The import used to look the name up across every fractal and every feed, so a Sigma
// rule sharing a title with a feed alert, or with an alert elsewhere, was written over
// that alert instead of landing here.
func TestImportReplacesOnlyAnAlertInScope(t *testing.T) {
	const fractal, other, prism = "f-1", "f-2", "p-1"

	if id, err := importReplaces(nil, "n", fractal, ""); err != nil || id != "" {
		t.Errorf("no holder: got (%q, %v), want a create", id, err)
	}

	here := &namedAlert{ID: "a-1", FractalID: fractal}
	if id, err := importReplaces(here, "n", fractal, ""); err != nil || id != "a-1" {
		t.Errorf("holder in scope: got (%q, %v), want it replaced", id, err)
	}

	elsewhere := &namedAlert{ID: "a-2", FractalID: other}
	if _, err := importReplaces(elsewhere, "n", fractal, ""); !errors.Is(err, ErrAlertNameTaken) {
		t.Errorf("holder in another fractal: got %v, want ErrAlertNameTaken", err)
	}
	if _, err := importReplaces(here, "n", "", prism); !errors.Is(err, ErrAlertNameTaken) {
		t.Errorf("fractal holder seen from a prism: got %v, want ErrAlertNameTaken", err)
	}
}

func TestSigmaImportBecomesADraftOfANewAlert(t *testing.T) {
	req, err := sigmaImportRequest(sigmaFixture, nil)
	if err != nil {
		t.Fatalf("translating the fixture: %v", err)
	}
	if req.Enabled {
		t.Error("an imported Sigma rule must arrive disabled")
	}

	in := yamlImport{Request: req}.changeInput()
	if in.Kind != ChangeCreate || in.AlertID != "" {
		t.Errorf("got kind %q alert %q, want a create", in.Kind, in.AlertID)
	}
	if in.Title != "Suspicious PowerShell Download" || in.Content.Name != in.Title {
		t.Errorf("title %q, content name %q: the rule's title must name the draft", in.Title, in.Content.Name)
	}
	if in.Content.QueryString == "" {
		t.Error("the translated query is missing")
	}
	if in.Content.AlertType != "event" || in.Content.Severity != string(SeverityHigh) {
		t.Errorf("type %q severity %q, want event/high", in.Content.AlertType, in.Content.Severity)
	}
	// A draft skips validation; submitting it does not. The import must survive both.
	if err := in.Validate(); err != nil {
		t.Errorf("the imported draft cannot be submitted: %v", err)
	}
}

func TestImportOfAnExistingNameIsAnEdit(t *testing.T) {
	req, err := sigmaImportRequest(sigmaFixture, nil)
	if err != nil {
		t.Fatal(err)
	}
	in := yamlImport{Request: req, ExistingID: "a-1"}.changeInput()
	if in.Kind != ChangeUpdate || in.AlertID != "a-1" {
		t.Errorf("got kind %q alert %q, want an update of a-1", in.Kind, in.AlertID)
	}
	if err := in.Validate(); err != nil {
		t.Errorf("the imported edit cannot be submitted: %v", err)
	}
}

func TestImportDefaultsTypeAndSeverity(t *testing.T) {
	in := yamlImport{Request: AlertUpdateRequest{Name: "n", QueryString: `a="b"`}}.changeInput()
	if in.Content.AlertType != "event" || in.Content.Severity != "medium" {
		t.Errorf("type %q severity %q, want event/medium", in.Content.AlertType, in.Content.Severity)
	}
}

// A document the caller got wrong is a 400 they can act on, not a 500.
func TestImportRefusalsAreTheCallersToFix(t *testing.T) {
	_, sigmaErr := sigmaImportRequest("title: x\ndetection:\n  sel:\n    a: b\n  condition: nope\n", nil)
	for _, err := range []error{
		sigmaErr,
		errors.New("failed to parse YAML: line 1"),
		errors.New("actions not found: Pager"),
		errors.New(`action name "x" is ambiguous: it matches webhook and email actions`),
	} {
		if err == nil || !importRefusal(err) {
			t.Errorf("%v: want an import refusal", err)
		}
	}
	if importRefusal(errors.New("look up alert by name: connection refused")) {
		t.Error("a server failure must not be blamed on the document")
	}
}

func TestDraftConflictsAnswer409(t *testing.T) {
	h := &Handler{}
	_, taken := importReplaces(&namedAlert{ID: "x", FractalID: "elsewhere"}, "n", "here", "")
	for _, err := range []error{
		ErrDraftExists,
		taken,
		errors.New("this proposal is draft"),
		errors.New("this proposal is no longer open"),
	} {
		rec := httptest.NewRecorder()
		h.importChangeError(rec, err)
		if rec.Code != http.StatusConflict {
			t.Errorf("%v: status %d, want 409", err, rec.Code)
		}
	}
}
