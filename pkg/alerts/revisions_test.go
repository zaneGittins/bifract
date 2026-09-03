package alerts

import "testing"

func TestHashIgnoresActionIDOrder(t *testing.T) {
	a := RevisionContent{Name: "x", WebhookActionIDs: []string{"b", "a"}, FractalActionIDs: []string{"d", "c"}}
	b := RevisionContent{Name: "x", WebhookActionIDs: []string{"a", "b"}, FractalActionIDs: []string{"c", "d"}}

	ha, err := a.Hash()
	if err != nil {
		t.Fatal(err)
	}
	hb, err := b.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Errorf("action ID order changed the hash: %s vs %s", ha, hb)
	}
}

func TestHashTreatsNilAndEmptyListsAlike(t *testing.T) {
	// An alert saved with no labels arrives as nil from JSON and as an empty array
	// from Postgres. If those hashed differently every sync would write a revision.
	a := RevisionContent{Name: "x"}
	b := RevisionContent{Name: "x", Labels: []string{}, References: []string{}, WebhookActionIDs: []string{}}

	ha, _ := a.Hash()
	hb, _ := b.Hash()
	if ha != hb {
		t.Errorf("nil and empty lists hashed differently: %s vs %s", ha, hb)
	}
}

func TestHashDetectsLabelReorder(t *testing.T) {
	a := RevisionContent{Labels: []string{"attack.t1059", "high"}}
	b := RevisionContent{Labels: []string{"high", "attack.t1059"}}

	ha, _ := a.Hash()
	hb, _ := b.Hash()
	if ha == hb {
		t.Error("label order is author-visible and should count as a change")
	}
}

func TestHashChangesWithDefinition(t *testing.T) {
	base := RevisionContent{Name: "x", QueryString: "process_name=\"a\""}
	changed := base
	changed.QueryString = "process_name=\"b\""

	hb, _ := base.Hash()
	hc, _ := changed.Hash()
	if hb == hc {
		t.Error("query change did not change the hash")
	}
}

func TestSummarizeChange(t *testing.T) {
	before := RevisionContent{Name: "a", QueryString: "q1", Severity: "low", Labels: []string{"x"}}
	after := RevisionContent{Name: "a", QueryString: "q2", Severity: "high", Labels: []string{"x"}}

	got := summarizeChange(before, after)
	if got != "query, severity" {
		t.Errorf("summarizeChange = %q, want %q", got, "query, severity")
	}
}

func TestSummarizeChangeActions(t *testing.T) {
	before := RevisionContent{EmailActionIDs: []string{"a"}}
	after := RevisionContent{EmailActionIDs: []string{"a", "b"}}

	if got := summarizeChange(before, after); got != "actions" {
		t.Errorf("summarizeChange = %q, want %q", got, "actions")
	}
}

func TestSummarizeChangeIdentical(t *testing.T) {
	c := RevisionContent{Name: "a", QueryString: "q"}
	if got := summarizeChange(c, c); got != "no definition change" {
		t.Errorf("summarizeChange = %q, want %q", got, "no definition change")
	}
}

func TestToUpdateRequestRoundTrip(t *testing.T) {
	window := 300
	cronExpr := "*/5 * * * *"
	queryWindow := 600
	content := RevisionContent{
		Name:                "rule",
		Description:         "desc",
		QueryString:         "process_name=\"cmd.exe\"",
		AlertType:           "scheduled",
		Severity:            "high",
		ThrottleTimeSeconds: 60,
		ThrottleField:       "host",
		Labels:              []string{"attack.t1059"},
		References:          []string{"https://example.test"},
		WindowDuration:      &window,
		ScheduleCron:        &cronExpr,
		QueryWindowSeconds:  &queryWindow,
		WebhookActionIDs:    []string{"w1"},
		FractalActionIDs:    []string{"f1"},
		DictionaryActionIDs: []string{"d1"},
		EmailActionIDs:      []string{"e1"},
	}

	req := content.ToUpdateRequest(true)
	back := revisionContentFromRequest(req, string(req.AlertType), string(req.Severity))

	want, _ := content.Hash()
	got, _ := back.Hash()
	if want != got {
		t.Errorf("round trip through an update request changed the definition")
	}
	if !req.Enabled {
		t.Error("enabled must come from the live alert, not the revision")
	}
}

func TestFeedFieldsPreserveLocalConfiguration(t *testing.T) {
	// A feed owns the rule text and metadata. Throttle, window and action wiring are
	// set in Bifract and must survive a sync.
	current := RevisionContent{
		ThrottleTimeSeconds: 300,
		ThrottleField:       "host",
		EmailActionIDs:      []string{"e1"},
	}

	got := feedRevisionFields{
		name: "n", description: "d", queryString: "q", alertType: "event", severity: "high",
	}.applyTo(current)

	if got.ThrottleTimeSeconds != 300 || got.ThrottleField != "host" {
		t.Error("feed sync overwrote local throttle configuration")
	}
	if len(got.EmailActionIDs) != 1 || got.EmailActionIDs[0] != "e1" {
		t.Error("feed sync overwrote local action wiring")
	}
	if got.QueryString != "q" || got.Severity != "high" {
		t.Error("feed sync did not apply the upstream definition")
	}
}

// An event alert seeded from its row carries window 0; the same alert saved from the
// editor carries null. They are one definition, so they hash alike.
func TestHashTreatsZeroAndUnsetWindowsAlike(t *testing.T) {
	zero, empty := 0, ""
	seeded := RevisionContent{Name: "a", QueryString: "x", AlertType: "event", WindowDuration: &zero, QueryWindowSeconds: &zero, ScheduleCron: &empty}
	saved := RevisionContent{Name: "a", QueryString: "x", AlertType: "event"}
	h1, err := seeded.Hash()
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := saved.Hash()
	if h1 != h2 {
		t.Fatalf("zero and unset windows hashed differently")
	}
}
