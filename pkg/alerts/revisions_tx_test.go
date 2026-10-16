package alerts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// newFakeTx opens a transaction on a scripted Postgres. respond, when set, answers
// every statement by its text; with none, queries return no rows and execs affect
// none. Statements are recorded on the returned fake.
func newFakeTx(t *testing.T, respond func(query string, args []driver.Value) fakedb.PGResult) (storage.Tx, *fakedb.PG) {
	t.Helper()
	pg, fake := storagetest.Postgres()
	if respond != nil {
		fake.Respond(func(query string, args []driver.Value) (fakedb.PGResult, bool) {
			return respond(query, args), true
		})
	}
	tx, err := pg.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		tx.Rollback(context.Background())
		pg.Close()
	})
	return tx, fake
}

// rowsOf chunks values into rows of cols columns.
func rowsOf(cols int, values ...driver.Value) fakedb.PGResult {
	var rows [][]driver.Value
	for i := 0; i+cols <= len(values); i += cols {
		rows = append(rows, values[i:i+cols])
	}
	return fakedb.RowsOf(rows...)
}

// alertRow answers the definition read of loadRevisionContentTx.
func alertRow(name, query string) fakedb.PGResult {
	return rowsOf(13,
		name, "desc", query, "event", "medium", int64(0), int64(0), "", []byte("{b,a}"), []byte("{}"),
		nil, nil, nil)
}

func mustHash(t *testing.T, c RevisionContent) string {
	t.Helper()
	h, err := c.Hash()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func headAnswer(t *testing.T, num int64, c RevisionContent) fakedb.PGResult {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return rowsOf(3, num, mustHash(t, c), raw)
}

// An alert from before revision history has no rows: its first edit must record the
// pre-edit definition as revision 1, then the edit as revision 2, so the head always
// equals the live definition and the edit is diffable.
func TestWriteRevisionSeedsHistoryOnFirstEdit(t *testing.T) {
	tx, script := newFakeTx(t, func(q string, args []driver.Value) fakedb.PGResult {
		switch {
		case strings.Contains(q, "FROM alert_revisions"):
			return fakedb.PGResult{} // no history yet
		case strings.Contains(q, "FROM alerts WHERE id"):
			return alertRow("old name", `a="1"`)
		case strings.Contains(q, "FROM alert_webhook_actions"):
			return rowsOf(1, "w2", "w1")
		}
		return fakedb.Affected(1)
	})

	edit := RevisionContent{Name: "new name", Description: "desc", QueryString: `a="1"`, AlertType: "event", Severity: "medium",
		Labels: []string{"b", "a"}, WebhookActionIDs: []string{"w1", "w2"}}
	if err := writeRevision(context.Background(), tx, "alert-1", edit, "alice", "Alice", 10); err != nil {
		t.Fatal(err)
	}

	inserts := script.Matching("INSERT INTO alert_revisions")
	if len(inserts) != 2 {
		t.Fatalf("want the seed and the edit, got %d inserts", len(inserts))
	}
	seed, change := inserts[0].Args, inserts[1].Args
	if seed[1] != int64(1) || seed[4] != "initial" {
		t.Errorf("seed = revision %v summary %v", seed[1], seed[4])
	}
	var seeded RevisionContent
	if err := json.Unmarshal(seed[2].([]byte), &seeded); err != nil {
		t.Fatal(err)
	}
	if seeded.Name != "old name" || strings.Join(seeded.WebhookActionIDs, ",") != "w1,w2" || strings.Join(seeded.Labels, ",") != "b,a" {
		t.Errorf("the seed must be the canonical pre-edit definition: %+v", seeded)
	}
	if seeded.References == nil || seeded.EmailActionIDs == nil {
		t.Error("canonical content has no nil lists")
	}
	if change[1] != int64(2) || change[4] != "name" || change[5] != "alice" || change[6] != "Alice" {
		t.Errorf("edit = revision %v summary %v author %v label %v", change[1], change[4], change[5], change[6])
	}
	if change[3] != mustHash(t, edit) {
		t.Error("the stored hash must be the edit's own hash")
	}

	// Each insert prunes to the retention limit.
	prunes := script.Matching("DELETE FROM alert_revisions")
	if len(prunes) != 2 || prunes[1].Args[1] != int64(2-10) {
		t.Errorf("prunes = %+v", prunes)
	}
}

func TestWriteRevisionSkipsAnUnchangedDefinition(t *testing.T) {
	head := RevisionContent{Name: "n", QueryString: `a="1"`, WebhookActionIDs: []string{"b", "a"}}
	tx, script := newFakeTx(t, func(q string, args []driver.Value) fakedb.PGResult {
		if strings.Contains(q, "FROM alert_revisions") {
			return headAnswer(t, 4, head)
		}
		return fakedb.Affected(1)
	})

	// Same definition, actions in a different order: not a change.
	same := RevisionContent{Name: "n", QueryString: `a="1"`, WebhookActionIDs: []string{"a", "b"}}
	if err := writeRevision(context.Background(), tx, "alert-1", same, "alice", "Alice", 10); err != nil {
		t.Fatal(err)
	}
	if n := len(script.Matching("INSERT")); n != 0 {
		t.Errorf("an unchanged definition wrote %d revisions", n)
	}
}

func TestWriteRevisionAppendsAfterTheHead(t *testing.T) {
	head := RevisionContent{Name: "n", QueryString: `a="1"`, Severity: "low"}
	tx, script := newFakeTx(t, func(q string, args []driver.Value) fakedb.PGResult {
		if strings.Contains(q, "FROM alert_revisions") {
			return headAnswer(t, 7, head)
		}
		return fakedb.Affected(1)
	})

	edit := head
	edit.Severity = "high"
	edit.QueryString = `a="2"`
	if err := writeRevision(context.Background(), tx, "alert-1", edit, "", "", 0); err != nil {
		t.Fatal(err)
	}
	inserts := script.Matching("INSERT INTO alert_revisions")
	if len(inserts) != 1 || inserts[0].Args[1] != int64(8) || inserts[0].Args[4] != "query, severity" {
		t.Fatalf("inserts = %+v", inserts)
	}
	// No author is NULL, not '': the column is a foreign key to users.
	if inserts[0].Args[5] != nil {
		t.Errorf("an unattributed revision must store NULL, got %#v", inserts[0].Args[5])
	}
	if n := len(script.Matching("DELETE")); n != 0 {
		t.Error("a retention of zero keeps everything")
	}
}

func TestWriteRevisionSurfacesStoreErrors(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]string{
		"head lookup": "FROM alert_revisions",
		"seed read":   "FROM alerts WHERE id",
		"action read": "FROM alert_email_actions",
		"insert":      "INSERT INTO alert_revisions",
		"prune":       "DELETE FROM alert_revisions",
	}
	for name, failOn := range cases {
		t.Run(name, func(t *testing.T) {
			tx, _ := newFakeTx(t, func(q string, args []driver.Value) fakedb.PGResult {
				if strings.Contains(q, failOn) {
					return fakedb.PGResult{Err: boom}
				}
				switch {
				case strings.Contains(q, "FROM alert_revisions"):
					return fakedb.PGResult{}
				case strings.Contains(q, "FROM alerts WHERE id"):
					return alertRow("n", `a="1"`)
				}
				return fakedb.Affected(1)
			})
			err := writeRevision(context.Background(), tx, "alert-1", RevisionContent{Name: "x"}, "u", "u", 5)
			if !errors.Is(err, boom) {
				t.Errorf("got %v, want the store error", err)
			}
		})
	}
}

func TestHeadRevisionRejectsACorruptRow(t *testing.T) {
	tx, _ := newFakeTx(t, func(q string, args []driver.Value) fakedb.PGResult {
		return rowsOf(3, int64(3), "hash", []byte("{not json"))
	})
	if _, _, _, _, err := headRevision(context.Background(), tx, "a"); err == nil || !strings.Contains(err.Error(), "decode revision 3") {
		t.Errorf("got %v", err)
	}
}

// Tests are validated before the old corpus is deleted, so a bad save never leaves an
// alert with no tests at all.
func TestReplaceTests(t *testing.T) {
	m := &Manager{}
	tx, script := newFakeTx(t, nil)

	err := m.replaceTests(context.Background(), tx, "alert-1", []AlertTest{{Name: "", Expectation: "match"}}, "u")
	if err == nil || len(script.Matching("")) != 0 {
		t.Fatalf("an invalid corpus must be refused before any statement: %v, sent %d", err, len(script.Matching("")))
	}

	tests := []AlertTest{
		{Name: "fires", Expectation: "match", Events: []map[string]interface{}{{"a": "b"}}, Position: 9},
		{Name: "quiet", Expectation: "no_match", Events: []map[string]interface{}{{"a": "c"}}, Position: 9},
	}
	if err := m.replaceTests(context.Background(), tx, "alert-1", tests, ""); err != nil {
		t.Fatal(err)
	}
	if len(script.Matching("DELETE FROM alert_tests")) != 1 {
		t.Error("the old corpus must be cleared")
	}
	inserts := script.Matching("INSERT INTO alert_tests")
	if len(inserts) != 2 {
		t.Fatalf("inserts = %d", len(inserts))
	}
	for i, ins := range inserts {
		if ins.Args[4] != int64(i) {
			t.Errorf("test %d stored at position %v: order in the request is the position", i, ins.Args[4])
		}
		if ins.Args[5] != nil {
			t.Errorf("an unattributed test must store NULL created_by, got %#v", ins.Args[5])
		}
	}
	if string(inserts[0].Args[3].([]byte)) != `[{"a":"b"}]` {
		t.Errorf("events = %s", inserts[0].Args[3])
	}
}

func TestReplaceTestsReportsWhichTestFailed(t *testing.T) {
	tx, _ := newFakeTx(t, func(q string, args []driver.Value) fakedb.PGResult {
		if strings.Contains(q, "INSERT") && args[1] == "second" {
			return fakedb.PGResult{Err: errors.New("constraint")}
		}
		return fakedb.Affected(1)
	})
	tests := []AlertTest{
		{Name: "first", Expectation: "match", Events: []map[string]interface{}{{"a": 1}}},
		{Name: "second", Expectation: "match", Events: []map[string]interface{}{{"a": 2}}},
	}
	err := (&Manager{}).replaceTests(context.Background(), tx, "a", tests, "u")
	if err == nil || !strings.Contains(err.Error(), `saving test "second"`) {
		t.Errorf("got %v", err)
	}
}

func TestAssociateActions(t *testing.T) {
	m := &Manager{}
	cases := []struct {
		table string
		fn    func(tx storage.Tx, ids []string) error
	}{
		{"alert_webhook_actions", func(tx storage.Tx, ids []string) error {
			return m.associateWebhookActions(context.Background(), tx, "a1", ids)
		}},
		{"alert_fractal_actions", func(tx storage.Tx, ids []string) error {
			return m.associateFractalActions(context.Background(), tx, "a1", ids)
		}},
		{"alert_dictionary_actions", func(tx storage.Tx, ids []string) error {
			return m.associateDictionaryActions(context.Background(), tx, "a1", ids)
		}},
		{"alert_email_actions", func(tx storage.Tx, ids []string) error {
			return m.associateEmailActions(context.Background(), tx, "a1", ids)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.table, func(t *testing.T) {
			tx, script := newFakeTx(t, nil)
			if err := tc.fn(tx, []string{"x1", "x2"}); err != nil {
				t.Fatal(err)
			}
			inserts := script.Matching("INSERT INTO " + tc.table)
			if len(inserts) != 2 || inserts[0].Args[0] != "a1" || inserts[1].Args[1] != "x2" {
				t.Errorf("inserts = %+v", inserts)
			}

			failing, _ := newFakeTx(t, func(string, []driver.Value) fakedb.PGResult { return fakedb.PGResult{Err: errors.New("fk")} })
			if err := tc.fn(failing, []string{"x1"}); err == nil {
				t.Error("an association failure must abort the write")
			}
		})
	}
}

func TestRestoreBlockedErrorNamesWhatIsMissing(t *testing.T) {
	err := &RestoreBlockedError{Missing: []MissingActionRef{
		{Kind: "webhook", ID: "w1", Name: "notify-soc"},
		{Kind: "email", ID: "e9"},
	}}
	want := "revision refers to actions that are no longer available: webhook action notify-soc, email action e9"
	if err.Error() != want {
		t.Errorf("got %q", err.Error())
	}
}

// Restore runs a revision back through UpdateAlert, so it must carry every field, and
// take enabled from the live alert rather than from history.
func TestRevisionRoundTripsThroughAnUpdateRequest(t *testing.T) {
	c := RevisionContent{
		Name: "n", Description: "d", QueryString: `a="b"`, AlertType: "scheduled", Severity: "high",
		ThrottleTimeSeconds: 60, ThrottleField: "host", MaxEventLagSeconds: 5,
		Labels: []string{"l"}, References: []string{"r"},
		WindowDuration: intPtr(30), ScheduleCron: strPtr("@hourly"), QueryWindowSeconds: intPtr(600),
		WebhookActionIDs: []string{"w"}, FractalActionIDs: []string{"f"},
		DictionaryActionIDs: []string{"d"}, EmailActionIDs: []string{"e"},
	}
	req := c.ToUpdateRequest(false)
	if req.Enabled {
		t.Error("enabled comes from the caller")
	}
	back := revisionContentFromRequest(req, string(req.AlertType), string(req.Severity))
	if mustHash(t, back) != mustHash(t, c) {
		t.Errorf("round trip changed the definition:\n%+v\n%+v", c, back)
	}
}

// A zero window and no window are one definition; a diff must not report a change
// between how the row stores it and how a request carries it.
func TestCanonicalizeTreatsZeroWindowsAsUnset(t *testing.T) {
	stored := RevisionContent{Name: "n", WindowDuration: intPtr(0), QueryWindowSeconds: intPtr(0), ScheduleCron: strPtr("")}
	requested := RevisionContent{Name: "n"}
	if mustHash(t, stored) != mustHash(t, requested) {
		t.Error("zero windows must hash like unset ones")
	}
	if got := summarizeChange(RevisionContent{WindowDuration: intPtr(5)}, RevisionContent{}); got != "window" {
		t.Errorf("got %q", got)
	}
	if got := summarizeChange(RevisionContent{ScheduleCron: strPtr("@daily")}, RevisionContent{ScheduleCron: strPtr("@hourly")}); got != "schedule" {
		t.Errorf("got %q", got)
	}
	if got := summarizeChange(RevisionContent{ThrottleField: "a"}, RevisionContent{ThrottleField: "b", MaxEventLagSeconds: 1}); got != "throttle, max event lag" {
		t.Errorf("got %q", got)
	}
}

// The policy view of an existing alert reads the actions that would actually run.
func TestAlertRevisionContent(t *testing.T) {
	a := &Alert{
		Name: "n", QueryString: "q", AlertType: "event", Severity: "low",
		WebhookActions:      []WebhookAction{{ID: "w2"}, {ID: "w1"}},
		FractalActions:      []FractalAction{{ID: "f1"}},
		DictionaryActionIDs: []string{"d1"},
	}
	c := alertRevisionContent(a)
	if strings.Join(c.WebhookActionIDs, ",") != "w1,w2" || len(c.FractalActionIDs) != 1 || len(c.DictionaryActionIDs) != 1 {
		t.Errorf("actions: %+v", c)
	}
	if c.EmailActionIDs == nil || c.Labels == nil {
		t.Error("content must be canonical")
	}
}

// Feed sync owns only the upstream fields; everything set in Bifract carries over.
func TestFeedRevisionFieldsKeepLocalSettings(t *testing.T) {
	local := RevisionContent{
		Name: "old", QueryString: "old", ThrottleTimeSeconds: 300, ThrottleField: "host",
		WebhookActionIDs: []string{"w1"}, WindowDuration: intPtr(60),
	}
	got := feedRevisionFields{
		name: "new", description: "d", queryString: `a="b"`, alertType: "event", severity: "high",
		labels: []string{"sigma:high"}, references: []string{"r"},
	}.applyTo(local)
	if got.Name != "new" || got.QueryString != `a="b"` || got.Severity != "high" || got.Labels[0] != "sigma:high" {
		t.Errorf("upstream fields not applied: %+v", got)
	}
	if got.ThrottleTimeSeconds != 300 || got.ThrottleField != "host" || got.WebhookActionIDs[0] != "w1" || *got.WindowDuration != 60 {
		t.Errorf("local settings lost: %+v", got)
	}
}
