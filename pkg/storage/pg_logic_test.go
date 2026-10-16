package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

var pgTS = time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)

// pgCommits, pgRollbacks and pgCloses read one of the fake's counters.
func pgCommits(p *fakedb.PG) int {
	_, _, commits, _ := p.Stats()
	return commits
}

func pgRollbacks(p *fakedb.PG) int {
	_, _, _, rollbacks := p.Stats()
	return rollbacks
}

func pgCloses(p *fakedb.PG) int {
	_, closes, _, _ := p.Stats()
	return closes
}

const (
	commentSelect = "FROM comments c\n\t\tLEFT JOIN users u ON c.author = u.username\n\t\tWHERE c.id = $1"
	sectionSelect = "WHERE s.id = $1"
)

// commentRow scripts GetComment's 14 columns.
func commentRow(id, logID string) fakedb.PGResult {
	return fakedb.Row(id, logID, pgTS, "text", "alice", "{a,b}", "q", pgTS, pgTS, "F1", "", "Alice", "#fff", "A")
}

// sectionRow scripts GetNotebookSection's 16 columns.
func sectionRow(id string, order int64) fakedb.PGResult {
	return fakedb.Row(id, "N1", "comment_context", nil, "{}", nil, order, nil, []byte("null"), nil, []byte("null"), "{}", "C1", pgTS, pgTS, pgTS)
}

// A repeated star must be a no-op, decided under an advisory lock on (notebook,
// log) so two racing clicks cannot both insert.
func TestInsertCommentWithEvidenceDedupes(t *testing.T) {
	c, p := newFakePostgres()
	p.On("pg_advisory_xact_lock", fakedb.Affected(0))
	p.On("JOIN comments c ON c.id = s.comment_id", fakedb.Row("C-existing"))
	p.On(commentSelect, commentRow("C-existing", "L1"))

	w, err := c.InsertCommentWithEvidence(context.Background(), Comment{LogID: "L1", Author: "alice"}, "N1", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if !w.Existing || w.Comment.ID != "C-existing" || !reflect.DeepEqual(w.Comment.Tags, []string{"a", "b"}) {
		t.Errorf("write = %+v", w)
	}
	if len(p.Matching("INSERT INTO comments")) != 0 {
		t.Error("a duplicate star must not insert")
	}
	lock := p.Matching("pg_advisory_xact_lock")
	if len(lock) != 1 || lock[0].Args[0] != "N1:L1" || !lock[0].InTx {
		t.Errorf("lock = %+v, want the (notebook, log) key inside the transaction", lock)
	}
}

// Filing a comment writes the comment and its evidence section in one transaction,
// appended after the notebook's last section; a comment that fails to file must not
// be left behind.
func TestInsertCommentWithEvidenceFilesInOneTransaction(t *testing.T) {
	c, p := newFakePostgres()
	p.On("INSERT INTO comments", fakedb.Row("C1", "L1", pgTS, "hi", "alice", "{}", "q", pgTS, pgTS, "F1", ""))
	p.On("COALESCE(MAX(order_index) + 1, 0)", fakedb.Row(int64(4)))
	p.On("INSERT INTO notebook_sections", fakedb.Row("S1"))
	p.On(sectionSelect, sectionRow("S1", 4))

	title := "evidence"
	w, err := c.InsertCommentWithEvidence(context.Background(), Comment{LogID: "L1", Author: "alice", FractalID: "F1", Tags: []string{"t"}}, "N1", &title, false)
	if err != nil {
		t.Fatal(err)
	}
	if w.Existing || w.Comment.ID != "C1" || w.Section == nil || w.Section.ID != "S1" {
		t.Errorf("write = %+v", w)
	}
	ins := p.Matching("INSERT INTO notebook_sections")
	if len(ins) != 1 || !ins[0].InTx || ins[0].Args[5] != int64(4) || ins[0].Args[10] != "C1" {
		t.Errorf("section insert = %+v, want order 4 and the new comment id, in the transaction", ins)
	}
	cm := p.Matching("INSERT INTO comments")[0]
	if cm.Args[4] != "{\"t\"}" || cm.Args[6] != "F1" || cm.Args[7] != nil {
		t.Errorf("comment args = %v: tags as an array, fractal set, prism NULL", cm.Args)
	}
	if pgCommits(p) != 1 {
		t.Errorf("commits = %d", pgCommits(p))
	}

	// The section insert fails: nothing commits.
	c, p = newFakePostgres()
	p.On("INSERT INTO comments", fakedb.Row("C1", "L1", pgTS, "hi", "alice", "{}", "q", pgTS, pgTS, "", ""))
	p.On("COALESCE(MAX(order_index) + 1, 0)", fakedb.Row(int64(0)))
	p.On("INSERT INTO notebook_sections", fakedb.PGResult{Err: errors.New("fk")})
	if _, err := c.InsertCommentWithEvidence(context.Background(), Comment{LogID: "L1"}, "N1", nil, false); err == nil {
		t.Fatal("expected the section failure")
	}
	if pgCommits(p) != 0 || pgRollbacks(p) != 1 {
		t.Errorf("commits=%d rollbacks=%d: an unfiled comment must not be left behind", pgCommits(p), pgRollbacks(p))
	}

	// No notebook: the comment alone, empty tags bound as NULL.
	c, p = newFakePostgres()
	p.On("INSERT INTO comments", fakedb.Row("C2", "L1", pgTS, "hi", "alice", "{}", "q", pgTS, pgTS, "", ""))
	w, err = c.InsertCommentWithEvidence(context.Background(), Comment{LogID: "L1", Author: "alice"}, "", nil, true)
	if err != nil || w.Section != nil || w.Comment.ID != "C2" {
		t.Fatalf("write = %+v, %v", w, err)
	}
	if len(p.Matching("pg_advisory_xact_lock")) != 0 || len(p.Matching("notebook_sections")) != 0 {
		t.Error("an unfiled comment takes no lock and writes no section")
	}
	if args := p.Matching("INSERT INTO comments")[0].Args; args[4] != nil || args[6] != nil {
		t.Errorf("args = %v", args)
	}
}

// Bulk filing skips comments already in the notebook (and duplicates within the
// request), appends in order, and honours an explicit index without consuming an
// append slot. A negative index would sort above sections the caller never saw, so
// it is treated as "append".
func TestInsertEvidenceSections(t *testing.T) {
	c, p := newFakePostgres()
	p.On("comment_id IS NOT NULL", fakedb.RowsOf([]driver.Value{"C-present"}))
	p.On("COALESCE(MAX(order_index) + 1, 0)", fakedb.Row(int64(10)))
	p.On("INSERT INTO notebook_sections", fakedb.Row("S"))
	p.On(sectionSelect, sectionRow("S", 0))

	neg, pinned := -3, 2
	items := []EvidenceItem{
		{CommentID: "C-present"},
		{CommentID: "C1"},
		{CommentID: "C2", OrderIndex: &pinned},
		{CommentID: "C1"}, // duplicate in the request
		{CommentID: "C3", OrderIndex: &neg},
	}
	got, err := c.InsertEvidenceSections(context.Background(), "N1", items)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("%d sections, want 3", len(got))
	}
	var placed [][2]any
	for _, call := range p.Matching("INSERT INTO notebook_sections") {
		placed = append(placed, [2]any{call.Args[10], call.Args[5]})
	}
	want := [][2]any{{"C1", int64(10)}, {"C2", int64(2)}, {"C3", int64(11)}}
	if !reflect.DeepEqual(placed, want) {
		t.Errorf("placed %v, want %v", placed, want)
	}

	if got, err := c.InsertEvidenceSections(context.Background(), "N1", nil); got != nil || err != nil {
		t.Error("no items, no transaction")
	}
}

// Removing a log from a notebook deletes its sections and then only those comments
// that are now empty stars; annotated comments stay.
func TestDeleteEvidence(t *testing.T) {
	c, p := newFakePostgres()
	p.On("DELETE FROM notebook_sections s", fakedb.RowsOf([]driver.Value{"S1", "C1"}, []driver.Value{"S2", "C2"}))
	ids, err := c.DeleteEvidence(context.Background(), "N1", "L1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ids, []string{"S1", "S2"}) {
		t.Errorf("ids = %v", ids)
	}
	dels := p.Matching("DELETE FROM comments")
	if len(dels) != 2 || !strings.Contains(dels[0].Query, "text = ''") || !strings.Contains(dels[0].Query, "NOT EXISTS") {
		t.Errorf("comment cleanup = %+v", dels)
	}
	if pgCommits(p) != 1 {
		t.Error("the removal must commit as one transaction")
	}

	c, p = newFakePostgres()
	p.On("DELETE FROM notebook_sections s", fakedb.RowsOf([]driver.Value{"S1", "C1"}))
	p.On("DELETE FROM comments", fakedb.PGResult{Err: errors.New("x")})
	if _, err := c.DeleteEvidence(context.Background(), "N1", "L1"); err == nil || pgCommits(p) != 0 {
		t.Errorf("err = %v commits = %d", err, pgCommits(p))
	}
}

func TestActiveNotebookAndCaptureState(t *testing.T) {
	c, p := newFakePostgres()
	if id, err := c.GetActiveNotebook(context.Background(), "u", "f:1"); id != "" || err != nil {
		t.Errorf("no row = %q, %v; want empty and no error", id, err)
	}
	p.On("FROM user_active_notebooks", fakedb.Row("N1"))
	if id, _ := c.GetActiveNotebook(context.Background(), "u", "f:1"); id != "N1" {
		t.Errorf("id = %q", id)
	}
	c.SetActiveNotebook(context.Background(), "u", "f:1", "N1")
	c.ClearActiveNotebook(context.Background(), "u", "f:1")
	if len(p.Matching("ON CONFLICT (username, scope_key)")) != 1 || len(p.Matching("DELETE FROM user_active_notebooks")) != 1 {
		t.Error("set must upsert and clear must delete")
	}

	// An unset scope binds NULL so IS NOT DISTINCT FROM matches the other scope's rows.
	p.On("EXISTS(SELECT 1 FROM notebooks", fakedb.Row(true, false))
	hasNB, hasC, err := c.ScopeCaptureState(context.Background(), "F1", "")
	if err != nil || !hasNB || hasC {
		t.Errorf("state = %v %v %v", hasNB, hasC, err)
	}
	if args := p.Matching("EXISTS(SELECT 1 FROM notebooks")[0].Args; args[0] != "F1" || args[1] != nil {
		t.Errorf("args = %v", args)
	}
}

// The first capture creates a scratch notebook in the requested scope; later ones
// reuse it. A prism caller's stray fractal id must not decide the scope.
func TestGetOrCreateScratchNotebook(t *testing.T) {
	c, p := newFakePostgres()
	p.On("INSERT INTO notebooks", fakedb.Row("N-new", ScratchNotebookName, "d", "24h", nil, nil, int64(1000), nil, "P1", []byte("[]"), "UTC", "alice", pgTS, pgTS))
	nb, err := c.GetOrCreateScratchNotebook(context.Background(), "F-default", "P1", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if nb.ID != "N-new" || nb.PrismID != "P1" || nb.FractalID != "" {
		t.Errorf("notebook = %+v", nb)
	}
	lookup := p.Matching("SELECT id::text FROM notebooks")[0]
	if !strings.Contains(lookup.Query, "prism_id = $3") || !strings.Contains(lookup.Query, "locked_at IS NULL") {
		t.Errorf("lookup = %q: scoped to the prism and skipping locked notebooks", lookup.Query)
	}
	ins := p.Matching("INSERT INTO notebooks")[0]
	if ins.Args[6] != nil || ins.Args[7] != "P1" {
		t.Errorf("insert scope args = %v, %v", ins.Args[6], ins.Args[7])
	}

	if _, err := c.GetOrCreateScratchNotebook(context.Background(), "", "", "alice"); err == nil {
		t.Error("an unscoped capture must be refused")
	}
}

// Locking seals the record: it refuses while a query section has never run, clears
// every user's capture target, and explains a no-op precisely.
func TestLockNotebook(t *testing.T) {
	ctx := context.Background()

	c, p := newFakePostgres()
	p.On("UPDATE notebooks SET locked_at = NOW()", fakedb.Affected(1))
	p.On("last_results IS NULL", fakedb.Row(int64(2)))
	_, err := c.LockNotebook(ctx, "N1", "alice")
	var unrun *UnrunQueriesError
	if !errors.As(err, &unrun) || unrun.Count != 2 {
		t.Fatalf("err = %v, want UnrunQueriesError{2}", err)
	}
	if pgCommits(p) != 0 {
		t.Error("a refused lock must roll back")
	}

	c, p = newFakePostgres()
	p.On("UPDATE notebooks SET locked_at = NOW()", fakedb.Affected(0))
	p.On("SELECT EXISTS(SELECT 1 FROM notebooks", fakedb.Row(true))
	if _, err := c.LockNotebook(ctx, "N1", "alice"); !errors.Is(err, ErrNotebookAlreadyLocked) {
		t.Errorf("err = %v, want ErrNotebookAlreadyLocked", err)
	}
	c, p = newFakePostgres()
	p.On("UPDATE notebooks SET locked_at = NOW()", fakedb.Affected(0))
	p.On("SELECT EXISTS(SELECT 1 FROM notebooks", fakedb.Row(false))
	if _, err := c.LockNotebook(ctx, "N1", "alice"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("err = %v, want sql.ErrNoRows for a missing notebook", err)
	}

	c, p = newFakePostgres()
	p.On("UPDATE notebooks SET locked_at = NOW()", fakedb.Affected(1))
	p.On("last_results IS NULL", fakedb.Row(int64(0)))
	p.On("FROM notebooks n", fakedb.PGResult{Err: errors.New("read back")})
	c.LockNotebook(ctx, "N1", "")
	if pgCommits(p) != 1 || len(p.Matching("DELETE FROM user_active_notebooks")) != 1 {
		t.Error("a successful lock commits and clears the capture targets")
	}
	if args := p.Matching("UPDATE notebooks SET locked_at")[0].Args; args[1] != nil {
		t.Errorf("locked_by = %v, want NULL for an unattributed lock", args[1])
	}

	c, p = newFakePostgres()
	p.On("UPDATE notebooks SET locked_at = NULL", fakedb.Affected(0))
	p.On("SELECT EXISTS(SELECT 1 FROM notebooks", fakedb.Row(true))
	if _, err := c.UnlockNotebook(ctx, "N1"); !errors.Is(err, ErrNotebookNotLocked) {
		t.Errorf("err = %v, want ErrNotebookNotLocked", err)
	}
}

// A TOTP step and a recovery code are each spendable once: the comparison is in
// the UPDATE, and "claimed" means exactly one row changed.
func TestOneTimeSecretsAreSpentOnce(t *testing.T) {
	c, p := newFakePostgres()
	p.OnOnce("totp_last_counter < $2", fakedb.Affected(1))
	p.On("totp_last_counter < $2", fakedb.Affected(0))
	if ok, _ := c.SpendTOTPCounter(context.Background(), "u", 5); !ok {
		t.Error("the first spend claims the step")
	}
	if ok, _ := c.SpendTOTPCounter(context.Background(), "u", 5); ok {
		t.Error("a replayed step must be refused")
	}
	p.OnOnce("SET used_at = NOW()", fakedb.Affected(1))
	p.On("SET used_at = NOW()", fakedb.Affected(0))
	if ok, _ := c.ConsumeRecoveryCode(context.Background(), "u", "h"); !ok {
		t.Error("an unused code is accepted")
	}
	if ok, _ := c.ConsumeRecoveryCode(context.Background(), "u", "h"); ok {
		t.Error("a used code must be refused")
	}
	p.ClearRules()
	p.On("totp_last_counter < $2", fakedb.PGResult{Err: errors.New("down")})
	if _, err := c.SpendTOTPCounter(context.Background(), "u", 6); err == nil {
		t.Error("expected the error")
	}
}

// Replacing recovery codes is all-or-nothing, and clearing TOTP removes the codes
// with the enrollment.
func TestRecoveryCodeLifecycle(t *testing.T) {
	c, p := newFakePostgres()
	if err := c.ReplaceRecoveryCodes(context.Background(), "u", []string{"h1", "h2"}); err != nil {
		t.Fatal(err)
	}
	calls := p.Matching("user_recovery_codes")
	if len(calls) != 3 || !strings.HasPrefix(strings.TrimSpace(calls[0].Query), "DELETE") || !calls[2].InTx {
		t.Errorf("calls = %+v", calls)
	}

	c, p = newFakePostgres()
	p.On("INSERT INTO user_recovery_codes", fakedb.PGResult{Err: errors.New("x")})
	if err := c.ReplaceRecoveryCodes(context.Background(), "u", []string{"h1"}); err == nil || pgCommits(p) != 0 {
		t.Error("a failed insert must not commit a deleted code list")
	}

	c, p = newFakePostgres()
	c.ClearUserTOTP(context.Background(), "u")
	if len(p.Matching("totp_secret = NULL")) != 1 || len(p.Matching("DELETE FROM user_recovery_codes")) != 1 || pgCommits(p) != 1 {
		t.Error("clearing TOTP must remove the codes in the same transaction")
	}

	p.On("SELECT COUNT(*) FROM user_recovery_codes", fakedb.Row(int64(7)))
	if n, err := c.CountUnusedRecoveryCodes(context.Background(), "u"); n != 7 || err != nil {
		t.Errorf("count = %d, %v", n, err)
	}
	c.StartUserTOTP(context.Background(), "u", "sealed")
	c.ConfirmUserTOTP(context.Background(), "u", 9)
	if start := p.Matching("totp_enrolled_at = NULL, totp_last_counter = 0"); len(start) != 2 {
		t.Error("starting enrollment must not count as enrolled")
	}
}

// Role changes map only "admin" to is_admin; the update picks its statement by
// which fields were given; and a missing user is an error rather than silence.
func TestUpdateUser(t *testing.T) {
	cases := []struct {
		name, display, role string
		wantQuery           string
		wantArgs            []driver.Value
	}{
		{"both", "Bob", "admin", "SET display_name = $2, is_admin = $3", []driver.Value{"u", "Bob", true}},
		{"name only", "Bob", "", "SET display_name = $2 WHERE", []driver.Value{"u", "Bob"}},
		{"role only", "", "viewer", "SET is_admin = $2", []driver.Value{"u", false}},
	}
	for _, tc := range cases {
		c, p := newFakePostgres()
		p.On("UPDATE users", fakedb.Affected(1))
		if err := c.UpdateUser(context.Background(), "u", tc.display, tc.role); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		call := p.Matching("UPDATE users")[0]
		if !strings.Contains(call.Query, tc.wantQuery) || !reflect.DeepEqual(call.Args, tc.wantArgs) {
			t.Errorf("%s: %q %v", tc.name, call.Query, call.Args)
		}
	}
	c, p := newFakePostgres()
	if err := c.UpdateUser(context.Background(), "u", "", ""); err != nil || len(p.Calls()) != 0 {
		t.Error("nothing to change is a no-op")
	}
	p.On("UPDATE users", fakedb.Affected(0))
	if err := c.UpdateUser(context.Background(), "ghost", "X", ""); err == nil {
		t.Error("a missing user must be an error")
	}
}

// Every user write that matches no row reports it, each with its own meaning.
func TestUserWritesReportMissingRows(t *testing.T) {
	c, p := newFakePostgres()
	p.On("UPDATE users", fakedb.Affected(0))
	ctx := context.Background()
	if err := c.UpdatePasswordHash(ctx, "u", "h"); !errors.Is(err, ErrUserNotFound) {
		t.Errorf("UpdatePasswordHash = %v", err)
	}
	for name, err := range map[string]error{
		"SetUserEnabled":         c.SetUserEnabled(ctx, "u", false),
		"SetUserDisplayTimezone": c.SetUserDisplayTimezone(ctx, "u", "UTC"),
		"AcceptInvite":           c.AcceptInvite(ctx, "t", "h"),
		"RegenerateInvite":       c.RegenerateInvite(ctx, "u", "t", pgTS),
		"ResetUserToInvite":      c.ResetUserToInvite(ctx, "u", "t", pgTS),
	} {
		if err == nil {
			t.Errorf("%s: no error for a missing row", name)
		}
	}
	if q := p.Matching("password_hash = '!invite'"); len(q) != 1 || !strings.Contains(q[0].Query, "!= 'oidc'") {
		t.Error("an SSO user's password must never be reset to an invite")
	}
}

func TestGetUser(t *testing.T) {
	c, p := newFakePostgres()
	if _, err := c.GetUser(context.Background(), "ghost"); !errors.Is(err, ErrUserNotFound) {
		t.Errorf("err = %v", err)
	}
	// An SSO user has a NULL password hash; it reads as empty, never as a hash.
	p.On("FROM users\n\t\tWHERE username = $1", fakedb.Row("u", nil, "U", "#000", "U", pgTS, nil, true, "oidc", false, true, "UTC", "", false, int64(0)))
	u, err := c.GetUser(context.Background(), "u")
	if err != nil {
		t.Fatal(err)
	}
	if u.PasswordHash != "" || u.AuthProvider != "oidc" || !u.IsAdmin || u.LastLogin != nil {
		t.Errorf("user = %+v", u)
	}

	c, p = newFakePostgres()
	if _, err := c.GetUserByInviteToken(context.Background(), "t"); err == nil || !strings.Contains(err.Error(), "invalid or expired") {
		t.Errorf("err = %v", err)
	}
	p.On("invite_token_hash = $1", fakedb.Row("u", "U", "#000", "U", pgTS, false))
	if u, err := c.GetUserByInviteToken(context.Background(), "t"); err != nil || u.Username != "u" {
		t.Errorf("user = %v, %v", u, err)
	}
}

// The comment() BQL resolver builds its filter dynamically; the placeholders must
// line up with the args whatever combination is used, and a prism scope wins over
// a fractal one.
func TestGetCommentedLogIDsFiltered(t *testing.T) {
	c, p := newFakePostgres()
	p.On("SELECT DISTINCT log_id FROM comments", fakedb.RowsOf([]driver.Value{"L1"}, []driver.Value{"L2"}))
	ids, err := c.GetCommentedLogIDsFiltered(context.Background(), "F1", "P1", pgTS, pgTS, []string{"x", "y"}, "50%_off")
	if err != nil || !reflect.DeepEqual(ids, []string{"L1", "L2"}) {
		t.Fatalf("ids = %v, %v", ids, err)
	}
	call := p.Calls()[0]
	for _, want := range []string{"AND prism_id = $3", "AND tags && $4", `AND text ILIKE $5 ESCAPE '\'`, "LIMIT 50000"} {
		if !strings.Contains(call.Query, want) {
			t.Errorf("query %q is missing %q", call.Query, want)
		}
	}
	if strings.Contains(call.Query, "fractal_id") {
		t.Error("a prism scope must not also filter by fractal")
	}
	// The keyword is a search term: its LIKE wildcards match literally.
	if !reflect.DeepEqual(call.Args[2:], []driver.Value{"P1", `{"x","y"}`, `%50\%\_off%`}) {
		t.Errorf("args = %v", call.Args)
	}

	c, p = newFakePostgres()
	c.GetCommentedLogIDsFiltered(context.Background(), "F1", "", pgTS, pgTS, nil, "")
	if q := p.Calls()[0].Query; !strings.Contains(q, "AND fractal_id = $3") || strings.Contains(q, "$4") {
		t.Errorf("query = %q", q)
	}
}

func TestGetCommentsByScope(t *testing.T) {
	c, p := newFakePostgres()
	if _, _, err := c.GetCommentsByScope(context.Background(), "", "", CommentFilingAny, 10, 0); err == nil {
		t.Error("an unscoped listing must be refused")
	}
	p.On("SELECT COUNT(*) FROM comments c", fakedb.Row(int64(2)))
	p.On("LEFT JOIN LATERAL", fakedb.RowsOf(
		[]driver.Value{"C1", "L1", pgTS, "t", "a", "{x}", "q", pgTS, pgTS, "", "P1", "A", "#f", "A", `[{"id":"N1","name":"case"}]`},
		[]driver.Value{"C2", "L2", pgTS, "t", "a", "{}", "q", pgTS, pgTS, "", "P1", "", "", "", `not json`},
	))
	got, total, err := c.GetCommentsByScope(context.Background(), "F-ignored", "P1", CommentFilingUnfiled, 10, 5)
	if err != nil || total != 2 || len(got) != 2 {
		t.Fatalf("got %v, %d, %v", got, total, err)
	}
	if !reflect.DeepEqual(got[0].Notebooks, []CommentNotebook{{ID: "N1", Name: "case"}}) || got[1].Notebooks != nil {
		t.Errorf("notebooks = %v / %v", got[0].Notebooks, got[1].Notebooks)
	}
	for _, call := range p.Calls() {
		if !strings.Contains(call.Query, "c.prism_id = $1::uuid") || !strings.Contains(call.Query, "NOT EXISTS (SELECT 1 FROM notebook_sections") {
			t.Errorf("query %q must be prism-scoped and unfiled-only", call.Query)
		}
	}
}

// Non-admins may only touch their own comments, and every bulk operation is
// confined to one scope.
func TestBulkCommentOpsScopeAndAuthorship(t *testing.T) {
	ctx := context.Background()
	ids := []string{"C1"}
	type op func(c *PostgresClient, admin bool) (int64, error)
	ops := map[string]op{
		"add": func(c *PostgresClient, admin bool) (int64, error) {
			return c.BulkAddTagToComments(ctx, ids, "t", "bob", admin, "F1", "")
		},
		"remove": func(c *PostgresClient, admin bool) (int64, error) {
			return c.BulkRemoveTagFromComments(ctx, ids, "t", "bob", admin, "F1", "")
		},
		"delete": func(c *PostgresClient, admin bool) (int64, error) {
			return c.BulkDeleteComments(ctx, ids, "bob", admin, "F1", "")
		},
	}
	for name, run := range ops {
		for _, admin := range []bool{true, false} {
			c, p := newFakePostgres()
			p.On("comments", fakedb.Affected(3))
			n, err := run(c, admin)
			if err != nil || n != 3 {
				t.Errorf("%s admin=%v: %d, %v", name, admin, n, err)
			}
			q := p.Calls()[0].Query
			if admin == strings.Contains(q, "author = ") {
				t.Errorf("%s admin=%v: author filter present=%v", name, admin, !admin)
			}
			if !strings.Contains(q, "fractal_id = $") || !strings.Contains(q, "prism_id = $") {
				t.Errorf("%s: unscoped statement %q", name, q)
			}
		}
		c, p := newFakePostgres()
		p.On("comments", fakedb.PGResult{Err: errors.New("x")})
		if _, err := run(c, false); err == nil {
			t.Errorf("%s: error swallowed", name)
		}
	}
}

func TestUpdateNotebookBuildsOnlyGivenFields(t *testing.T) {
	c, p := newFakePostgres()
	p.On("UPDATE notebooks", fakedb.Affected(1))
	name, tz, max := "n", "Bogus/Zone", 50
	if err := c.UpdateNotebook(context.Background(), "N1", &name, nil, nil, &tz, nil, nil, nil, nil, &max); err != nil {
		t.Fatal(err)
	}
	call := p.Calls()[0]
	if !strings.Contains(call.Query, "SET name = $1, max_results_per_section = $2, timezone = $3, updated_at = NOW()") || !strings.Contains(call.Query, "WHERE id = $4") {
		t.Errorf("query = %q", call.Query)
	}
	if !reflect.DeepEqual(call.Args, []driver.Value{"n", int64(50), "UTC", "N1"}) {
		t.Errorf("args = %v: an invalid zone is stored as UTC", call.Args)
	}
	if err := c.UpdateNotebook(context.Background(), "N1", nil, nil, nil, nil, nil, nil, nil, nil, nil); err == nil {
		t.Error("an empty update must be refused")
	}
	p.ClearRules()
	p.On("UPDATE notebooks", fakedb.Affected(0))
	if err := c.UpdateNotebook(context.Background(), "N1", &name, nil, nil, nil, nil, nil, nil, nil, nil); err == nil {
		t.Error("a missing notebook must be reported")
	}
}

func TestUpdateNotebookSection(t *testing.T) {
	c, p := newFakePostgres()
	p.On("UPDATE notebook_sections", fakedb.Affected(1))
	content, tags := "body", []string{"a"}
	at := pgTS
	if err := c.UpdateNotebookSection(context.Background(), "S1", NotebookSectionUpdate{Content: &content, Tags: &tags, EventTime: &at, ClearEventTime: true}); err != nil {
		t.Fatal(err)
	}
	call := p.Calls()[0]
	if !strings.Contains(call.Query, "SET content = $1, tags = $2, event_time = NULL, updated_at = NOW()") || !strings.Contains(call.Query, "WHERE id = $3") {
		t.Errorf("query = %q: ClearEventTime wins over EventTime", call.Query)
	}
	if len(call.Args) != 3 {
		t.Errorf("args = %v", call.Args)
	}
	if err := c.UpdateNotebookSection(context.Background(), "S1", NotebookSectionUpdate{}); err == nil {
		t.Error("an empty update must be refused")
	}
}

func TestGetNotebooksByScopeEscapesSearch(t *testing.T) {
	c, p := newFakePostgres()
	p.On("SELECT COUNT(*) FROM notebooks n", fakedb.Row(int64(0)))
	if _, total, err := c.GetNotebooksByScope(context.Background(), "F1", "", " 50%_off ", 10, 0); err != nil || total != 0 {
		t.Fatalf("total=%d err=%v", total, err)
	}
	count := p.Calls()[0]
	if count.Args[1] != `%50\%\_off%` {
		t.Errorf("search arg = %v: LIKE wildcards in the term must match literally", count.Args[1])
	}
	list := p.Calls()[1]
	if !strings.Contains(list.Query, "LIMIT $3 OFFSET $4") || !reflect.DeepEqual(list.Args[2:], []driver.Value{int64(10), int64(0)}) {
		t.Errorf("paging = %q %v", list.Query, list.Args)
	}
}

// Version 1 dashboards are converted to the finer grid on their first layout write,
// exactly once; widget writes are confined to their dashboard.
func TestDashboardGridUpgradeAndWidgetScope(t *testing.T) {
	c, p := newFakePostgres()
	p.On("SET grid_version = 2", fakedb.Affected(1))
	p.On("WHERE id = $5 AND dashboard_id = $6", fakedb.Affected(1))
	n, err := c.UpdateDashboardWidgetLayouts(context.Background(), "D1", []WidgetLayout{{ID: "W1"}, {ID: "W2"}})
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	scale := p.Matching("pos_x = pos_x * $2")
	if len(scale) != 1 || !reflect.DeepEqual(scale[0].Args, []driver.Value{"D1", int64(dashboardGridV1Col), int64(dashboardGridV1Row)}) {
		t.Errorf("scale = %+v", scale)
	}

	c, p = newFakePostgres()
	p.On("SET grid_version = 2", fakedb.Affected(0)) // already version 2
	c.UpdateDashboardWidgetLayouts(context.Background(), "D1", nil)
	if len(p.Matching("pos_x = pos_x * $2")) != 0 {
		t.Error("a version 2 dashboard must not be rescaled")
	}

	c, p = newFakePostgres()
	p.On("dashboard_widgets", fakedb.Affected(0))
	if err := c.DeleteDashboardWidget(context.Background(), "D1", "W-other"); !errors.Is(err, ErrDashboardWidgetNotFound) {
		t.Errorf("err = %v, want ErrDashboardWidgetNotFound", err)
	}
	title := "t"
	if err := c.UpdateDashboardWidget(context.Background(), "D1", "W-other", &title, nil, nil, nil); !errors.Is(err, ErrDashboardWidgetNotFound) {
		t.Errorf("err = %v", err)
	}
	p.ClearRules()
	p.On("dashboard_widgets", fakedb.PGResult{Err: errors.New("x")})
	if err := c.DeleteDashboardWidget(context.Background(), "D1", "W1"); err == nil || errors.Is(err, ErrDashboardWidgetNotFound) {
		t.Errorf("err = %v, want the write failure", err)
	}
}

// A widget deleted while its query ran matches no row: that is a lost race, not a
// query failure.
func TestUpdateDashboardWidgetResultsToleratesDeletedWidget(t *testing.T) {
	c, p := newFakePostgres()
	at, err := c.UpdateDashboardWidgetResults(context.Background(), "W1", "[]", nil)
	if err != nil || !at.IsZero() {
		t.Errorf("deleted widget = %v, %v", at, err)
	}
	p.On("RETURNING last_executed_at", fakedb.Row(pgTS))
	if at, err := c.UpdateDashboardWidgetResults(context.Background(), "W1", "[]", nil); err != nil || !at.Equal(pgTS) {
		t.Errorf("stored time = %v, %v", at, err)
	}
}

// An advisory lock that cannot be released must not go back to the pool still
// held: the connection is discarded, which releases it.
func TestAdvisoryLockReleasedOrDiscarded(t *testing.T) {
	c, p := newFakePostgres()
	if unlock, ok := c.TryAdvisoryLock(context.Background(), LockAlertEngine); ok || unlock != nil {
		t.Fatal("a lock that was not granted must not be reported held")
	}

	p.On("pg_try_advisory_lock", fakedb.Row(true))
	p.On("pg_advisory_unlock", fakedb.Row(true))
	unlock, ok := c.TryAdvisoryLock(context.Background(), LockAlertEngine)
	if !ok {
		t.Fatal("expected the lock")
	}
	unlock()
	if got := p.Matching("pg_advisory_unlock"); len(got) != 1 || got[0].Args[0] != LockAlertEngine {
		t.Errorf("unlock = %+v", got)
	}
	closesBefore := pgCloses(p)

	p.ClearRules()
	p.On("pg_try_advisory_lock", fakedb.Row(true))
	p.On("pg_advisory_unlock", fakedb.Row(false)) // not held by this session
	unlock, _ = c.TryAdvisoryLock(context.Background(), LockAlertEngine)
	unlock()
	if pgCloses(p) != closesBefore+1 {
		t.Errorf("closes = %d, want the connection discarded after a failed unlock", pgCloses(p)-closesBefore)
	}
}

func TestSettingsAndSpoolStatus(t *testing.T) {
	c, p := newFakePostgres()
	if _, err := c.GetSetting(context.Background(), "k"); err == nil || err.Error() != "setting not found" {
		t.Errorf("err = %v", err)
	}
	if s := c.ReadSpoolStatus(context.Background()); s.Provisioned {
		t.Error("never published reads as not provisioned")
	}
	if err := c.PublishSpoolStatus(context.Background(), SpoolStatus{Provisioned: true, UsedBytes: 5}); err != nil {
		t.Fatal(err)
	}
	published := p.Matching("INSERT INTO settings")[0].Args[1].(string)
	if !strings.Contains(published, `"provisioned":true`) || strings.Contains(published, `"updated_at":"0001`) {
		t.Errorf("published %s: must be stamped", published)
	}
	p.On("SELECT value FROM settings", fakedb.Row(published))
	if s := c.ReadSpoolStatus(context.Background()); !s.Provisioned || s.UsedBytes != 5 || s.UpdatedAt.IsZero() {
		t.Errorf("read back %+v", s)
	}
}

func TestSystemMetricsSQL(t *testing.T) {
	c, p := newFakePostgres()
	if err := c.InsertSystemMetrics(context.Background(), pgTS, nil); err != nil || len(p.Calls()) != 0 {
		t.Error("no samples, no statement")
	}
	c.InsertSystemMetrics(context.Background(), pgTS, []SystemMetricSample{{Metric: "a", Value: 1}, {Metric: "b", Node: "n", Value: 2}})
	call := p.Calls()[0]
	if !strings.HasSuffix(call.Query, "VALUES ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10)") || len(call.Args) != 10 {
		t.Errorf("insert = %q %v", call.Query, call.Args)
	}

	p.On("GROUP BY node, bucket", fakedb.RowsOf([]driver.Value{"n1", pgTS, 1.5}, []driver.Value{"n1", pgTS.Add(time.Minute), 2.5}))
	byNode, err := c.QueryMetricSeriesByNode(context.Background(), "cpu", time.Hour, 0)
	if err != nil || len(byNode["n1"]) != 2 {
		t.Errorf("byNode = %v, %v", byNode, err)
	}
	if args := p.Matching("GROUP BY node, bucket")[0].Args; args[0] != int64(60) || args[2] != int64(3600) {
		t.Errorf("args = %v: a non-positive bucket means 60s", args)
	}
	p.ClearRules()
	if byNode, _ := c.QueryMetricSeriesByNode(context.Background(), "cpu", time.Hour, 30); byNode != nil {
		t.Error("no named nodes reads as nil (single-node install)")
	}
	p.On("GROUP BY bucket", fakedb.RowsOf([]driver.Value{pgTS, 2.4}))
	pts, err := c.QueryMetricSeries(context.Background(), "cpu", time.Hour, 0)
	if err != nil || len(pts) != 1 || pts[0].Value != 2.4 {
		t.Errorf("series = %v, %v", pts, err)
	}
	p.On("DELETE FROM system_metrics", fakedb.Affected(9))
	if n, err := c.PruneSystemMetrics(context.Background(), 7); n != 9 || err != nil {
		t.Errorf("pruned %d, %v", n, err)
	}

	// The monitors read their history back through the same series.
	m := NewDDLMonitor(&ClickHouseClient{}, c, nil)
	p.ClearRules()
	p.On("GROUP BY bucket", fakedb.RowsOf([]driver.Value{pgTS, 2.6}))
	if h := m.History(context.Background(), 0, 0); len(h) != 1 || h[0].Pending != 3 || h[0].Time != pgTS.Unix() {
		t.Errorf("history = %+v", h)
	}
	dm := NewDistributionMonitor(&ClickHouseClient{}, c, nil)
	if h := dm.History(context.Background(), 0, 0); len(h) != 1 || h[0].DataFiles != 3 {
		t.Errorf("history = %+v", h)
	}
	p.ClearRules()
	p.On("GROUP BY bucket", fakedb.PGResult{Err: errors.New("x")})
	if m.History(context.Background(), time.Hour, 60) != nil || dm.History(context.Background(), time.Hour, 60) != nil {
		t.Error("a failed history read returns nil")
	}
}

func TestEnsureIngestRole(t *testing.T) {
	c, p := newFakePostgres()
	if err := c.EnsureIngestRole(context.Background(), ""); err != nil || len(p.Calls()) != 0 {
		t.Fatal("an empty password skips provisioning")
	}
	if err := c.EnsureIngestRole(context.Background(), "it's"); err != nil {
		t.Fatal(err)
	}
	if q := p.Calls()[1].Query; q != "ALTER ROLE bifract_ingest WITH LOGIN PASSWORD 'it''s'" {
		t.Errorf("password sync = %q", q)
	}
	all := ""
	for _, call := range p.Calls() {
		all += call.Query + "\n"
	}
	for _, forbidden := range []string{"users", "sessions", "api_keys"} {
		if strings.Contains(all, " "+forbidden+",") || strings.Contains(all, " "+forbidden+" TO") {
			t.Errorf("the ingest role must never be granted %s", forbidden)
		}
	}
	c, p = newFakePostgres()
	p.On("GRANT", fakedb.PGResult{Err: errors.New("x")})
	if err := c.EnsureIngestRole(context.Background(), "pw"); err == nil {
		t.Error("a failed grant must be reported")
	}
}

func TestClearLogDataState(t *testing.T) {
	c, p := newFakePostgres()
	if err := c.ClearLogDataState(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if len(p.Calls()) != len(ClearedLogDataPostgresStatements())+1 || p.Calls()[len(p.Calls())-1].Query != "DELETE FROM comments" || pgCommits(p) != 1 {
		t.Errorf("calls = %d, commits = %d", len(p.Calls()), pgCommits(p))
	}
	for _, call := range p.Calls() {
		if !call.InTx {
			t.Errorf("%q ran outside the transaction", call.Query)
		}
	}
	c, p = newFakePostgres()
	p.On("TRUNCATE", fakedb.PGResult{Err: errors.New("x")})
	if err := c.ClearLogDataState(context.Background(), false); err == nil || pgCommits(p) != 0 {
		t.Error("a partial clear must not commit")
	}
}

// Shared links are no-auth access, so only the token hash is stored and it is
// never read back out: listing or resolving a link must not select it.
func TestDashboardSharedLinksNeverReadTheHash(t *testing.T) {
	c, p := newFakePostgres()
	linkRow := []driver.Value{"K1", "D1", "abcd", nil, "alice", pgTS, nil, nil, nil}
	p.On("INSERT INTO dashboard_shared_links", fakedb.PGResult{Rows: [][]driver.Value{linkRow}})
	p.On("WHERE dashboard_id = $1 AND revoked_at IS NULL", fakedb.RowsOf(linkRow))
	p.On("WHERE token_hash = $1", fakedb.RowsOf(linkRow))

	l, err := c.CreateDashboardSharedLink(context.Background(), "D1", "hash", "abcd", "", "", nil)
	if err != nil || l.ID != "K1" || l.Label != "" || l.CreatedBy != "alice" {
		t.Fatalf("link = %+v, %v", l, err)
	}
	if args := p.Matching("INSERT INTO dashboard_shared_links")[0].Args; args[3] != nil || args[4] != nil {
		t.Errorf("args = %v: an empty label and creator bind as NULL", args)
	}
	if links, err := c.ListDashboardSharedLinks(context.Background(), "D1"); err != nil || len(links) != 1 {
		t.Errorf("list = %v, %v", links, err)
	}
	if l, err := c.GetDashboardSharedLinkByHash(context.Background(), "hash"); err != nil || l.DashboardID != "D1" {
		t.Errorf("get = %v, %v", l, err)
	}
	for _, call := range p.Calls() {
		// What a statement reads back: the RETURNING list, or the SELECT list.
		var readBack string
		if i := strings.Index(call.Query, "RETURNING"); i >= 0 {
			readBack = call.Query[i:]
		} else if i := strings.Index(call.Query, "SELECT"); i >= 0 {
			readBack = call.Query[i:strings.Index(call.Query, "FROM")]
		}
		if strings.Contains(readBack, "token_hash") {
			t.Errorf("token_hash is read back by %q", call.Query)
		}
	}

	c, p = newFakePostgres()
	if _, err := c.GetDashboardSharedLinkByHash(context.Background(), "nope"); err == nil || err.Error() != "shared link not found" {
		t.Errorf("err = %v", err)
	}
	p.On("SET revoked_at = NOW()", fakedb.Affected(0))
	if err := c.RevokeDashboardSharedLink(context.Background(), "D-other", "K1"); err == nil {
		t.Error("revoking another dashboard's link must fail")
	}
	if q := p.Matching("SET revoked_at")[0]; !strings.Contains(q.Query, "dashboard_id = $2") || !strings.Contains(q.Query, "revoked_at IS NULL") {
		t.Errorf("revoke = %q", q.Query)
	}
	p.On("SET last_accessed_at", fakedb.PGResult{Err: errors.New("x")})
	c.TouchDashboardSharedLink(context.Background(), "K1") // best effort: must not panic or surface
}

// A grant is an upsert keyed by user or by group. The user form is tried first;
// a group grant that conflicts falls back to the group-keyed upsert, while a user
// grant that fails is simply an error.
func TestGrantPermissionUpsertFallback(t *testing.T) {
	ctx := context.Background()
	group := "G1"
	row := fakedb.Row("P1", "S1", nil, "G1", "viewer", "admin", pgTS, pgTS)

	c, p := newFakePostgres()
	p.On("WHERE username IS NOT NULL", fakedb.PGResult{Err: errors.New("duplicate key")})
	p.On("WHERE group_id IS NOT NULL", row)
	fp, err := c.GrantFractalPermission(ctx, "S1", nil, &group, "viewer", "admin")
	if err != nil || fp.ID != "P1" || fp.GroupID == nil || *fp.GroupID != "G1" || fp.Username != nil {
		t.Fatalf("fractal grant = %+v, %v", fp, err)
	}
	pp, err := c.GrantPrismPermission(ctx, "S1", nil, &group, "viewer", "admin")
	if err != nil || pp.ID != "P1" {
		t.Fatalf("prism grant = %+v, %v", pp, err)
	}

	user := "bob"
	c, p = newFakePostgres()
	p.On("WHERE username IS NOT NULL", fakedb.PGResult{Err: errors.New("fk violation")})
	if _, err := c.GrantFractalPermission(ctx, "S1", &user, nil, "viewer", "admin"); err == nil {
		t.Error("a failed user grant must be an error")
	}
	if _, err := c.GrantPrismPermission(ctx, "S1", &user, nil, "viewer", "admin"); err == nil {
		t.Error("a failed user grant must be an error")
	}
	if len(p.Matching("WHERE group_id IS NOT NULL")) != 0 {
		t.Error("a user grant must never retry as a group grant")
	}

	c, p = newFakePostgres()
	p.On("permissions", fakedb.Affected(0))
	for name, err := range map[string]error{
		"RevokeFractalPermission":     c.RevokeFractalPermission(ctx, "x"),
		"UpdateFractalPermissionRole": c.UpdateFractalPermissionRole(ctx, "x", "admin"),
		"RevokePrismPermission":       c.RevokePrismPermission(ctx, "x"),
		"UpdatePrismPermissionRole":   c.UpdatePrismPermissionRole(ctx, "x", "admin"),
	} {
		if err == nil || err.Error() != "permission not found" {
			t.Errorf("%s = %v", name, err)
		}
	}
}
