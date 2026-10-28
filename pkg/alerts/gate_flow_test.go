package alerts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"bifract/internal/fakedb"
)

// crStore holds change requests and their reviews behind the scripted driver, so a
// flow that reads a proposal, changes it and reads it back sees its own writes.
type crStore struct {
	t  *testing.T
	pg *pgFake

	mu      sync.Mutex
	rows    map[string]*crSpec
	reviews map[string][][]driver.Value
	scope   map[string][2]string // id -> fractal, prism
}

func newCRStore(t *testing.T, pg *pgFake) *crStore {
	s := &crStore{t: t, pg: pg, rows: map[string]*crSpec{}, reviews: map[string][][]driver.Value{}, scope: map[string][2]string{}}
	pg.handle(sqlCRSelect, func([]driver.Value) fakedb.PGResult {
		s.mu.Lock()
		defer s.mu.Unlock()
		var out [][]driver.Value
		for _, r := range s.rows {
			out = append(out, r.row(t))
		}
		return fakedb.RowsOf(out...)
	})
	pg.handle("WHERE cr.id = $1", func(args []driver.Value) fakedb.PGResult {
		s.mu.Lock()
		defer s.mu.Unlock()
		if r, ok := s.rows[args[0].(string)]; ok {
			return fakedb.RowsOf(r.row(t))
		}
		return fakedb.PGResult{}
	})
	pg.handle(sqlReviews, func(args []driver.Value) fakedb.PGResult {
		s.mu.Lock()
		defer s.mu.Unlock()
		return fakedb.RowsOf(s.reviews[args[0].(string)]...)
	})
	pg.handle(sqlCRScope, func(args []driver.Value) fakedb.PGResult {
		s.mu.Lock()
		defer s.mu.Unlock()
		sc, ok := s.scope[args[0].(string)]
		if !ok {
			return fakedb.PGResult{}
		}
		return fakedb.Row(sc[0], sc[1])
	})
	pg.handle(sqlClaim, func(args []driver.Value) fakedb.PGResult {
		s.mu.Lock()
		defer s.mu.Unlock()
		r, ok := s.rows[args[0].(string)]
		if !ok || (r.Status != ChangeOpen && r.Status != ChangeRejected) {
			return fakedb.Affected(0)
		}
		r.Status = ChangeMerged
		return fakedb.Affected(1)
	})
	return s
}

func (s *crStore) add(c crSpec, fractalID, prismID string) *crSpec {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[c.ID] = &c
	s.scope[c.ID] = [2]string{fractalID, prismID}
	return &c
}

func (s *crStore) approve(crID, reviewer string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.rows[crID].hash(s.t)
	s.reviews[crID] = append(s.reviews[crID], reviewRow("r-"+reviewer, reviewer, ReviewApprove, h))
}

func (s *crStore) status(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows[id].Status
}

// callIndex is the position of the first statement containing sub, or -1.
func callIndex(pg *pgFake, sub string) int {
	for i, c := range pg.Calls() {
		if strings.Contains(c.Query, sub) {
			return i
		}
	}
	return -1
}

func TestSubmitChangeRequestOpensAProposal(t *testing.T) {
	m, pg := newTestManager(t)
	store := newCRStore(t, pg)
	pg.handle("INSERT INTO alert_change_requests", func(args []driver.Value) fakedb.PGResult {
		store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("New"), Author: "alice"}, "f1", "")
		return fakedb.Row("cr1")
	})

	content := eventContent("New")
	content.Labels = []string{"z", "a", "z"}
	cr, err := m.SubmitChangeRequest(context.Background(), "", "f1", "", ChangeRequestInput{
		Kind: ChangeCreate, Title: "Add rule", Content: content, Tests: sampleTests,
	}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if cr.ID != "cr1" || cr.Status != ChangeOpen || cr.Reviews == nil {
		t.Errorf("proposal = %+v", cr)
	}
	call := pg.Matching("INSERT INTO alert_change_requests")[0]
	if !strings.Contains(call.Query, "(fractal_id, alert_id") {
		t.Errorf("a fractal proposal hangs off fractal_id: %s", call.Query)
	}
	args := call.Args
	if args[0] != "f1" || args[1] != nil || args[2] != ChangeCreate || args[9] != "alice" || args[10] != "alice" {
		t.Errorf("insert args = %v", args)
	}
	var stored RevisionContent
	if err := json.Unmarshal(args[5].([]byte), &stored); err != nil || stored.References == nil || stored.WebhookActionIDs == nil {
		t.Errorf("the stored content is canonical: %+v %v", stored, err)
	}
	if args[7] != mustHash(t, stored) || args[8] != "" {
		t.Errorf("content hash %v base %v", args[7], args[8])
	}
	if len(pg.Matching(sqlHeadHash)) != 0 {
		t.Error("a create has no head to read")
	}
}

func TestSubmitChangeRequestScopesAndBases(t *testing.T) {
	ctx := context.Background()
	in := ChangeRequestInput{Kind: ChangeUpdate, AlertID: "a1", Title: "t", Content: eventContent("n")}

	t.Run("an alert from another scope does not exist here", func(t *testing.T) {
		m, pg := newTestManager(t)
		pg.answer("COALESCE(prism_id::text, '') FROM alerts WHERE id", fakedb.Row("f-other", ""))
		if _, err := m.SubmitChangeRequest(ctx, "", "f1", "", in, "alice"); !errors.Is(err, ErrAlertNotFound) {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching("INSERT INTO alert_change_requests")) != 0 {
			t.Error("a cross-scope proposal was stored")
		}
	})
	t.Run("an update records the head it was written against", func(t *testing.T) {
		m, pg := newTestManager(t)
		newCRStore(t, pg).add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "", "p1")
		pg.answer("COALESCE(prism_id::text, '') FROM alerts WHERE id", fakedb.Row("", "p1"))
		pg.answer(sqlHeadHash, fakedb.Row("head-hash"))
		pg.answer("INSERT INTO alert_change_requests", fakedb.Row("cr1"))
		if _, err := m.SubmitChangeRequest(ctx, "", "", "p1", in, "alice"); err != nil {
			t.Fatal(err)
		}
		call := pg.Matching("INSERT INTO alert_change_requests")[0]
		if !strings.Contains(call.Query, "(prism_id, alert_id") || call.Args[1] != "a1" || call.Args[8] != "head-hash" {
			t.Errorf("insert = %s %v", call.Query, call.Args)
		}
	})
	t.Run("an invalid proposal is refused before anything is read", func(t *testing.T) {
		m, pg := newTestManager(t)
		if _, err := m.SubmitChangeRequest(ctx, "", "f1", "", ChangeRequestInput{Kind: ChangeDelete, AlertID: "a1"}, "alice"); err == nil {
			t.Error("a delete without a reason must be refused")
		}
		if len(pg.Calls()) != 0 {
			t.Error("an invalid proposal reached the database")
		}
	})
	t.Run("no scope", func(t *testing.T) {
		m, _ := newTestManager(t)
		if _, err := m.SubmitChangeRequest(ctx, "", "", "", ChangeRequestInput{Kind: ChangeCreate, Title: "t", Content: eventContent("n")}, "alice"); err == nil {
			t.Error("a proposal needs a scope")
		}
	})
}

// A revision keeps the proposal's own kind, alert and scope, whatever the request
// claims, and reopens it for review.
func TestResubmitKeepsTheProposalsIdentity(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)
	store := newCRStore(t, pg)
	store.add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeRejected, Content: eventContent("old"), Author: "alice"}, "f1", "")
	pg.answer("COALESCE(prism_id::text, '') FROM alerts WHERE id", fakedb.Row("f1", ""))
	pg.answer(sqlHeadHash, fakedb.Row("head-2"))

	// The request names a different kind and no alert: both are ignored.
	revised := ChangeRequestInput{Kind: ChangeCreate, Title: "revised", Summary: "fixed", Content: eventContent("new")}
	if _, err := m.SubmitChangeRequest(ctx, "cr1", "f-elsewhere", "", revised, "alice"); err != nil {
		t.Fatal(err)
	}
	upd := pg.args("SET title = $2, summary = $3, content = $4, tests = $5")
	if len(upd) != 1 {
		t.Fatalf("want one update, got %d", len(upd))
	}
	if upd[0][0] != "cr1" || upd[0][1] != "revised" || upd[0][6] != "head-2" {
		t.Errorf("update args = %v", upd[0])
	}
	if !strings.Contains(pg.Matching("SET title = $2, summary = $3")[0].Query, "status = 'open'") {
		t.Error("a revision reopens the proposal")
	}

	if _, err := m.SubmitChangeRequest(ctx, "cr1", "f1", "", revised, "mallory"); err == nil || !strings.Contains(err.Error(), "only the author") {
		t.Errorf("err = %v", err)
	}
	store.rows["cr1"].Status = ChangeMerged
	if _, err := m.SubmitChangeRequest(ctx, "cr1", "f1", "", revised, "alice"); err == nil || !strings.Contains(err.Error(), "can no longer be edited") {
		t.Errorf("err = %v", err)
	}
	if _, err := m.SubmitChangeRequest(ctx, "missing", "f1", "", revised, "alice"); !errors.Is(err, ErrChangeRequestNotFound) {
		t.Errorf("err = %v", err)
	}
}

func TestReviewChangeRequest(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)
	store := newCRStore(t, pg)
	cr := store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")

	if _, err := m.ReviewChangeRequest(ctx, "cr1", "maybe", "", "bob", false); err == nil {
		t.Error("an unknown decision must be refused")
	}
	if _, err := m.ReviewChangeRequest(ctx, "cr1", ReviewApprove, "", "alice", false); err == nil || !strings.Contains(err.Error(), "cannot be approved by its author") {
		t.Errorf("self approval by an analyst: %v", err)
	}
	pg.answer(sqlGateConfig, gateRow(true, 1, false))
	if _, err := m.ReviewChangeRequest(ctx, "cr1", ReviewApprove, "", "alice", true); err == nil || !strings.Contains(err.Error(), "self approval is turned off") {
		t.Errorf("self approval with it turned off: %v", err)
	}
	if len(pg.Matching("INSERT INTO alert_change_reviews")) != 0 {
		t.Fatal("a refused review was recorded")
	}

	if _, err := m.ReviewChangeRequest(ctx, "cr1", ReviewReject, "needs a label", "bob", false); err != nil {
		t.Fatal(err)
	}
	ins := pg.args("INSERT INTO alert_change_reviews")[0]
	if ins[0] != "cr1" || ins[1] != "bob" || ins[3] != ReviewReject || ins[4] != "needs a label" || ins[5] != cr.hash(t) {
		t.Errorf("review args = %v", ins)
	}
	status := pg.args("UPDATE alert_change_requests SET status = $2")[0]
	if status[1] != ChangeRejected {
		t.Errorf("a rejection moves the proposal to %v", status[1])
	}
	allInTx(t, pg, "INSERT INTO alert_change_reviews")
	allInTx(t, pg, "UPDATE alert_change_requests SET status = $2")

	store.rows["cr1"].Status = ChangeMerged
	if _, err := m.ReviewChangeRequest(ctx, "cr1", ReviewReject, "", "bob", false); err == nil || !strings.Contains(err.Error(), "is merged") {
		t.Errorf("rejecting a merged proposal: %v", err)
	}

	pg.answer("INSERT INTO alert_change_reviews", fakedb.PGResult{Err: errors.New("fk")})
	store.rows["cr1"].Status = ChangeOpen
	if _, err := m.ReviewChangeRequest(ctx, "cr1", ReviewApprove, "", "bob", false); err == nil {
		t.Error("want the insert error")
	}
	if _, rollbacks := pg.txCounts(); rollbacks != 1 {
		t.Error("a failed review rolls back")
	}
}

func TestWithdrawChangeRequest(t *testing.T) {
	ctx := context.Background()

	t.Run("an update returns to the author's drafts", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		pg.answer("SET status = $2, updated_at = NOW()\n\t\t WHERE id = $1 AND status IN", fakedb.Affected(1))
		if _, err := m.WithdrawChangeRequest(ctx, "cr1", "alice", false); err != nil {
			t.Fatal(err)
		}
		if args := pg.args("WHERE id = $1 AND status IN ('open', 'changes_requested')")[0]; args[1] != ChangeDraft {
			t.Errorf("withdrawn to %v", args[1])
		}
	})
	t.Run("a delete closes for good", func(t *testing.T) {
		m, pg := newTestManager(t)
		newCRStore(t, pg).add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeDelete, Status: ChangeOpen, Summary: "noisy", Author: "alice"}, "f1", "")
		pg.answer("SET status = $2, updated_at = NOW()\n\t\t WHERE id = $1 AND status IN", fakedb.Affected(1))
		if _, err := m.WithdrawChangeRequest(ctx, "cr1", "root", true); err != nil {
			t.Fatal(err)
		}
		if args := pg.args("WHERE id = $1 AND status IN ('open', 'changes_requested')")[0]; args[1] != ChangeDiscarded {
			t.Errorf("withdrawn to %v", args[1])
		}
	})
	t.Run("the author already has a draft of that alert", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		store.add(crSpec{ID: "d1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeDraft, Content: eventContent("n"), Author: "alice"}, "f1", "")
		pg.answer("AND created_by = $2 AND status = 'draft'", fakedb.Row("d1"))
		if _, err := m.WithdrawChangeRequest(ctx, "cr1", "alice", false); !errors.Is(err, ErrDraftExists) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("losing to the unique index or a merge", func(t *testing.T) {
		m, pg := newTestManager(t)
		newCRStore(t, pg).add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		pg.answer("SET status = $2, updated_at = NOW()\n\t\t WHERE id = $1 AND status IN", fakedb.PGResult{Err: errors.New(`duplicate key value violates unique constraint "idx_alert_cr_draft_per_alert"`)})
		if _, err := m.WithdrawChangeRequest(ctx, "cr1", "alice", false); !errors.Is(err, ErrDraftExists) {
			t.Errorf("err = %v", err)
		}
		pg.answer("SET status = $2, updated_at = NOW()\n\t\t WHERE id = $1 AND status IN", fakedb.Affected(0))
		if _, err := m.WithdrawChangeRequest(ctx, "cr1", "alice", false); err == nil || !strings.Contains(err.Error(), "no longer open") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("only the author or an admin", func(t *testing.T) {
		m, pg := newTestManager(t)
		newCRStore(t, pg).add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		if _, err := m.WithdrawChangeRequest(ctx, "cr1", "bob", false); err == nil || !strings.Contains(err.Error(), "only the author or an admin") {
			t.Errorf("err = %v", err)
		}
	})
}

func TestDeleteAndListChangeRequests(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)
	if err := m.DeleteChangeRequest(ctx, "cr1"); !errors.Is(err, ErrChangeRequestNotFound) {
		t.Errorf("err = %v", err)
	}
	pg.answer("DELETE FROM alert_change_requests WHERE id = $1", fakedb.Affected(1))
	if err := m.DeleteChangeRequest(ctx, "cr1"); err != nil {
		t.Error(err)
	}

	if list, err := m.ListChangeRequests(ctx, "", "", false); err != nil || len(list) != 0 {
		t.Error("an unscoped list is empty, not every scope")
	}
	store := newCRStore(t, pg)
	store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
	store.approve("cr1", "bob")
	list, err := m.ListChangeRequests(ctx, "", "p1", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || len(list[0].Approvals()) != 1 {
		t.Errorf("each listed proposal carries its reviews: %+v", list)
	}
	q := pg.Matching(sqlCRSelect)[0].Query
	if !strings.Contains(q, "cr.prism_id = $1") || !strings.Contains(q, "status IN ('open', 'changes_requested')") {
		t.Errorf("query = %s", q)
	}
	if _, err := m.ListChangeRequests(ctx, "f1", "", false); err != nil {
		t.Fatal(err)
	}
	if q := pg.Matching(sqlCRSelect)[1].Query; !strings.Contains(q, "status <> 'draft'") {
		t.Errorf("drafts never appear in the queue: %s", q)
	}
}

func TestEvaluateChangeRequest(t *testing.T) {
	ctx := context.Background()

	t.Run("approvals against the scope's minimum", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		store.approve("cr1", "bob")
		pg.answer(sqlGateConfig, gateRow(true, 2, false))
		cr, _ := m.GetChangeRequest(ctx, "cr1")
		r, err := m.EvaluateChangeRequest(ctx, cr, TestsNever)
		if err != nil {
			t.Fatal(err)
		}
		if r.OK() || r.Blocker != "1 of 2 approvals" || r.Approvals != 1 || r.MinApprovals != 2 {
			t.Errorf("readiness = %+v", r)
		}
	})
	t.Run("a proposal written against an older head", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice", BaseHash: "old"}, "f1", "")
		store.approve("cr1", "bob")
		pg.answer(sqlHeadHash, fakedb.Row("new"))
		cr, _ := m.GetChangeRequest(ctx, "cr1")
		r, err := m.EvaluateChangeRequest(ctx, cr, TestsNever)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(r.Blocker, "needs reproposing") || r.HeadHash != "new" {
			t.Errorf("readiness = %+v", r)
		}
	})
	t.Run("an approved proposal that breaks a policy", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		store.approve("cr1", "bob")
		pg.answer(sqlPolicies, blockingLabelPolicy())
		cr, _ := m.GetChangeRequest(ctx, "cr1")
		r, err := m.EvaluateChangeRequest(ctx, cr, TestsIfNeeded)
		if err != nil {
			t.Fatal(err)
		}
		if r.Blocker != "1 policy check(s) must be fixed" || r.Policy == nil || r.Policy.Blocking != 1 {
			t.Errorf("readiness = %+v", r)
		}
	})
	t.Run("a stored test run still describing the proposal is evidence", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		spec := crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Tests: sampleTests, Author: "alice"}
		probe := &ChangeRequest{ContentHash: spec.hash(t), Tests: sampleTests}
		spec.TestResult = []byte(`{"results":[]}`)
		spec.TestResultHash = probe.testsHash()
		store.add(spec, "f1", "")
		cr, _ := m.GetChangeRequest(ctx, "cr1")
		r, err := m.EvaluateChangeRequest(ctx, cr, TestsNever)
		if err != nil {
			t.Fatal(err)
		}
		if r.Tests == nil {
			t.Error("the stored run must be reported")
		}

		// Edited since: the run no longer counts.
		spec.TestResultHash = "stale"
		store.add(spec, "f1", "")
		cr, _ = m.GetChangeRequest(ctx, "cr1")
		if r, _ := m.EvaluateChangeRequest(ctx, cr, TestsNever); r.Tests != nil {
			t.Error("a run against other content is not evidence")
		}
	})
	t.Run("a delete has no definition to judge", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeDelete, Status: ChangeOpen, Summary: "noisy", Author: "alice"}, "f1", "")
		pg.answer(sqlPolicies, blockingLabelPolicy())
		cr, _ := m.GetChangeRequest(ctx, "cr1")
		if _, err := m.EvaluateChangeRequest(ctx, cr, TestsAlways); err != nil {
			t.Fatal(err)
		}
		if len(pg.Matching(sqlPolicies)) != 0 {
			t.Error("a delete was judged against policies")
		}
	})
	t.Run("a vanished proposal", func(t *testing.T) {
		m, pg := newTestManager(t)
		newCRStore(t, pg)
		if _, err := m.EvaluateChangeRequest(ctx, &ChangeRequest{ID: "gone"}, TestsNever); !errors.Is(err, ErrChangeRequestNotFound) {
			t.Errorf("err = %v", err)
		}
	})
}

// Merging a create claims the proposal first (marking it merged), then writes the
// alert enabled and credited to its author, bypassing the gate it just satisfied.
func TestMergeCreateClaimsThenApplies(t *testing.T) {
	m, pg := newTestManager(t)
	store := newCRStore(t, pg)
	store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("Approved rule"), Tests: sampleTests, Author: "alice"}, "f1", "")
	store.approve("cr1", "bob")
	pg.answer(sqlGateConfig, gateRow(true, 1, false))
	pg.answer(sqlInsertAlert, fakedb.Row("new-alert"))
	pg.answer(sqlGetAlert, fakedb.RowsOf(alertSpec{ID: "new-alert", Name: "Approved rule", FractalID: "f1"}.getRow()))

	merged, err := m.MergeChangeRequest(context.Background(), "cr1", "bob")
	if err != nil {
		t.Fatal(err)
	}
	if merged.Status != ChangeMerged {
		t.Errorf("status = %s", merged.Status)
	}
	claim, insert := callIndex(pg, sqlClaim), callIndex(pg, sqlInsertAlert)
	if claim < 0 || insert < 0 || claim > insert {
		t.Errorf("the claim (at %d) must precede the write (at %d)", claim, insert)
	}
	if args := pg.args(sqlClaim)[0]; args[0] != "cr1" || args[1] != "bob" {
		t.Errorf("claim args = %v", args)
	}
	ins := pg.args(sqlInsertAlert)[0]
	if ins[0] != "Approved rule" || ins[4] != true || ins[10] != "high" || ins[11] != "alice" || ins[12] != "f1" {
		t.Errorf("created alert: name %v enabled %v severity %v author %v fractal %v", ins[0], ins[4], ins[10], ins[11], ins[12])
	}
	if len(pg.Matching("INSERT INTO alert_tests")) != 1 {
		t.Error("the proposal's tests are applied with it")
	}
}

// What merge refuses: an unapproved proposal, one someone else is already merging,
// and a proposal whose write fails after the claim stays claimed (by design, so it
// can never be applied twice).
func TestMergeRefusals(t *testing.T) {
	ctx := context.Background()

	t.Run("not yet approved", func(t *testing.T) {
		m, pg := newTestManager(t)
		newCRStore(t, pg).add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		_, err := m.MergeChangeRequest(ctx, "cr1", "bob")
		var blocked *MergeBlockedError
		if !errors.As(err, &blocked) || blocked.Readiness == nil || !strings.Contains(err.Error(), "0 of 1 approvals") {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching(sqlClaim)) != 0 {
			t.Error("an unready proposal was claimed")
		}
	})
	t.Run("already being merged", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		store.approve("cr1", "bob")
		pg.answer(sqlClaim, fakedb.Affected(0))
		_, err := m.MergeChangeRequest(ctx, "cr1", "bob")
		if err == nil || !strings.Contains(err.Error(), "already being merged") {
			t.Fatalf("err = %v", err)
		}
		if len(pg.Matching(sqlInsertAlert)) != 0 {
			t.Error("a lost claim still wrote the alert")
		}
	})
	t.Run("a failed apply leaves the claim in place", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		store.approve("cr1", "bob")
		pg.answer(sqlInsertAlert, fakedb.PGResult{Err: errors.New(`duplicate key value violates unique constraint "alerts_name_key"`)})
		_, err := m.MergeChangeRequest(ctx, "cr1", "bob")
		if err == nil || !strings.Contains(err.Error(), "applying proposal") {
			t.Fatalf("err = %v", err)
		}
		if store.status("cr1") != ChangeMerged {
			t.Errorf("status = %s: releasing the claim would reopen the double-apply race", store.status("cr1"))
		}
		if len(pg.Matching("SET status = 'open'")) != 0 {
			t.Error("the claim was released")
		}
	})
	t.Run("missing", func(t *testing.T) {
		m, pg := newTestManager(t)
		newCRStore(t, pg)
		if _, err := m.MergeChangeRequest(ctx, "gone", "bob"); !errors.Is(err, ErrChangeRequestNotFound) {
			t.Errorf("err = %v", err)
		}
	})
}

// Merging an update keeps the alert's enabled state; merging a delete removes the
// alert and, since the proposal cascades away with it, reports it merged anyway.
func TestMergeUpdateAndDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("update", func(t *testing.T) {
		head := RevisionContent{Name: "old", QueryString: `a=1`, AlertType: "event", Severity: "medium"}
		head.canonicalize()
		m, pg := updateFixture(t, head, false)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeOpen, Content: eventContent("new"), Author: "alice"}, "f1", "")
		store.approve("cr1", "bob")
		pg.answer("SELECT enabled FROM alerts WHERE id = $1", fakedb.Row(false))
		pg.answer("COALESCE(prism_id::text, '') FROM alerts WHERE id", fakedb.Row("f1", ""))
		if _, err := m.MergeChangeRequest(ctx, "cr1", "bob"); err != nil {
			t.Fatal(err)
		}
		upd := pg.args("UPDATE alerts\n\t\tSET name = $2")[0]
		if upd[1] != "new" || upd[4] != false || upd[10] != "alice" {
			t.Errorf("update: name %v enabled %v by %v", upd[1], upd[4], upd[10])
		}
	})
	t.Run("delete", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeDelete, Status: ChangeOpen, Summary: "noisy", Author: "alice"}, "f1", "")
		store.approve("cr1", "bob")
		pg.answer(sqlAlertScope, fakedb.Row("f1", "", false))
		pg.handle("DELETE FROM alerts WHERE id = $1", func([]driver.Value) fakedb.PGResult {
			store.mu.Lock()
			delete(store.rows, "cr1")
			store.mu.Unlock()
			return fakedb.Affected(1)
		})
		merged, err := m.MergeChangeRequest(ctx, "cr1", "bob")
		if err != nil {
			t.Fatal(err)
		}
		if merged.Status != ChangeMerged || merged.ID != "cr1" {
			t.Errorf("merged = %+v", merged)
		}
	})
}

func TestDrafts(t *testing.T) {
	ctx := context.Background()

	t.Run("a new draft", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		pg.handle("VALUES ($1, $2, $3, 'draft'", func([]driver.Value) fakedb.PGResult {
			store.add(crSpec{ID: "d1", Kind: ChangeCreate, Status: ChangeDraft, Content: &RevisionContent{Name: "wip"}, Author: "alice"}, "f1", "")
			return fakedb.Row("d1")
		})
		// A draft may be incomplete: no query yet.
		d, err := m.SaveDraft(ctx, "", "f1", "", ChangeRequestInput{Content: &RevisionContent{Name: "wip"}}, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if d.ID != "d1" || !d.Draft() {
			t.Errorf("draft = %+v", d)
		}
		if args := pg.args("VALUES ($1, $2, $3, 'draft'")[0]; args[2] != ChangeCreate || args[0] != "f1" {
			t.Errorf("insert args = %v", args)
		}
	})
	t.Run("autosave lands on the author's draft of the alert", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "d1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeDraft, Content: eventContent("n"), Author: "alice"}, "f1", "")
		pg.answer("COALESCE(prism_id::text, '') FROM alerts WHERE id", fakedb.Row("f1", ""))
		pg.answer("AND created_by = $2 AND status = 'draft'", fakedb.Row("d1"))
		if _, err := m.SaveDraft(ctx, "", "f1", "", ChangeRequestInput{AlertID: "a1", Content: eventContent("edited")}, "alice"); err != nil {
			t.Fatal(err)
		}
		upd := pg.args("WHERE id = $1 AND status = 'draft'")
		if len(upd) != 1 || upd[0][0] != "d1" {
			t.Errorf("updates = %v", upd)
		}
		if len(pg.Matching("VALUES ($1, $2, $3, 'draft'")) != 0 {
			t.Error("autosave created a second draft of the alert")
		}
	})
	t.Run("refusals", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "open", Kind: ChangeCreate, Status: ChangeOpen, Content: eventContent("n"), Author: "alice"}, "f1", "")
		store.add(crSpec{ID: "theirs", Kind: ChangeCreate, Status: ChangeDraft, Content: eventContent("n"), Author: "bob"}, "f1", "")
		for name, tc := range map[string]struct {
			id string
			in ChangeRequestInput
		}{
			"a delete is not a draft": {"", ChangeRequestInput{Kind: ChangeDelete, AlertID: "a1", Content: eventContent("n")}},
			"no definition":           {"", ChangeRequestInput{}},
			"no longer a draft":       {"open", ChangeRequestInput{Content: eventContent("n")}},
			"someone else's draft":    {"theirs", ChangeRequestInput{Content: eventContent("n")}},
			"no scope":                {"", ChangeRequestInput{Content: eventContent("n")}},
		} {
			fractal := "f1"
			if name == "no scope" {
				fractal = ""
			}
			if _, err := m.SaveDraft(ctx, tc.id, fractal, "", tc.in, "alice"); err == nil {
				t.Errorf("%s: want a refusal", name)
			}
		}
	})
	t.Run("submit validates and bases the draft", func(t *testing.T) {
		m, pg := newTestManager(t)
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "d1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeDraft, Content: eventContent("n"), Author: "alice"}, "f1", "")
		store.add(crSpec{ID: "bad", Kind: ChangeCreate, Status: ChangeDraft, Content: &RevisionContent{Name: "wip"}, Author: "alice"}, "f1", "")
		pg.answer(sqlHeadHash, fakedb.Row("head-now"))
		if _, err := m.SubmitDraft(ctx, "d1", "alice"); err != nil {
			t.Fatal(err)
		}
		if args := pg.args("SET status = 'open', base_hash = $2")[0]; args[1] != "head-now" {
			t.Errorf("submit args = %v", args)
		}
		if _, err := m.SubmitDraft(ctx, "bad", "alice"); err == nil || !strings.Contains(err.Error(), "query string is required") {
			t.Errorf("an incomplete draft cannot be submitted: %v", err)
		}
		if _, err := m.SubmitDraft(ctx, "d1", "bob"); !errors.Is(err, ErrChangeRequestNotFound) {
			t.Errorf("another author's draft: %v", err)
		}
	})
	t.Run("list, find and delete", func(t *testing.T) {
		m, pg := newTestManager(t)
		if list, _ := m.ListDrafts(ctx, "", "", "alice"); len(list) != 0 {
			t.Error("an unscoped list is empty")
		}
		store := newCRStore(t, pg)
		store.add(crSpec{ID: "d1", Kind: ChangeCreate, Status: ChangeDraft, Content: eventContent("n"), Author: "alice"}, "f1", "")
		list, err := m.ListDrafts(ctx, "f1", "", "alice")
		if err != nil || len(list) != 1 {
			t.Errorf("list = %v %v", list, err)
		}
		if args := pg.Matching(sqlCRSelect)[0].Args; args[0] != "f1" || args[1] != "alice" {
			t.Errorf("drafts are the caller's own: %v", args)
		}
		if d, err := m.DraftForAlert(ctx, "a1", "alice"); d != nil || err != nil {
			t.Errorf("no draft = %v %v", d, err)
		}
		if err := m.DeleteDraft(ctx, "d1", "alice"); !errors.Is(err, ErrChangeRequestNotFound) {
			t.Errorf("err = %v", err)
		}
		pg.answer("DELETE FROM alert_change_requests WHERE id = $1 AND created_by = $2", fakedb.Affected(1))
		if err := m.DeleteDraft(ctx, "d1", "alice"); err != nil {
			t.Error(err)
		}
	})
}

func TestGateConfigStore(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)
	if cfg, err := m.GateConfigFor(ctx, "f1", ""); err != nil || cfg != DefaultGateConfig() {
		t.Errorf("an unconfigured scope gets the default: %+v %v", cfg, err)
	}
	pg.answer(sqlGateConfig, fakedb.PGResult{Err: errors.New("down")})
	if _, err := m.GateConfigFor(ctx, "f1", ""); err == nil {
		t.Error("a failed lookup must not read as an ungated scope")
	}

	if _, err := m.SaveGateConfig(ctx, "f1", "", GateConfig{MinApprovals: 0}, "root"); err == nil {
		t.Error("zero approvals must be refused")
	}
	if _, err := m.SaveGateConfig(ctx, "", "", GateConfig{MinApprovals: 1}, "root"); err == nil {
		t.Error("an unscoped gate must be refused")
	}
	if _, err := m.SaveGateConfig(ctx, "", "p1", GateConfig{Enabled: true, MinApprovals: 2}, "root"); err != nil {
		t.Fatal(err)
	}
	c := pg.Matching("INSERT INTO alert_gate_config")[0]
	if !strings.Contains(c.Query, "ON CONFLICT (prism_id) WHERE prism_id IS NOT NULL") || c.Args[0] != "p1" || c.Args[2] != int64(2) {
		t.Errorf("upsert = %s %v", c.Query, c.Args)
	}
}

func TestPolicyStore(t *testing.T) {
	ctx := context.Background()
	m, pg := newTestManager(t)
	if _, err := m.ReplacePolicies(ctx, "", "", nil, "root"); err == nil {
		t.Error("an unscoped rule set must be refused")
	}
	if _, err := m.ReplacePolicies(ctx, "f1", "", []Policy{{Field: "nope"}}, "root"); err == nil {
		t.Error("an invalid rule must be refused")
	}
	p := mustPolicy(t, Policy{Field: "labels", Operator: "any_matches", Value: `^attack\.`})
	pg.answer(sqlPolicies, blockingLabelPolicy())
	saved, err := m.ReplacePolicies(ctx, "f1", "", []Policy{p, p}, "root")
	if err != nil {
		t.Fatal(err)
	}
	if len(saved) != 1 {
		t.Errorf("the saved set is read back: %+v", saved)
	}
	allInTx(t, pg, "DELETE FROM alert_policies")
	ins := pg.args("INSERT INTO alert_policies")
	if len(ins) != 2 || ins[1][7] != int64(1) {
		t.Errorf("inserts = %v, want positions in order", ins)
	}

	pg.answer("INSERT INTO alert_policies", fakedb.PGResult{Err: errors.New("check violation")})
	if _, err := m.ReplacePolicies(ctx, "f1", "", []Policy{p}, "root"); err == nil || !strings.Contains(err.Error(), "saving rule 1") {
		t.Errorf("err = %v", err)
	}
	if _, rollbacks := pg.txCounts(); rollbacks != 1 {
		t.Error("a failed rule rolls the whole set back")
	}
}

// Propose and draft share import's resolution, so a document that would replace an
// alert becomes an update proposal against it.
func TestProposeAndDraftFromYAML(t *testing.T) {
	ctx := context.Background()
	doc := "name: Brute force\nqueryString: status=500\n"

	m, pg := newTestManager(t)
	store := newCRStore(t, pg)
	pg.answer("FROM alerts WHERE name = $1 AND feed_id IS NULL", fakedb.Row("a1"))
	pg.answer("COALESCE(prism_id::text, '') FROM alerts WHERE id", fakedb.Row("f1", ""))
	pg.handle("INSERT INTO alert_change_requests", func([]driver.Value) fakedb.PGResult {
		store.add(crSpec{ID: "cr1", AlertID: "a1", Kind: ChangeUpdate, Status: ChangeOpen, Content: eventContent("Brute force"), Author: "alice"}, "f1", "")
		return fakedb.Row("cr1")
	})
	if _, err := m.ProposeFromYAML(ctx, doc, "tighten", "alice", "f1", "", ""); err != nil {
		t.Fatal(err)
	}
	ins := pg.args("INSERT INTO alert_change_requests")[0]
	if ins[1] != "a1" || ins[2] != ChangeUpdate || ins[4] != "tighten" {
		t.Errorf("proposal = %v", ins)
	}

	// The author already has a draft of that alert: a draft import is refused.
	pg.answer("AND created_by = $2 AND status = 'draft'", fakedb.Row("cr1"))
	if _, err := m.DraftFromYAML(ctx, doc, "alice", "f1", "", ""); !errors.Is(err, ErrDraftExists) {
		t.Errorf("err = %v", err)
	}
	if _, err := m.ProposeFromYAML(ctx, "name: [", "s", "alice", "f1", "", ""); err == nil {
		t.Error("an unreadable document must be refused")
	}
}
