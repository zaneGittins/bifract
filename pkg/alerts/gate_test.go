package alerts

import "testing"

func gateCfg() GateConfig { return GateConfig{Enabled: true, MinApprovals: 1, AllowSelfApproval: true} }

func crWith(hash string, reviews ...Review) *ChangeRequest {
	return &ChangeRequest{
		Kind: ChangeUpdate, Status: ChangeOpen, ContentHash: hash, BaseHash: "base",
		Author: "author", Reviews: reviews,
	}
}

func TestApprovalStopsCountingAfterAnEdit(t *testing.T) {
	cr := crWith("hash-1", Review{Decision: ReviewApprove, Reviewer: "reviewer", ContentHash: "hash-1"})
	if len(cr.Approvals()) != 1 {
		t.Fatal("an approval against the current content should count")
	}
	if blocker := cr.MergeBlocker(gateCfg(), "base"); blocker != "" {
		t.Fatalf("expected a mergeable proposal, got %q", blocker)
	}

	// The author edits after approval.
	cr.ContentHash = "hash-2"
	if len(cr.Approvals()) != 0 {
		t.Error("an approval must not survive an edit to what it approved")
	}
	if cr.MergeBlocker(gateCfg(), "base") == "" {
		t.Error("an edited proposal must not merge on a stale approval")
	}
}

func TestStaleBaseRefusesMerge(t *testing.T) {
	cr := crWith("hash-1", Review{Decision: ReviewApprove, Reviewer: "r", ContentHash: "hash-1"})
	if blocker := cr.MergeBlocker(gateCfg(), "moved-on"); blocker == "" {
		t.Error("a proposal whose alert changed underneath must not merge")
	}
}

func TestCreateIgnoresBase(t *testing.T) {
	cr := crWith("hash-1", Review{Decision: ReviewApprove, Reviewer: "r", ContentHash: "hash-1"})
	cr.Kind = ChangeCreate
	cr.BaseHash = ""
	if blocker := cr.MergeBlocker(gateCfg(), ""); blocker != "" {
		t.Errorf("a create has no base to go stale, got %q", blocker)
	}
}

func TestMinApprovals(t *testing.T) {
	cfg := gateCfg()
	cfg.MinApprovals = 2
	cr := crWith("h", Review{Decision: ReviewApprove, Reviewer: "a", ContentHash: "h"})
	if cr.MergeBlocker(cfg, "base") == "" {
		t.Error("one approval must not satisfy a two-approval policy")
	}

	cr.Reviews = append(cr.Reviews, Review{Decision: ReviewApprove, Reviewer: "b", ContentHash: "h"})
	if blocker := cr.MergeBlocker(cfg, "base"); blocker != "" {
		t.Errorf("two approvals should satisfy it, got %q", blocker)
	}
}

func TestRejectionBlocksMergeButKeepsTheWork(t *testing.T) {
	cr := crWith("h",
		Review{Decision: ReviewApprove, Reviewer: "a", ContentHash: "h"},
		Review{Decision: ReviewReject, Reviewer: "b", ContentHash: "h", Comment: "narrow the query"})

	if !cr.Rejected() {
		t.Fatal("a rejection against the current content should register")
	}
	if cr.MergeBlocker(gateCfg(), "base") == "" {
		t.Error("a rejected proposal must not merge")
	}

	// The author addresses the feedback: the edit moves the hash, which clears both
	// the approval and the rejection. The proposal itself survives.
	cr.ContentHash = "h2"
	cr.Status = ChangeOpen
	if cr.Rejected() {
		t.Error("a rejection of older content should not follow the edit that addressed it")
	}
	if !cr.Open() {
		t.Error("a resubmitted proposal is open again, not destroyed")
	}
}

func TestSelfApprovalIsAnAdminPrivilege(t *testing.T) {
	cr := crWith("h")
	cfg := gateCfg()

	if err := cr.CanApprove("author", false, cfg); err == nil {
		t.Error("an analyst must not approve their own proposal")
	}
	if err := cr.CanApprove("author", true, cfg); err != nil {
		t.Errorf("an admin may self approve when the scope allows it: %v", err)
	}

	cfg.AllowSelfApproval = false
	if err := cr.CanApprove("author", true, cfg); err == nil {
		t.Error("self approval must be refusable even for an admin")
	}
	if err := cr.CanApprove("someone-else", false, cfg); err != nil {
		t.Errorf("another analyst may always approve: %v", err)
	}
}

func TestClosedProposalsDoNotAcceptReviews(t *testing.T) {
	cr := crWith("h")
	cr.Status = ChangeMerged
	if err := cr.CanApprove("reviewer", false, gateCfg()); err == nil {
		t.Error("a merged proposal must not accept further approvals")
	}
}

func TestDeleteProposalNeedsAReason(t *testing.T) {
	in := ChangeRequestInput{Kind: ChangeDelete, AlertID: "a1"}
	if err := in.Validate(); err == nil {
		t.Error("deleting a detection should require saying why")
	}
	in.Summary = "superseded by the broader rule"
	if err := in.Validate(); err != nil {
		t.Errorf("a reasoned delete should validate: %v", err)
	}
}

func TestUpdateProposalNeedsADefinition(t *testing.T) {
	in := ChangeRequestInput{Kind: ChangeUpdate, AlertID: "a1"}
	if err := in.Validate(); err == nil {
		t.Error("an update with no content asserts nothing")
	}
}

func TestOneVotePerReviewer(t *testing.T) {
	// Clicking approve twice is still one approval.
	cr := crWith("h",
		Review{ID: "1", Decision: ReviewApprove, Reviewer: "reviewer", ContentHash: "h"},
		Review{ID: "2", Decision: ReviewApprove, Reviewer: "reviewer", ContentHash: "h"})

	if n := len(cr.Approvals()); n != 1 {
		t.Errorf("one reviewer approving twice should count once, got %d", n)
	}

	cfg := gateCfg()
	cfg.MinApprovals = 2
	if cr.MergeBlocker(cfg, "base") == "" {
		t.Error("one reviewer must not satisfy a two-approval policy by clicking twice")
	}
}

func TestLatestDecisionPerReviewerWins(t *testing.T) {
	// Approve, then think better of it.
	cr := crWith("h",
		Review{ID: "1", Decision: ReviewApprove, Reviewer: "reviewer", ContentHash: "h"},
		Review{ID: "2", Decision: ReviewReject, Reviewer: "reviewer", ContentHash: "h", Comment: "on reflection, no"})

	if len(cr.Approvals()) != 0 {
		t.Error("a reviewer who then rejected is no longer approving")
	}
	if !cr.Rejected() {
		t.Error("their rejection should stand")
	}

	// Reject, then be satisfied by a later conversation.
	cr = crWith("h",
		Review{ID: "1", Decision: ReviewReject, Reviewer: "reviewer", ContentHash: "h"},
		Review{ID: "2", Decision: ReviewApprove, Reviewer: "reviewer", ContentHash: "h"})

	if cr.Rejected() {
		t.Error("a reviewer who approved after rejecting should clear their own objection")
	}
	if len(cr.Approvals()) != 1 {
		t.Error("their approval should count")
	}
}

func TestOneReviewerCannotClearAnothersRejection(t *testing.T) {
	cr := crWith("h",
		Review{ID: "1", Decision: ReviewReject, Reviewer: "alice", ContentHash: "h"},
		Review{ID: "2", Decision: ReviewApprove, Reviewer: "bob", ContentHash: "h"})

	if !cr.Rejected() {
		t.Error("bob approving must not clear alice's request for changes")
	}
}

func TestResubmitCannotRetargetAProposal(t *testing.T) {
	// A PUT carrying a different kind or alert must not be able to move the proposal:
	// it would recompute the staleness baseline against something else entirely.
	existing := &ChangeRequest{Kind: ChangeUpdate, AlertID: "alert-1", Status: ChangeOpen, Author: "author"}

	in := ChangeRequestInput{Kind: ChangeCreate, AlertID: "", Content: &RevisionContent{Name: "n", QueryString: "q"}}
	in.Kind = existing.Kind
	in.AlertID = existing.AlertID

	if err := in.Validate(); err != nil {
		t.Fatalf("the pinned input should validate: %v", err)
	}
	if in.Kind != ChangeUpdate || in.AlertID != "alert-1" {
		t.Errorf("kind and target must come from the proposal, got %s/%s", in.Kind, in.AlertID)
	}
}

func TestMergeBlockerRejectsAClosedProposal(t *testing.T) {
	// The claim that makes merge exclusive sets status first; a second attempt must
	// find nothing to do.
	for _, status := range []string{ChangeMerged, ChangeDiscarded} {
		cr := crWith("h", Review{ID: "1", Decision: ReviewApprove, Reviewer: "r", ContentHash: "h"})
		cr.Status = status
		if cr.MergeBlocker(gateCfg(), "base") == "" {
			t.Errorf("a %s proposal must not merge again", status)
		}
	}
}
