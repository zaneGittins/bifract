package alerts

import (
	"errors"
	"fmt"
	"strings"
)

// ErrChangeRequestNotFound distinguishes a missing proposal from a failed lookup.
var ErrChangeRequestNotFound = errors.New("change request not found")

// Change request kinds and statuses.
const (
	ChangeCreate = "create"
	ChangeUpdate = "update"
	ChangeDelete = "delete"

	// ChangeOpen is awaiting review.
	ChangeOpen = "open"
	// ChangeRejected is a reviewer asking for changes. The proposal survives: the
	// author edits it and resubmits rather than starting again.
	ChangeRejected = "changes_requested"
	// ChangeMerged has been applied to the alert.
	ChangeMerged = "merged"
	// ChangeDiscarded was withdrawn by its author.
	ChangeDiscarded = "discarded"

	ReviewApprove = "approve"
	ReviewReject  = "reject"
)

// GateConfig is a scope's review policy.
type GateConfig struct {
	Enabled      bool `json:"enabled"`
	MinApprovals int  `json:"min_approvals"`
	// AllowSelfApproval lets a fractal or tenant admin approve their own proposal.
	// It is never available to an analyst: that would make the gate self-service.
	AllowSelfApproval bool `json:"allow_self_approval"`
}

// DefaultGateConfig is the posture of a scope that has never configured the gate.
func DefaultGateConfig() GateConfig {
	return GateConfig{Enabled: false, MinApprovals: 1, AllowSelfApproval: true}
}

// Validate bounds the configuration.
func (c *GateConfig) Validate() error {
	if c.MinApprovals < 1 {
		return fmt.Errorf("at least one approval is required")
	}
	if c.MinApprovals > 10 {
		return fmt.Errorf("at most 10 approvals")
	}
	return nil
}

// Review is one reviewer's decision, recorded against the content they saw.
type Review struct {
	ID            string `json:"id"`
	Reviewer      string `json:"reviewer"`
	ReviewerLabel string `json:"reviewer_label"`
	Decision      string `json:"decision"`
	Comment       string `json:"comment"`
	ContentHash   string `json:"content_hash"`
	CreatedAt     string `json:"created_at"`
	// Stale marks a decision made against content that has since changed.
	Stale bool `json:"stale"`
}

// ChangeRequest is a proposed change to an alert, held apart from the alert so the
// live definition keeps running while it is reviewed.
type ChangeRequest struct {
	ID          string           `json:"id"`
	AlertID     string           `json:"alert_id,omitempty"`
	AlertName   string           `json:"alert_name,omitempty"`
	Kind        string           `json:"kind"`
	Status      string           `json:"status"`
	Title       string           `json:"title"`
	Summary     string           `json:"summary"`
	Content     *RevisionContent `json:"content,omitempty"`
	Tests       []AlertTest      `json:"tests,omitempty"`
	ContentHash string           `json:"content_hash"`
	BaseHash    string           `json:"base_hash,omitempty"`
	Author      string           `json:"author"`
	AuthorLabel string           `json:"author_label"`
	CreatedAt   string           `json:"created_at"`
	UpdatedAt   string           `json:"updated_at"`
	MergedAt    string           `json:"merged_at,omitempty"`
	MergedBy    string           `json:"merged_by,omitempty"`

	Reviews []Review `json:"reviews,omitempty"`
}

// Open reports whether the proposal is still live: awaiting review, or sent back for
// changes. Both are editable by their author.
func (c *ChangeRequest) Open() bool {
	return c.Status == ChangeOpen || c.Status == ChangeRejected
}

// currentDecisions maps each reviewer to their latest decision on the current content.
//
// Latest wins, and one vote per reviewer: approving twice is still one approval, and a
// reviewer who asked for changes and is later satisfied clears their own objection by
// approving. Decisions made before an edit are excluded entirely, which is what stops a
// reviewed proposal from being altered after the fact.
//
// Reviews arrive ordered by time, so a later entry simply overwrites an earlier one.
func (c *ChangeRequest) currentDecisions() map[string]string {
	decisions := make(map[string]string, len(c.Reviews))
	for _, r := range c.Reviews {
		if r.ContentHash != c.ContentHash {
			continue
		}
		// An unattributed review cannot be deduplicated against anything, so it keeps
		// its own identity rather than collapsing with other unattributed ones.
		key := r.Reviewer
		if key == "" {
			key = "review:" + r.ID
		}
		decisions[key] = r.Decision
	}
	return decisions
}

// Approvals counts the reviewers currently approving, one vote each.
func (c *ChangeRequest) Approvals() []Review {
	approving := map[string]bool{}
	for key, decision := range c.currentDecisions() {
		if decision == ReviewApprove {
			approving[key] = true
		}
	}

	var live []Review
	seen := map[string]bool{}
	for _, r := range c.Reviews {
		key := r.Reviewer
		if key == "" {
			key = "review:" + r.ID
		}
		if approving[key] && !seen[key] {
			seen[key] = true
			live = append(live, r)
		}
	}
	return live
}

// Rejected reports whether any reviewer is currently asking for changes.
func (c *ChangeRequest) Rejected() bool {
	for _, decision := range c.currentDecisions() {
		if decision == ReviewReject {
			return true
		}
	}
	return false
}

// MergeBlocker explains why a proposal cannot merge yet, or "" when it can.
//
// headHash is the alert's current definition hash, empty for a create.
func (c *ChangeRequest) MergeBlocker(cfg GateConfig, headHash string) string {
	if !c.Open() {
		return "this proposal is " + c.Status
	}
	if c.Rejected() {
		return "a reviewer asked for changes"
	}
	if n := len(c.Approvals()); n < cfg.MinApprovals {
		return fmt.Sprintf("%d of %d approvals", n, cfg.MinApprovals)
	}
	// A proposal written against an older definition would silently discard whatever
	// landed while it sat open. This is the one place the merge problem is real, and
	// the honest answer is to surface it rather than guess.
	if c.Kind != ChangeCreate && c.BaseHash != "" && headHash != "" && c.BaseHash != headHash {
		return "the alert changed since this was proposed, so it needs reproposing"
	}
	return ""
}

// CanApprove reports whether a principal may approve, and why not when they may not.
//
// Self approval is an admin privilege and configurable even for them, so an analyst can
// never wave through their own work.
func (c *ChangeRequest) CanApprove(username string, isAdmin bool, cfg GateConfig) error {
	if !c.Open() {
		return fmt.Errorf("this proposal is %s", c.Status)
	}
	if username != "" && username == c.Author {
		if !isAdmin {
			return fmt.Errorf("a proposal cannot be approved by its author")
		}
		if !cfg.AllowSelfApproval {
			return fmt.Errorf("self approval is turned off for this scope")
		}
	}
	return nil
}

// ChangeRequestInput is a proposal as submitted.
type ChangeRequestInput struct {
	AlertID string           `json:"alert_id,omitempty"`
	Kind    string           `json:"kind"`
	Title   string           `json:"title"`
	Summary string           `json:"summary"`
	Content *RevisionContent `json:"content,omitempty"`
	Tests   []AlertTest      `json:"tests,omitempty"`
}

// Validate checks a proposal is coherent before it is stored.
func (in *ChangeRequestInput) Validate() error {
	switch in.Kind {
	case ChangeCreate, ChangeUpdate, ChangeDelete:
	default:
		return fmt.Errorf("kind must be create, update or delete")
	}

	if in.Kind != ChangeCreate && in.AlertID == "" {
		return fmt.Errorf("a %s proposal needs an alert", in.Kind)
	}
	if in.Kind == ChangeCreate && in.AlertID != "" {
		return fmt.Errorf("a create proposal has no alert yet")
	}

	if in.Kind == ChangeDelete {
		if strings.TrimSpace(in.Summary) == "" {
			return fmt.Errorf("say why the alert should be deleted")
		}
		return nil
	}

	if in.Content == nil {
		return fmt.Errorf("a %s proposal needs a definition", in.Kind)
	}
	if strings.TrimSpace(in.Content.Name) == "" {
		return fmt.Errorf("alert name is required")
	}
	if strings.TrimSpace(in.Content.QueryString) == "" {
		return fmt.Errorf("query string is required")
	}
	return ValidateTests(in.Tests)
}
