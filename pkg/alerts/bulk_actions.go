package alerts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// MaxBulkActionAlerts bounds one bulk edit, matching the batch toggle. The write is
// one transaction that has to fit the API request deadline.
const MaxBulkActionAlerts = 5000

// ActionRef names one action of any kind.
type ActionRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// ActionChange adds and removes actions on a set of alerts.
//
// Both are set operations on each alert: adding an action an alert already runs, or
// removing one it does not, leaves it alone. That is what makes a bulk edit safe to
// apply over wiring changed since the list was loaded, and safe to merge after sitting
// in review.
type ActionChange struct {
	AlertIDs []string    `json:"alert_ids"`
	Add      []ActionRef `json:"add,omitempty"`
	Remove   []ActionRef `json:"remove,omitempty"`
}

// BulkActionChange is one or more changes applied together, all or nothing. Each alert
// may appear in only one of them.
type BulkActionChange struct {
	Changes []ActionChange `json:"changes"`
}

// ErrBulkActionAlerts reports alerts a bulk edit cannot touch: missing, feed managed,
// or in another scope. They are not told apart, since IDs alone could name alerts
// anywhere.
var ErrBulkActionAlerts = errors.New("some alerts were not found in this scope or are feed managed")

// ErrInvalidActionChange marks a bulk edit the caller can fix.
var ErrInvalidActionChange = errors.New("invalid action change")

type actionChangeError string

func (e actionChangeError) Error() string        { return string(e) }
func (e actionChangeError) Is(target error) bool { return target == ErrInvalidActionChange }

func invalidChange(format string, args ...interface{}) error {
	return actionChangeError(fmt.Sprintf(format, args...))
}

// canonicalize sorts everything whose order carries no meaning, so a proposal's hash
// and stored form do not depend on how the client listed it.
func (b *BulkActionChange) canonicalize() {
	for i := range b.Changes {
		c := &b.Changes[i]
		c.AlertIDs = normalizeList(c.AlertIDs, true)
		sortRefs(c.Add)
		sortRefs(c.Remove)
	}
	sort.SliceStable(b.Changes, func(i, j int) bool {
		return firstOr(b.Changes[i].AlertIDs) < firstOr(b.Changes[j].AlertIDs)
	})
}

func firstOr(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

func sortRefs(refs []ActionRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Kind != refs[j].Kind {
			return refs[i].Kind < refs[j].Kind
		}
		return refs[i].ID < refs[j].ID
	})
}

// Hash identifies a bulk proposal for approval binding.
func (b BulkActionChange) Hash() (string, error) {
	c := BulkActionChange{Changes: make([]ActionChange, len(b.Changes))}
	for i, ch := range b.Changes {
		c.Changes[i] = ActionChange{
			AlertIDs: append([]string(nil), ch.AlertIDs...),
			Add:      append([]ActionRef(nil), ch.Add...),
			Remove:   append([]ActionRef(nil), ch.Remove...),
		}
	}
	c.canonicalize()
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// AlertIDs lists every alert the change names.
func (b BulkActionChange) AlertIDs() []string {
	var ids []string
	for _, c := range b.Changes {
		ids = append(ids, c.AlertIDs...)
	}
	return ids
}

// addedRefs lists every action the change adds, once each.
func (b BulkActionChange) addedRefs() []ActionRef {
	seen := map[ActionRef]bool{}
	var out []ActionRef
	for _, c := range b.Changes {
		for _, r := range c.Add {
			if !seen[r] {
				seen[r] = true
				out = append(out, r)
			}
		}
	}
	return out
}

func validateActionChanges(changes []ActionChange) error {
	if len(changes) == 0 {
		return invalidChange("no changes")
	}
	seen := map[string]bool{}
	for _, c := range changes {
		if len(c.AlertIDs) == 0 {
			return invalidChange("a change names no alerts")
		}
		if len(c.Add) == 0 && len(c.Remove) == 0 {
			return invalidChange("add or remove at least one action")
		}
		for _, id := range c.AlertIDs {
			if id == "" {
				return invalidChange("an alert id is empty")
			}
			if seen[id] {
				return invalidChange("alert %s appears more than once", id)
			}
			seen[id] = true
		}
		added := map[ActionRef]bool{}
		for _, r := range c.Add {
			if err := validateActionRef(r); err != nil {
				return err
			}
			added[r] = true
		}
		for _, r := range c.Remove {
			if err := validateActionRef(r); err != nil {
				return err
			}
			if added[r] {
				return invalidChange("%s action %s is both added and removed", r.Kind, r.ID)
			}
		}
	}
	if len(seen) > MaxBulkActionAlerts {
		return invalidChange("too many alerts (max %d)", MaxBulkActionAlerts)
	}
	return nil
}

func validateActionRef(r ActionRef) error {
	if _, ok := lookupActionKind(r.Kind); !ok {
		return invalidChange("unknown action kind %q", r.Kind)
	}
	if r.ID == "" {
		return invalidChange("an action id is required")
	}
	return nil
}

// AlertActionDelta is what a bulk edit does to one alert.
type AlertActionDelta struct {
	AlertID   string      `json:"alert_id"`
	AlertName string      `json:"alert_name"`
	Added     []ActionRef `json:"added,omitempty"`
	Removed   []ActionRef `json:"removed,omitempty"`
	// Blocking lists policy rules the edit would newly break on this alert.
	Blocking []Violation `json:"blocking,omitempty"`
}

// ActionTally is one action's effect across the whole edit. Skipped counts alerts that
// already had an added action, or lacked a removed one.
type ActionTally struct {
	ActionRef
	Name    string `json:"name,omitempty"`
	Added   int    `json:"added"`
	Removed int    `json:"removed"`
	Skipped int    `json:"skipped"`
}

// BulkActionPlan is a bulk edit worked out against the alerts as they are now.
type BulkActionPlan struct {
	Selected  int `json:"selected"`
	Changed   int `json:"changed"`
	Unchanged int `json:"unchanged"`
	Blocked   int `json:"blocked"`
	// Alerts lists only those the edit changes or that block it.
	Alerts  []AlertActionDelta `json:"alerts"`
	Actions []ActionTally      `json:"actions"`
	// Summary describes the edit in a sentence, e.g. for a proposal title.
	Summary string `json:"summary"`
	// Undo reverses exactly what an applied edit changed.
	Undo []ActionChange `json:"undo,omitempty"`
	// Gated means the scope reviews changes, so applying opens a proposal.
	Gated    bool           `json:"gated"`
	Proposal *ChangeRequest `json:"proposal,omitempty"`

	// before and after hold each changed alert's definition either side of the edit.
	before map[string]RevisionContent
	after  map[string]RevisionContent
}

// planActionChanges works out what a bulk edit does to each alert, and which policy
// rules it would newly break.
//
// Policies judge the edit, not the alert: a rule an alert already failed before is not
// the edit's doing, so an old alert without a description can still gain an action.
// Test outcomes cannot change with actions, so rules that read them stay deferred.
func planActionChanges(defs map[string]RevisionContent, changes []ActionChange, policies []Policy) *BulkActionPlan {
	plan := &BulkActionPlan{Alerts: []AlertActionDelta{}, Actions: []ActionTally{}, before: map[string]RevisionContent{}, after: map[string]RevisionContent{}}
	tallies := map[ActionRef]*ActionTally{}
	var order []ActionRef
	type undoKey struct{ add, remove string }
	undo := map[undoKey]*ActionChange{}
	var undoOrder []undoKey

	for _, change := range changes {
		for _, r := range append(append([]ActionRef(nil), change.Add...), change.Remove...) {
			if _, ok := tallies[r]; !ok {
				tallies[r] = &ActionTally{ActionRef: r}
				order = append(order, r)
			}
		}
		for _, id := range change.AlertIDs {
			before, ok := defs[id]
			if !ok {
				continue
			}
			plan.Selected++
			after := cloneActions(before)
			delta := AlertActionDelta{AlertID: id, AlertName: before.Name}

			for _, r := range change.Add {
				if addRef(&after, r) {
					delta.Added = append(delta.Added, r)
					tallies[r].Added++
				} else {
					tallies[r].Skipped++
				}
			}
			for _, r := range change.Remove {
				if removeRef(&after, r) {
					delta.Removed = append(delta.Removed, r)
					tallies[r].Removed++
				} else {
					tallies[r].Skipped++
				}
			}

			if len(delta.Added) == 0 && len(delta.Removed) == 0 {
				plan.Unchanged++
				continue
			}
			after.canonicalize()
			plan.Changed++
			plan.before[id] = before
			plan.after[id] = after

			delta.Blocking = newlyBlocking(policies, before, after)
			if len(delta.Blocking) > 0 {
				plan.Blocked++
			}
			plan.Alerts = append(plan.Alerts, delta)

			key := undoKey{refsKey(delta.Removed), refsKey(delta.Added)}
			u, ok := undo[key]
			if !ok {
				u = &ActionChange{Add: delta.Removed, Remove: delta.Added}
				undo[key] = u
				undoOrder = append(undoOrder, key)
			}
			u.AlertIDs = append(u.AlertIDs, id)
		}
	}

	for _, r := range order {
		plan.Actions = append(plan.Actions, *tallies[r])
	}
	for _, k := range undoOrder {
		plan.Undo = append(plan.Undo, *undo[k])
	}
	return plan
}

func cloneActions(c RevisionContent) RevisionContent {
	for _, k := range actionKinds {
		ids := k.get(&c)
		*ids = append([]string(nil), *ids...)
	}
	return c
}

// addRef adds an action to a definition, reporting whether it was not already there.
func addRef(c *RevisionContent, r ActionRef) bool {
	k, _ := lookupActionKind(r.Kind)
	ids := k.get(c)
	for _, id := range *ids {
		if id == r.ID {
			return false
		}
	}
	*ids = append(*ids, r.ID)
	return true
}

// removeRef removes an action from a definition, reporting whether it was there.
func removeRef(c *RevisionContent, r ActionRef) bool {
	k, _ := lookupActionKind(r.Kind)
	ids := k.get(c)
	for i, id := range *ids {
		if id == r.ID {
			*ids = append((*ids)[:i:i], (*ids)[i+1:]...)
			return true
		}
	}
	return false
}

func refsKey(refs []ActionRef) string {
	parts := make([]string, len(refs))
	for i, r := range refs {
		parts[i] = r.Kind + ":" + r.ID
	}
	return strings.Join(parts, ",")
}

// newlyBlocking returns the blocking violations after an edit that the alert did not
// already have before it.
func newlyBlocking(policies []Policy, before, after RevisionContent) []Violation {
	if len(policies) == 0 {
		return nil
	}
	failing := map[string]bool{}
	for _, v := range EvaluatePolicies(policies, NewPolicySubject(before, nil)).Violations {
		failing[violationKey(v)] = true
	}
	var out []Violation
	for _, v := range EvaluatePolicies(policies, NewPolicySubject(after, nil)).Violations {
		if v.Severity == PolicyBlock && !failing[violationKey(v)] {
			out = append(out, v)
		}
	}
	return out
}

func violationKey(v Violation) string {
	if v.PolicyID != "" {
		return v.PolicyID
	}
	return v.Field + "\x00" + v.Message
}

// describeActionChange renders a plan as one sentence, e.g. "Add Slack to 40 alerts,
// remove SOC leads from 1 alert".
func describeActionChange(plan *BulkActionPlan, names map[ActionRef]string) string {
	var parts []string
	for _, t := range plan.Actions {
		name := names[t.ActionRef]
		if name == "" {
			name = t.Kind + " action"
		}
		if t.Added > 0 {
			parts = append(parts, fmt.Sprintf("add %s to %s", name, pluralAlerts(t.Added)))
		}
		if t.Removed > 0 {
			parts = append(parts, fmt.Sprintf("remove %s from %s", name, pluralAlerts(t.Removed)))
		}
	}
	if len(parts) == 0 {
		return "No changes"
	}
	s := strings.Join(parts, ", ")
	return strings.ToUpper(s[:1]) + s[1:]
}

func pluralAlerts(n int) string {
	if n == 1 {
		return "1 alert"
	}
	return fmt.Sprintf("%d alerts", n)
}
