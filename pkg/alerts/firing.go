package alerts

import (
	"fmt"

	"github.com/google/uuid"

	"bifract/pkg/attack"
)

// Firing identifies one evaluation that delivered results. Every action and
// every retry of it carries the same ID, which is also the alert_executions row
// ID, so a receiver can deduplicate on it and trace it back to Bifract.
type Firing struct {
	ID string
	// GroupField is the alert's throttle field; GroupKey is its value when every
	// delivered row shares one, so receivers can group firings per host or user.
	GroupField string
	GroupKey   string
}

func newFiring(alert *Alert, rows []map[string]interface{}) Firing {
	f := Firing{ID: uuid.NewString(), GroupField: alert.ThrottleField}
	if f.GroupField == "" || len(rows) == 0 {
		return f
	}
	var key string
	for i, row := range rows {
		val, ok := row[f.GroupField]
		if !ok {
			return f
		}
		s := fmt.Sprintf("%v", val)
		if i > 0 && s != key {
			return f
		}
		key = s
	}
	f.GroupKey = key
	return f
}

// AttackRef is an ATT&CK tactic or technique with its matrix name.
type AttackRef struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

// AttackTags is an alert's attack.* labels resolved against the ATT&CK matrix.
type AttackTags struct {
	Tactics    []AttackRef `json:"tactics"`
	Techniques []AttackRef `json:"techniques"`
	Groups     []string    `json:"groups"`
	Software   []string    `json:"software"`
	Campaigns  []string    `json:"campaigns"`
}

// attackTags resolves labels, following revoked technique IDs to their
// replacement. A retired technique keeps its ID without a name.
func attackTags(labels []string) AttackTags {
	out := AttackTags{Tactics: []AttackRef{}, Techniques: []AttackRef{}, Groups: []string{}, Software: []string{}, Campaigns: []string{}}
	m, err := attack.Get()
	if err != nil {
		return out
	}
	seen := map[string]bool{}
	for _, label := range labels {
		kind, id := m.ParseLabel(label)
		if kind == attack.KindNone || seen[id] {
			continue
		}
		seen[id] = true
		switch kind {
		case attack.KindTactic:
			if t := m.Tactic(id); t != nil {
				out.Tactics = append(out.Tactics, AttackRef{ID: t.ID, Name: t.Name})
			}
		case attack.KindTechnique:
			ref := AttackRef{ID: id}
			if t := m.Technique(id); t != nil {
				ref.Name = t.Name
			}
			out.Techniques = append(out.Techniques, ref)
		case attack.KindRetired:
			out.Techniques = append(out.Techniques, AttackRef{ID: id})
		case attack.KindGroup:
			out.Groups = append(out.Groups, id)
		case attack.KindSoftware:
			out.Software = append(out.Software, id)
		case attack.KindCampaign:
			out.Campaigns = append(out.Campaigns, id)
		}
	}
	return out
}
