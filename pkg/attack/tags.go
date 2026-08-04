package attack

import "strings"

// Kind classifies what an `attack.*` label refers to.
type Kind int

const (
	KindNone Kind = iota
	KindTechnique
	KindTactic
	KindGroup    // attack.g0016
	KindSoftware // attack.s0002
	KindCampaign // attack.c0001
	// KindRetired is a technique tag whose ID no longer exists in the shipped
	// matrix and has no replacement. Surfaced as a hygiene metric so operators
	// can fix rules that map to nothing.
	KindRetired
)

// tacticAliases maps historical tactic slugs to their current shortname.
// ATT&CK renames tactics (v19 renamed Defense Evasion to Stealth) but Sigma rule
// sets keep emitting the old slug for years. Applied only as a fallback when the
// literal slug does not resolve, so it stays correct in both directions if the
// embedded matrix is regenerated from an older or newer bundle.
var tacticAliases = map[string]string{
	"defense-evasion": "stealth",
	"stealth":         "defense-evasion",
}

// ParseLabel interprets a single alert label. Non-ATT&CK labels (product:*,
// sigma:*, cve.*, free-form user tags) return KindNone.
//
// Sigma writes tags lowercase with dots, e.g. attack.t1543.003,
// attack.privilege-escalation. Older rule sets use underscores.
func (m *Matrix) ParseLabel(label string) (Kind, string) {
	s := strings.ToLower(strings.TrimSpace(label))
	if !strings.HasPrefix(s, "attack.") {
		return KindNone, ""
	}
	s = strings.ReplaceAll(s[len("attack."):], "_", "-")
	if s == "" {
		return KindNone, ""
	}

	switch s[0] {
	case 't':
		// "ta0003" is a tactic ID; "t1543" / "t1543.003" are techniques.
		if strings.HasPrefix(s, "ta") && isDigits(s[2:]) {
			if t := m.Tactic(s); t != nil {
				return KindTactic, t.Short
			}
			return KindNone, ""
		}
		return m.resolveTechnique(s)
	case 'g':
		if isDigits(s[1:]) {
			return KindGroup, strings.ToUpper(s)
		}
	case 's':
		if isDigits(s[1:]) {
			return KindSoftware, strings.ToUpper(s)
		}
	case 'c':
		if isDigits(s[1:]) {
			return KindCampaign, strings.ToUpper(s)
		}
	}

	// Anything left is a tactic slug.
	if t := m.Tactic(s); t != nil {
		return KindTactic, t.Short
	}
	if alias, ok := tacticAliases[s]; ok {
		if t := m.Tactic(alias); t != nil {
			return KindTactic, t.Short
		}
	}
	return KindNone, ""
}

// resolveTechnique normalizes a technique slug to its canonical matrix ID,
// following revoked-by so retired IDs still count as coverage of their
// replacement rather than as nothing.
func (m *Matrix) resolveTechnique(slug string) (Kind, string) {
	base, sub, hasSub := strings.Cut(slug, ".")
	if !isDigits(base[1:]) || (hasSub && !isDigits(sub)) {
		return KindNone, ""
	}

	id := strings.ToUpper(slug)
	if t := m.byID[id]; t != nil {
		return KindTechnique, t.ID
	}
	// Follow at most a short chain: ATT&CK has re-revoked a handful of IDs, but
	// a cycle in the data must not hang the parser.
	for i := 0; i < 4; i++ {
		next, ok := m.RevokedBy[id]
		if !ok {
			break
		}
		id = next
		if t := m.byID[id]; t != nil {
			return KindTechnique, t.ID
		}
	}
	return KindRetired, strings.ToUpper(slug)
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
