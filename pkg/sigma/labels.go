package sigma

// BuildLabels renders a Sigma rule's metadata as Bifract alert labels.
//
// Rule tags (attack.persistence, attack.t1543.003, cve.*) are carried through
// verbatim and unprefixed: they are the rule's own vocabulary, and pkg/attack
// reads the attack.* ones back to build the coverage map. Bifract's own facets
// are prefixed so they cannot collide with a rule tag.
//
// This lives here rather than in each import path because the manual importer
// and the feed syncer had drifted apart, which meant the same rule produced
// different labels depending on how it entered the system.
func BuildLabels(rule *SigmaRule) []string {
	labels := make([]string, 0, len(rule.Tags)+4)

	if rule.Level != "" {
		labels = append(labels, "sigma:"+rule.Level)
	}
	if rule.Status != "" {
		labels = append(labels, "status:"+rule.Status)
	}
	labels = append(labels, rule.Tags...)
	if rule.LogSource.Product != "" {
		labels = append(labels, "product:"+rule.LogSource.Product)
	}
	if rule.LogSource.Category != "" {
		labels = append(labels, "category:"+rule.LogSource.Category)
	}

	return labels
}
