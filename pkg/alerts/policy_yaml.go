package alerts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// PolicyDocument is the on-disk form of a rule set, so a policy can be shared between
// fractals and instances or kept in a repo. It is a transport, not an authoring
// language: the editor writes rows, and this is what those rows serialize to.
type PolicyDocument struct {
	Name        string       `yaml:"name" json:"name"`
	Description string       `yaml:"description,omitempty" json:"description,omitempty"`
	Rules       []PolicyRule `yaml:"rules" json:"rules"`
}

// PolicyRule is one rule in a document.
type PolicyRule struct {
	Field    string `yaml:"field" json:"field"`
	Operator string `yaml:"operator" json:"operator"`
	Value    string `yaml:"value,omitempty" json:"value,omitempty"`
	Message  string `yaml:"message" json:"message"`
	Severity string `yaml:"severity,omitempty" json:"severity,omitempty"`
	Disabled bool   `yaml:"disabled,omitempty" json:"disabled,omitempty"`
}

// ParsePolicyDocument reads a rule set and validates every rule, so a bad file is
// rejected as a whole rather than half-imported.
func ParsePolicyDocument(content string) ([]Policy, error) {
	var doc PolicyDocument
	if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
		return nil, fmt.Errorf("parsing policy YAML: %w", err)
	}
	if len(doc.Rules) == 0 {
		return nil, fmt.Errorf("the document has no rules")
	}

	policies := make([]Policy, 0, len(doc.Rules))
	for i, rule := range doc.Rules {
		severity := strings.TrimSpace(rule.Severity)
		if severity == "" {
			severity = PolicyWarn
		}
		p := Policy{
			Field:    strings.TrimSpace(rule.Field),
			Operator: strings.TrimSpace(rule.Operator),
			Value:    rule.Value,
			Message:  strings.TrimSpace(rule.Message),
			Severity: severity,
			Enabled:  !rule.Disabled,
			Position: i,
		}
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("rule %d (%s %s): %w", i+1, rule.Field, rule.Operator, err)
		}
		policies = append(policies, p)
	}

	if err := ValidatePolicies(policies); err != nil {
		return nil, err
	}
	return policies, nil
}

// RenderPolicyDocument serializes a rule set for export.
func RenderPolicyDocument(name string, policies []Policy) (string, error) {
	doc := PolicyDocument{Name: name, Rules: make([]PolicyRule, 0, len(policies))}
	for _, p := range policies {
		doc.Rules = append(doc.Rules, PolicyRule{
			Field:    p.Field,
			Operator: p.Operator,
			Value:    p.Value,
			Message:  p.Message,
			Severity: p.Severity,
			Disabled: !p.Enabled,
		})
	}

	out, err := yaml.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("rendering policy YAML: %w", err)
	}
	return string(out), nil
}
