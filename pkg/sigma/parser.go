package sigma

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// IsSigmaRule returns true if the YAML content looks like a Sigma rule
// (has detection.condition) and not a Bifract alert (has queryString).
func IsSigmaRule(content string) bool {
	var probe struct {
		QueryString string                 `yaml:"queryString"`
		Detection   map[string]interface{} `yaml:"detection"`
	}
	if err := yaml.Unmarshal([]byte(content), &probe); err != nil {
		return false
	}
	if probe.QueryString != "" {
		return false
	}
	if probe.Detection == nil {
		return false
	}
	_, hasCondition := probe.Detection["condition"]
	return hasCondition
}

// ParseSigmaRule parses a Sigma YAML rule into a SigmaRule struct.
func ParseSigmaRule(content string) (*SigmaRule, error) {
	// First pass: unmarshal standard fields
	var rule SigmaRule
	if err := yaml.Unmarshal([]byte(content), &rule); err != nil {
		return nil, fmt.Errorf("failed to parse Sigma YAML: %w", err)
	}

	// Second pass: parse detection block with custom logic
	var raw struct {
		Detection map[string]interface{} `yaml:"detection"`
	}
	if err := yaml.Unmarshal([]byte(content), &raw); err != nil {
		return nil, fmt.Errorf("failed to parse detection block: %w", err)
	}

	if raw.Detection == nil {
		return nil, fmt.Errorf("Sigma rule missing detection block")
	}

	detection, err := parseDetection(raw.Detection)
	if err != nil {
		return nil, err
	}
	rule.Detection = *detection

	if rule.Title == "" {
		return nil, fmt.Errorf("Sigma rule missing title")
	}

	return &rule, nil
}

// parseDetection extracts the condition string and parses each named selection.
func parseDetection(raw map[string]interface{}) (*Detection, error) {
	condRaw, ok := raw["condition"]
	if !ok {
		return nil, fmt.Errorf("detection block missing 'condition' key")
	}

	condition, ok := condRaw.(string)
	if !ok {
		return nil, fmt.Errorf("detection condition must be a string")
	}

	d := &Detection{
		Condition:  strings.TrimSpace(condition),
		Selections: make(map[string]SelectionGroup),
	}

	for key, val := range raw {
		if key == "condition" {
			continue
		}
		sg, err := parseSelectionGroup(val)
		if err != nil {
			return nil, fmt.Errorf("failed to parse selection '%s': %w", key, err)
		}
		d.Selections[key] = *sg
	}

	return d, nil
}

// parseSelectionGroup parses a single named selection value.
// It can be a map (field conditions) or a list of maps (alternatives).
func parseSelectionGroup(val interface{}) (*SelectionGroup, error) {
	sg := &SelectionGroup{}

	switch v := val.(type) {
	case map[string]interface{}:
		conditions, err := parseFieldConditions(v)
		if err != nil {
			return nil, err
		}
		sg.FieldConditions = conditions

	case []interface{}:
		// Could be a list of maps (alternatives) or a list of simple values
		// Check first element to determine
		if len(v) == 0 {
			return sg, nil
		}

		if _, isMap := v[0].(map[string]interface{}); isMap {
			// List of maps - alternatives
			for i, item := range v {
				m, ok := item.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("alternative %d: expected map, got %T", i, item)
				}
				conditions, err := parseFieldConditions(m)
				if err != nil {
					return nil, fmt.Errorf("alternative %d: %w", i, err)
				}
				sg.Alternatives = append(sg.Alternatives, conditions)
			}
		} else {
			// List of simple values - treat as a single field condition
			// with no field name (keyword matching). This handles:
			// selection:
			//   - 'value1'
			//   - 'value2'
			values := make([]string, 0, len(v))
			for _, item := range v {
				values = append(values, fmt.Sprintf("%v", item))
			}
			sg.FieldConditions = []FieldCondition{{
				Field:  "",
				Values: values,
			}}
		}

	default:
		// Single value - treat as keyword match
		sg.FieldConditions = []FieldCondition{{
			Field:  "",
			Values: []string{fmt.Sprintf("%v", v)},
		}}
	}

	return sg, nil
}

// parseFieldConditions parses a map of field -> value(s) with optional modifiers.
func parseFieldConditions(m map[string]interface{}) ([]FieldCondition, error) {
	var conditions []FieldCondition

	for key, val := range m {
		fc := FieldCondition{}

		// Split key on '|' to separate field name from modifiers
		parts := strings.Split(key, "|")
		fc.Field = strings.TrimSpace(parts[0])
		for _, mod := range parts[1:] {
			mod = strings.TrimSpace(mod)
			if mod != "" {
				fc.Modifiers = append(fc.Modifiers, strings.ToLower(mod))
			}
		}

		// Coerce value(s) to string slice
		switch v := val.(type) {
		case []interface{}:
			for _, item := range v {
				fc.Values = append(fc.Values, fmt.Sprintf("%v", item))
			}
		case string:
			fc.Values = []string{v}
		case int:
			fc.Values = []string{fmt.Sprintf("%d", v)}
		case int64:
			fc.Values = []string{fmt.Sprintf("%d", v)}
		case float64:
			// Handle integers stored as float64
			if v == float64(int64(v)) {
				fc.Values = []string{fmt.Sprintf("%d", int64(v))}
			} else {
				fc.Values = []string{fmt.Sprintf("%g", v)}
			}
		case bool:
			fc.Values = []string{fmt.Sprintf("%t", v)}
		case nil:
			fc.Values = nil // null value - field should be empty/absent
		default:
			fc.Values = []string{fmt.Sprintf("%v", v)}
		}

		conditions = append(conditions, fc)
	}

	return conditions, nil
}
