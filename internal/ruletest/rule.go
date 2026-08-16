package ruletest

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"bifract/pkg/alerts"
	"bifract/pkg/normalizers"
	"bifract/pkg/parser"
	"bifract/pkg/sigma"
)

// Rule is a detection rule reduced to the BQL the alert engine would run.
type Rule struct {
	Name     string
	Kind     string // "sigma" or "bifract"
	Path     string
	BQL      string
	Pipeline *parser.PipelineNode
}

// LoadRule reads a YAML rule and lowers it to BQL exactly as the ingest side does:
// Sigma rules through sigma.Translate with the normalizer's field mapper, native
// alerts straight from their queryString. The result is parsed so a rule that could
// never run fails here rather than silently reporting "no match" for every case.
func LoadRule(path string, norm *normalizers.CompiledNormalizer) (*Rule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	content := string(data)

	r := &Rule{Path: path}

	if sigma.IsSigmaRule(content) {
		parsed, err := sigma.ParseSigmaRule(content)
		if err != nil {
			return nil, fmt.Errorf("parsing Sigma rule: %w", err)
		}
		// The same mapper the feed syncer and Sigma import use, so a rule tested
		// here resolves field names identically to one deployed from a feed.
		bql, err := sigma.Translate(parsed, sigma.BuildFieldMapper(norm))
		if err != nil {
			return nil, fmt.Errorf("translating Sigma rule to BQL: %w", err)
		}
		r.Name = parsed.Title
		r.Kind = "sigma"
		r.BQL = bql
	} else {
		var ya alerts.YAMLAlert
		if err := yaml.Unmarshal(data, &ya); err != nil {
			return nil, fmt.Errorf("parsing alert YAML: %w", err)
		}
		if strings.TrimSpace(ya.QueryString) == "" {
			return nil, fmt.Errorf("not a Sigma rule (no detection.condition) and no queryString")
		}
		r.Name = ya.Name
		if r.Name == "" {
			r.Name = path
		}
		r.Kind = "bifract"
		r.BQL = ya.QueryString
	}

	pipeline, err := parser.ParseQuery(r.BQL)
	if err != nil {
		return nil, fmt.Errorf("generated BQL is invalid: %w (query: %s)", err, r.BQL)
	}
	r.Pipeline = pipeline

	if err := checkSupported(pipeline); err != nil {
		return nil, err
	}

	return r, nil
}

// unsupportedCommands are commands whose data lives outside the scratch table: the
// provenance tables are fed by materialized views on the real `logs` table, and
// model_lookup/join read tables the tester does not populate. Against a scratch
// table they would return no rows, which a caller would read as "rule did not
// fire". Failing loudly is the only safe behavior.
var unsupportedCommands = map[string]string{
	"model_lookup": "reads analytics model tables that only exist in a live deployment",
	"join":         "reads a second table the tester does not populate",
}

func checkSupported(pipeline *parser.PipelineNode) error {
	if cmd, ok := parser.FirstSourceCommand(pipeline); ok {
		return fmt.Errorf("rule uses the source command %s(), which reads provenance tables "+
			"built by materialized views on the live logs table and cannot be tested offline", cmd.Name)
	}
	var name, reason string
	parser.ForEachCommand(pipeline, func(cmd parser.CommandNode) {
		if name == "" {
			if r, bad := unsupportedCommands[cmd.Name]; bad {
				name, reason = cmd.Name, r
			}
		}
	})
	if name != "" {
		return fmt.Errorf("rule uses %s(), which %s and cannot be tested offline", name, reason)
	}
	return nil
}
