package models

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Definitions written before the rename carry confidence_threshold; dropping it
// would turn a rarity alert into one that fires on every rare value.
func TestAlertConfigReadsLegacyConfidenceThreshold(t *testing.T) {
	var j AlertConfig
	if err := json.Unmarshal([]byte(`{"severity":"high","confidence_threshold":0.85,"percent_threshold":5}`), &j); err != nil {
		t.Fatal(err)
	}
	if j.CoverageThreshold != 0.85 || j.PercentThreshold != 5 || j.Severity != "high" {
		t.Errorf("json legacy decode = %+v", j)
	}
	var y AlertConfig
	if err := yaml.Unmarshal([]byte("severity: low\nconfidence_threshold: 0.7\n"), &y); err != nil {
		t.Fatal(err)
	}
	if y.CoverageThreshold != 0.7 || y.Severity != "low" {
		t.Errorf("yaml legacy decode = %+v", y)
	}
	var n AlertConfig
	if err := json.Unmarshal([]byte(`{"coverage_threshold":0.9,"confidence_threshold":0.1}`), &n); err != nil {
		t.Fatal(err)
	}
	if n.CoverageThreshold != 0.9 {
		t.Errorf("coverage_threshold must win over the legacy key, got %v", n.CoverageThreshold)
	}
	out, _ := json.Marshal(n)
	if strings.Contains(string(out), "confidence") {
		t.Errorf("encoding must use the new name only: %s", out)
	}
}
