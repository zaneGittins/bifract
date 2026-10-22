package models

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

// The window sets the network state TTL, the scorer's read range and the strobe
// limit. The UI offers 1d/7d/14d; a stored bare day count is tolerated and capped
// at the longest window the state keeps.
func TestWindowDays(t *testing.T) {
	cases := map[string]int{
		"": 1, "1d": 1, "24h": 1, "7d": 7, "14d": 14,
		"3d": 3, " 5d ": 5, "10": 10, "30d": 14, "0d": 1, "d": 1, "-2d": 1, "week": 1,
	}
	for in, want := range cases {
		if got := (ModelDefinition{Window: in}).WindowDays(); got != want {
			t.Errorf("WindowDays(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestParamDefaults(t *testing.T) {
	nf := (&NetworkFieldMap{DstField: "id.resp_h"}).WithDefaults()
	if nf.SrcField != "src_ip" || nf.DstField != "id.resp_h" || nf.PortField != "dst_port" || nf.DurationField != "duration" || nf.BytesField != "orig_bytes" {
		t.Errorf("field map: %+v", nf)
	}
	var nilLC *LongConnParams
	lc := nilLC.WithDefaults()
	if lc.BaseSeconds != 3600 || lc.LowSeconds != 14400 || lc.MedSeconds != 28800 || lc.HighSeconds != 43200 || lc.ScoreThreshold != 0.5 {
		t.Errorf("long-conn defaults: %+v", lc)
	}
	if got := (&LongConnParams{HighSeconds: 100000}).WithDefaults(); got.HighSeconds != 100000 || got.BaseSeconds != 3600 {
		t.Errorf("partial long-conn: %+v", got)
	}
	var nilMP *ModifierParams
	mp := nilMP.WithDefaults()
	if mp.PrevalenceLowThreshold != 0.02 || mp.PrevalenceHighThreshold != 0.5 || mp.PrevalenceIncrease != 0.15 || mp.PrevalenceDecrease != 0.15 {
		t.Errorf("modifier defaults: %+v", mp)
	}
	if mp.FirstSeenRecentDays != 7 || mp.FirstSeenEstablishedDays != 90 || mp.FirstSeenIncrease != 0.15 || mp.FirstSeenDecrease != 0.15 || mp.ThreatIntelIncrease != 0.25 {
		t.Errorf("context modifier defaults: %+v", mp)
	}
	// Setting one field leaves the others at their defaults, and a negative
	// adjustment turns that side off.
	partial := (&ModifierParams{FirstSeenModel: "fs", PrevalenceDecrease: -1}).WithDefaults()
	if partial.PrevalenceIncrease != 0.15 || partial.FirstSeenIncrease != 0.15 || partial.PrevalenceDecrease != 0 {
		t.Errorf("partial modifiers: %+v", partial)
	}
}

func TestAlertConfigRejectsMalformedInput(t *testing.T) {
	var a AlertConfig
	if err := json.Unmarshal([]byte(`{"coverage_threshold":"high"}`), &a); err == nil {
		t.Error("json: a string threshold decoded")
	}
	if err := yaml.Unmarshal([]byte("coverage_threshold: [1]\n"), &a); err == nil {
		t.Error("yaml: a list threshold decoded")
	}
}
