package ruletest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"bifract/pkg/normalizers"
	"bifract/pkg/ruleeval"
)

// queryWindow is how far either side of now the tester looks. Entries are stamped
// with an ingest timestamp of now, so this only has to be wide enough to absorb
// clock skew and a slow run.
const queryWindow = time.Hour

// CaseResult is the outcome of one assertion.
type CaseResult struct {
	Spec         string        `json:"spec"`
	Rule         string        `json:"rule"`
	Case         string        `json:"case"`
	Expect       Expectation   `json:"expect"`
	Matched      bool          `json:"matched"`
	Rows         int           `json:"rows"`
	Each         bool          `json:"each,omitempty"`
	Units        int           `json:"units,omitempty"`
	UnitsMatched int           `json:"units_matched,omitempty"`
	Passed       bool          `json:"passed"`
	Reason       string        `json:"reason,omitempty"`
	Duration     time.Duration `json:"duration_ns"`

	// Populated for --explain.
	BQL    string              `json:"bql,omitempty"`
	SQL    string              `json:"sql,omitempty"`
	Fields []map[string]string `json:"normalized_fields,omitempty"`
}

// SpecResult groups the case results for one spec file, or records why the spec
// could not run at all.
type SpecResult struct {
	Path    string       `json:"spec"`
	Rule    string       `json:"rule,omitempty"`
	BQL     string       `json:"bql,omitempty"`
	Error   string       `json:"error,omitempty"`
	Results []CaseResult `json:"cases,omitempty"`
}

// Summary is the whole run.
type Summary struct {
	Specs    []SpecResult  `json:"specs"`
	Passed   int           `json:"passed"`
	Failed   int           `json:"failed"`
	Errored  int           `json:"errored"`
	Duration time.Duration `json:"duration_ns"`
}

// OK reports whether the run should exit zero.
func (s *Summary) OK() bool { return s.Failed == 0 && s.Errored == 0 }

// preparedCase is a case with its logs already normalized and scoped to a fractal.
// A case is normally one unit per log, each judged independently. With
// `together: true` all its logs collapse into a single unit.
type preparedCase struct {
	spec  *Spec
	def   *Case
	units []caseUnit
}

// caseUnit is one independent evaluation. Aliased to the shared engine's unit so the
// tester and the alert editor cannot drift apart on what a unit means.
type caseUnit = ruleeval.Unit

// SpecLoad is a spec that either loaded or failed to. Carrying the failure through
// rather than aborting lets one broken file be reported alongside everything that
// did run.
type SpecLoad struct {
	Path string
	Spec *Spec
	Err  error
}

// Run executes every spec and returns the summary. The backend must already be
// connected; the caller owns its lifecycle.
func Run(ctx context.Context, b *Backend, loads []SpecLoad, opts Options) (*Summary, error) {
	started := time.Now()
	summary := &Summary{}

	var prepared []preparedCase
	rules := make(map[string]*Rule) // spec path -> rule

	// Phase 1: load and normalize everything before touching ClickHouse, so a bad
	// spec fails fast rather than after a container start and a schema init.
	for _, load := range loads {
		path := load.Path
		sr := SpecResult{Path: path}

		if load.Err != nil {
			sr.Error = load.Err.Error()
			summary.Specs = append(summary.Specs, sr)
			summary.Errored++
			continue
		}
		spec := load.Spec

		norm, err := resolveNormalizer(spec, opts)
		if err != nil {
			sr.Error = err.Error()
			summary.Specs = append(summary.Specs, sr)
			summary.Errored++
			continue
		}

		rule, err := LoadRule(spec.Rule, norm)
		if err != nil {
			sr.Error = err.Error()
			summary.Specs = append(summary.Specs, sr)
			summary.Errored++
			continue
		}
		sr.Rule = rule.Name
		sr.BQL = rule.BQL
		rules[path] = rule

		specCopy := spec
		failed := false
		for i := range spec.Cases {
			c := &spec.Cases[i]
			units, err := buildUnits(c, norm)
			if err != nil {
				sr.Error = fmt.Sprintf("case %q: %v", c.Name, err)
				failed = true
				break
			}
			prepared = append(prepared, preparedCase{spec: specCopy, def: c, units: units})
		}
		if failed {
			summary.Specs = append(summary.Specs, sr)
			summary.Errored++
			// Drop any cases already queued for this spec.
			prepared = dropSpec(prepared, specCopy)
			delete(rules, path)
			continue
		}

		summary.Specs = append(summary.Specs, sr)
	}

	if len(prepared) == 0 {
		summary.Duration = time.Since(started)
		return summary, nil
	}

	// The time window is fixed once for the whole run and shared by the visibility
	// check and every case query, so the check actually proves what the queries need.
	// It is UTC because the translator formats bounds with the location it is given
	// and ClickHouse reads naive literals in server time, which is UTC.
	window := ruleeval.NewWindow(time.Now())

	// Phase 2: one batch insert for the whole run.
	var units []ruleeval.Unit
	for _, pc := range prepared {
		units = append(units, pc.units...)
	}
	if err := b.Scratch.Insert(ctx, units); err != nil {
		return nil, err
	}
	if err := b.Scratch.WaitVisible(ctx, units, window); err != nil {
		return nil, err
	}

	// Phase 3: one query per case, scoped to that case's synthetic fractal.
	byPath := make(map[string]*SpecResult, len(summary.Specs))
	for i := range summary.Specs {
		byPath[summary.Specs[i].Path] = &summary.Specs[i]
	}

	for _, pc := range prepared {
		rule := rules[pc.spec.Path]
		res := evaluate(ctx, b, rule, pc, window, opts)

		if sr := byPath[pc.spec.Path]; sr != nil {
			sr.Results = append(sr.Results, res)
		}
		if res.Passed {
			summary.Passed++
		} else {
			summary.Failed++
		}
	}

	summary.Duration = time.Since(started)
	return summary, nil
}

// evaluate runs the case's unit(s) and compares the outcome to its expectation.
// A multi-unit case (each: true) passes only if every unit meets the expectation.
func evaluate(ctx context.Context, b *Backend, rule *Rule, pc preparedCase, window ruleeval.Window, opts Options) CaseResult {
	start := time.Now()
	res := CaseResult{
		Spec:   pc.spec.Path,
		Rule:   rule.Name,
		Case:   pc.def.Name,
		Expect: pc.def.Expectation(),
		Units:  len(pc.units),
	}

	var failures []string
	for _, u := range pc.units {
		rows, sql, err := b.Scratch.Evaluate(ctx, rule.Pipeline, u, window)
		if err != nil {
			res.Reason = err.Error()
			if sql != "" {
				res.BQL, res.SQL = rule.BQL, sql
			}
			res.Duration = time.Since(start)
			return res
		}

		res.Rows += rows
		matched := rows > 0
		if matched {
			res.UnitsMatched++
		}

		ok, reason := ruleeval.Verdict(pc.def.Expectation(), matched, rows, pc.def.Count)
		if !ok {
			if u.Label != "" {
				reason = u.Label + ": " + reason
			}
			failures = append(failures, reason)
		}
		if opts.Explain && !ok {
			res.BQL, res.SQL = rule.BQL, sql
			for _, e := range u.Entries {
				res.Fields = append(res.Fields, e.Fields)
			}
		}
	}

	res.Matched = res.UnitsMatched > 0
	res.Passed = len(failures) == 0
	if !res.Passed {
		// Cap the detail so a case with many logs reports usefully rather than flooding.
		const maxShown = 5
		shown := failures
		if len(shown) > maxShown {
			shown = shown[:maxShown]
		}
		res.Reason = strings.Join(shown, "; ")
		if len(failures) > maxShown {
			res.Reason += fmt.Sprintf(" (and %d more)", len(failures)-maxShown)
		}
	}
	res.Duration = time.Since(start)
	return res
}

// buildUnits normalizes a case's logs through the real ingest path and groups them
// into independently evaluated units, each in its own synthetic fractal so units
// cannot see each other's data.
//
// One unit per log by default, so every log must meet the expectation on its own.
// `together: true` collapses them into a single unit for threshold rules.
func buildUnits(c *Case, norm *normalizers.CompiledNormalizer) ([]caseUnit, error) {
	logs := c.ResolvedLogs()

	if c.Together {
		u, err := ruleeval.NewUnit("", logs, norm)
		if err != nil {
			return nil, err
		}
		return []caseUnit{u}, nil
	}

	units := make([]caseUnit, 0, len(logs))
	for i, obj := range logs {
		// Only label when there is more than one, so a single-log case reads cleanly.
		label := ""
		if len(logs) > 1 {
			label = fmt.Sprintf("log %d", i+1)
		}
		u, err := ruleeval.NewUnit(label, []map[string]interface{}{obj}, norm)
		if err != nil {
			return nil, err
		}
		units = append(units, u)
	}
	return units, nil
}

// resolveNormalizer prefers the spec's own normalizer, falling back to a
// command-line default so an ad hoc run does not need a spec file.
func resolveNormalizer(spec *Spec, opts Options) (*normalizers.CompiledNormalizer, error) {
	path := spec.Normalizer
	if path == "" {
		path = opts.NormalizerPath
	}
	if path == "" {
		return nil, nil
	}
	return LoadNormalizer(path)
}

func dropSpec(prepared []preparedCase, spec *Spec) []preparedCase {
	out := prepared[:0]
	for _, pc := range prepared {
		if pc.spec != spec {
			out = append(out, pc)
		}
	}
	return out
}
