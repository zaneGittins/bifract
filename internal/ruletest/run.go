package ruletest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"bifract/pkg/ingest"
	"bifract/pkg/normalizers"
	"bifract/pkg/parser"
	"bifract/pkg/storage"
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

// caseUnit is one independent evaluation: a set of logs in their own fractal, and
// the query run against just them.
type caseUnit struct {
	fractalID string
	label     string
	entries   []storage.LogEntry
}

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
	now := time.Now().UTC()
	window := timeWindow{start: now.Add(-queryWindow), end: now.Add(queryWindow)}

	// Phase 2: one batch insert for the whole run.
	var batch []storage.LogEntry
	for _, pc := range prepared {
		for _, u := range pc.units {
			batch = append(batch, u.entries...)
		}
	}
	if err := b.Client.InsertLogsInto(ctx, b.Scratch, batch); err != nil {
		return nil, fmt.Errorf("inserting test logs: %w", err)
	}
	if err := waitForVisibility(ctx, b, prepared, window); err != nil {
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
func evaluate(ctx context.Context, b *Backend, rule *Rule, pc preparedCase, window timeWindow, opts Options) CaseResult {
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
		sql, err := parser.TranslateToSQL(rule.Pipeline, queryOptsFor(b, u, window))
		if err != nil {
			res.Reason = fmt.Sprintf("translating BQL to SQL: %v", err)
			res.Duration = time.Since(start)
			return res
		}

		rows, err := b.Client.Query(ctx, sql)
		if err != nil {
			res.Reason = fmt.Sprintf("query failed: %v", err)
			res.BQL, res.SQL = rule.BQL, sql
			res.Duration = time.Since(start)
			return res
		}

		res.Rows += len(rows)
		matched := len(rows) > 0
		if matched {
			res.UnitsMatched++
		}

		ok, reason := verdict(pc.def, matched, len(rows))
		if !ok {
			if u.label != "" {
				reason = u.label + ": " + reason
			}
			failures = append(failures, reason)
		}
		if opts.Explain && !ok {
			res.BQL, res.SQL = rule.BQL, sql
			for _, e := range u.entries {
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

// queryOptsFor mirrors alerts.Engine.buildQueryOpts: alerts filter on
// ingest_timestamp, so a log whose own timestamp is months old is still evaluated
// when it arrives.
func queryOptsFor(b *Backend, u caseUnit, window timeWindow) parser.QueryOptions {
	return parser.QueryOptions{
		StartTime:          window.start,
		EndTime:            window.end,
		MaxRows:            10000,
		UseIngestTimestamp: true,
		TableName:          b.Scratch,
		FractalID:          u.fractalID,
	}
}

// verdict compares the observed result to the case's expectation.
func verdict(c *Case, matched bool, rows int) (bool, string) {
	switch c.Expectation() {
	case ExpectMatch:
		if !matched {
			return false, "expected the rule to trigger, but it returned no rows"
		}
		if c.Count != nil && rows != *c.Count {
			return false, fmt.Sprintf("expected %d result rows, got %d", *c.Count, rows)
		}
		return true, ""
	default:
		if matched {
			return false, fmt.Sprintf("expected the rule not to trigger, but it returned %d row(s)", rows)
		}
		return true, ""
	}
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
		fractalID := uuid.NewString()
		entries, err := normalizeLogs(logs, norm, fractalID)
		if err != nil {
			return nil, err
		}
		return []caseUnit{{fractalID: fractalID, entries: entries}}, nil
	}

	units := make([]caseUnit, 0, len(logs))
	for i, obj := range logs {
		fractalID := uuid.NewString()
		entries, err := normalizeLogs([]map[string]interface{}{obj}, norm, fractalID)
		if err != nil {
			return nil, err
		}
		u := caseUnit{fractalID: fractalID, entries: entries}
		// Only label when there is more than one, so a single-log case reads cleanly.
		if len(logs) > 1 {
			u.label = fmt.Sprintf("log %d", i+1)
		}
		units = append(units, u)
	}
	return units, nil
}

func normalizeLogs(logs []map[string]interface{}, norm *normalizers.CompiledNormalizer, fractalID string) ([]storage.LogEntry, error) {
	entries := make([]storage.LogEntry, 0, len(logs))
	for i, obj := range logs {
		entry, err := ingest.BuildLogEntry(obj, norm, nil)
		if err != nil {
			return nil, fmt.Errorf("log %d: %w", i+1, err)
		}
		entry.FractalID = fractalID
		entries = append(entries, entry)
	}
	return entries, nil
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

// timeWindow is the ingest_timestamp range every query in a run shares.
type timeWindow struct {
	start time.Time
	end   time.Time
}

// waitForVisibility blocks until every case's logs are readable through the exact
// window and fractal scoping its rule query will use.
//
// This is a correctness guard, not just a wait. If the harness cannot see its own
// rows -- clock skew between this machine and the ClickHouse server, async inserts
// still in flight, a fractal scoping mistake -- then every rule returns no rows, and
// each "expect: no_match" case passes for entirely the wrong reason. Silent vacuous
// passes are the worst outcome for a detection gate, so an invisible row is a hard
// error rather than a verdict.
func waitForVisibility(ctx context.Context, b *Backend, prepared []preparedCase, w timeWindow) error {
	want := make(map[string]int, len(prepared))
	for _, pc := range prepared {
		for _, u := range pc.units {
			want[u.fractalID] += len(u.entries)
		}
	}

	query := fmt.Sprintf(
		"SELECT fractal_id, count() AS c FROM %s.%s WHERE ingest_timestamp >= '%s' AND ingest_timestamp <= '%s' GROUP BY fractal_id",
		logsDatabase, b.Scratch,
		w.start.Format("2006-01-02 15:04:05"),
		w.end.Format("2006-01-02 15:04:05"))

	deadline := time.Now().Add(30 * time.Second)
	var missing []string

	for {
		rows, err := b.Client.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("verifying test logs are queryable: %w", err)
		}

		seen := make(map[string]int, len(rows))
		for _, r := range rows {
			if id, ok := r["fractal_id"].(string); ok {
				seen[id] = int(toUint64(r["c"]))
			}
		}

		missing = nil
		for _, pc := range prepared {
			for _, u := range pc.units {
				if seen[u.fractalID] < len(u.entries) {
					missing = append(missing, fmt.Sprintf("%s (%d/%d rows)",
						pc.def.Name, seen[u.fractalID], len(u.entries)))
				}
			}
		}
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("test logs were inserted but are not visible to the query window "+
		"%s..%s UTC, so every rule would report no match. Cases affected: %s. "+
		"Check for clock skew between this machine and the ClickHouse server",
		w.start.Format(time.RFC3339), w.end.Format(time.RFC3339), strings.Join(missing, "; "))
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
