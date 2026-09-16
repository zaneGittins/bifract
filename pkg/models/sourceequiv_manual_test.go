//go:build modelequiv

// Manual harness comparing the two ways a model's source filter reaches SQL:
// ddl.go's filterConditionToSQL, and the BQL querygen.go writes compiled by the
// real translator. It measures the blast radius of moving models onto the
// translator before any migration is written.
//
// Needs the dev stack (ClickHouse on localhost:9000 with log rows):
//
//	go test -tags modelequiv ./pkg/models/ -run TestSourceFilterEquivalence -v
//
// BIFRACT_MODEL_DUMP=<file> adds real definitions, as a JSON array of
// {"name":..., "model_type":..., "definition":{...}}, so an install can measure
// its own models rather than these synthetic ones.
package models

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

const (
	equivStart = "2020-01-01 00:00:00.000"
	equivEnd   = "2040-01-01 00:00:00.000"
)

// equivFractal is the fractal the comparison runs in, discovered at run time: a
// hard-coded id matches no rows on a real install, and a comparison over an empty
// row set reports every case as equivalent while proving nothing.
var equivFractal string

type equivCase struct {
	name string
	def  ModelDefinition
	// differs marks a case the two builders are known to disagree on. Asserted
	// rather than tolerated: a known divergence that quietly goes away means the
	// harness stopped measuring what it was written for.
	differs bool
}

// syntheticCases covers every FilterCondition.Op that parse.go can produce, so a
// divergence in any operator is caught without needing a model that uses it.
func syntheticCases() []equivCase {
	f := func(field, op, value string) []FilterCondition {
		return []FilterCondition{{Field: field, Op: op, Value: value}}
	}
	return []equivCase{
		{"empty", ModelDefinition{}, false},
		{"eq", ModelDefinition{Filter: f("event_id", "=", "1")}, false},
		{"eq-miss", ModelDefinition{Filter: f("event_id", "=", "no-such-value")}, false},
		{"neq", ModelDefinition{Filter: f("event_id", "!=", "1")}, false},
		{"wildcard", ModelDefinition{Filter: f("user", "=", "*")}, false},
		{"wildcard-neg", ModelDefinition{Filter: f("user", "!=", "*")}, false},
		{"regex", ModelDefinition{Filter: f("image", "~", "powershell")}, false},
		{"regex-anchored", ModelDefinition{Filter: f("image", "~", `\.exe$`)}, false},
		{"regex-neg", ModelDefinition{Filter: f("image", "!~", "svchost")}, false},
		{"cidr", ModelDefinition{Filter: f("src_ip", "cidr", "10.0.0.0/8")}, false},
		{"cidr-neg", ModelDefinition{Filter: f("src_ip", "!cidr", "10.0.0.0/8")}, false},
		{"cidr-v6", ModelDefinition{Filter: f("src_ip", "cidr", "2001:db8::/32")}, false},
		// Predicted divergence: the translator validates a range with net.ParseCIDR,
		// ddl.go does not, so a stored model holding this compiles today and will
		// not after the move. Present so the harness is known to detect a real
		// difference and not only ever report agreement.
		{"cidr-malformed", ModelDefinition{Filter: f("src_ip", "cidr", "10.0.0.0")}, true},
		{"multi-and", ModelDefinition{Filter: []FilterCondition{
			{Field: "event_id", Op: "=", Value: "1"},
			{Field: "image", Op: "~", Value: "powershell"},
		}}, false},
		{"quote-in-value", ModelDefinition{Filter: f("user", "=", `say "hi"`)}, false},
		{"backslash-in-value", ModelDefinition{Filter: f("user", "=", `CORP\rpatel`)}, false},
	}
}

func dumpCases(t *testing.T) []equivCase {
	path := os.Getenv("BIFRACT_MODEL_DUMP")
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var rows []struct {
		Name       string          `json:"name"`
		ModelType  string          `json:"model_type"`
		Definition ModelDefinition `json:"definition"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := make([]equivCase, 0, len(rows))
	for _, r := range rows {
		out = append(out, equivCase{name: "dump/" + r.Name + "(" + r.ModelType + ")", def: r.Definition})
	}
	return out
}

// oldSourceSQL is the row set ddl.go selects: the same predicates it ANDs into
// every model's scan, in the same order.
func oldSourceSQL(def ModelDefinition) string {
	preds := []string{
		fmt.Sprintf("fractal_id = '%s'", equivFractal),
		fmt.Sprintf("timestamp >= '%s' AND timestamp < '%s'", equivStart, equivEnd),
	}
	for _, fc := range def.Filter {
		preds = append(preds, filterConditionToSQL(fc))
	}
	return "SELECT count() AS n FROM logs WHERE " + strings.Join(preds, " AND ")
}

// newSourceSQL is the row set the translator selects from the BQL querygen writes
// for the same definition. Extractions are dropped: they render as commands the
// translator composes differently, which the phase-4 work addresses; this measures
// the filters, where the two builders genuinely duplicate each other.
func newSourceSQL(def ModelDefinition) (string, error) {
	filtersOnly := ModelDefinition{Filter: def.Filter}
	bql := GenerateSourceQuery(filtersOnly)
	if strings.TrimSpace(bql) == "" {
		bql = "*"
	}
	pipeline, err := parser.ParseQuery(bql)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", bql, err)
	}
	start, _ := time.Parse("2006-01-02 15:04:05.000", equivStart)
	end, _ := time.Parse("2006-01-02 15:04:05.000", equivEnd)
	res, err := parser.TranslateToSQLWithOrder(pipeline, parser.QueryOptions{
		StartTime: start,
		EndTime:   end,
		MaxRows:   1 << 30,
		FractalID: equivFractal,
	})
	if err != nil {
		return "", fmt.Errorf("translate %q: %w", bql, err)
	}
	return "SELECT count() AS n FROM (" + res.SQL + ")", nil
}

func TestSourceFilterEquivalence(t *testing.T) {
	client, err := storage.NewClickHouseClient(storage.ClientOptions{
		Conn: storage.ConnOptions{
			Addrs:    storage.HostAddrs([]string{"localhost:9000"}, 9000),
			Database: "logs",
			User:     "default",
			Password: "bifract",
		},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	count := func(sql string) (uint64, error) {
		rows, err := client.Query(context.Background(), sql)
		if err != nil {
			return 0, err
		}
		if len(rows) == 0 {
			return 0, fmt.Errorf("no rows")
		}
		switch v := rows[0]["n"].(type) {
		case uint64:
			return v, nil
		case int64:
			return uint64(v), nil
		}
		return 0, fmt.Errorf("unexpected count type %T", rows[0]["n"])
	}

	// Pick the busiest fractal and refuse to run without rows: every case would
	// report 0 == 0 and the harness would pass having compared nothing.
	rows, err := client.Query(context.Background(),
		"SELECT fractal_id AS f, count() AS n FROM logs GROUP BY f ORDER BY n DESC LIMIT 1")
	if err != nil {
		t.Fatalf("discover fractal: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("no rows in logs: the comparison would be vacuous")
	}
	equivFractal = fmt.Sprintf("%v", rows[0]["f"])
	baseline, err := count(oldSourceSQL(ModelDefinition{}))
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	if baseline == 0 {
		t.Fatalf("fractal %s has no rows in the window: the comparison would be vacuous", equivFractal)
	}
	t.Logf("comparing in fractal %s over %d rows", equivFractal, baseline)

	cases := append(syntheticCases(), dumpCases(t)...)
	var same, differ, failed int
	for _, c := range cases {
		oldSQL := oldSourceSQL(c.def)
		newSQL, err := newSourceSQL(c.def)
		if err != nil {
			if c.differs {
				t.Logf("%-20s diverges as expected: %v", c.name, err)
				differ++
			} else {
				t.Errorf("%-20s BUILD: %v", c.name, err)
				failed++
			}
			continue
		}
		if c.differs {
			t.Errorf("%-20s was expected to diverge and no longer does; the harness has stopped measuring it", c.name)
		}
		oldN, oldErr := count(oldSQL)
		newN, newErr := count(newSQL)
		switch {
		case oldErr != nil && newErr != nil:
			t.Logf("%-20s both refuse: old=%v new=%v", c.name, oldErr, newErr)
			same++
		case oldErr != nil:
			t.Errorf("%-20s old refuses but new runs (%d rows): %v", c.name, newN, oldErr)
			differ++
		case newErr != nil:
			t.Errorf("%-20s new refuses but old runs (%d rows): %v", c.name, oldN, newErr)
			differ++
		case oldN != newN:
			t.Errorf("%-20s ROW SETS DIFFER old=%d new=%d\n  old: %s\n  new: %s", c.name, oldN, newN, oldSQL, newSQL)
			differ++
		case oldN == 0:
			t.Logf("%-20s same, but both matched no rows: proves nothing", c.name)
			same++
		default:
			t.Logf("%-20s same (%d rows)", c.name, oldN)
			same++
		}
	}
	t.Logf("equivalence: %d same, %d differ, %d failed to build, of %d cases", same, differ, failed, len(cases))
}
