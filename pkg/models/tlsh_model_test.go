package models

import (
	"regexp"
	"strings"
	"testing"

	"bifract/pkg/tlsh"
)

func tlshTestDef() ModelDefinition {
	return ModelDefinition{KeyFields: []string{"tlsh"}}
}

func TestTLSHModelDDL(t *testing.T) {
	tableSQL, mvSQL, err := GenerateDDL(tlshTestDef(), ModelTypeTLSH, "`t`", "`mv`", "fractal-a")
	if err != nil {
		t.Fatal(err)
	}

	mustContain(t, tableSQL, "digest      String", "digest column")
	mustContain(t, tableSQL, "ORDER BY (fractal_id, digest)", "sorted by fractal then digest")
	mustContain(t, tableSQL, "AggregatingMergeTree", "aggregating engine")

	mustContain(t, mvSQL, "fractal_id = 'fractal-a'", "mv scoped to owner")
	mustContain(t, mvSQL, "AS digest", "digest projection")
	mustContain(t, mvSQL, "match(", "digest shape guard")
	mustContain(t, mvSQL, "GROUP BY fractal_id, digest, first_seen, last_seen", "group by digest")
}

// The MV's guard is what keeps unusable digests out of the index, so it has to
// admit exactly what the matcher can parse. A guard looser than the parser lets
// empty and malformed values in, where they compare at distance 0 and match every
// needle; a guard tighter than the parser silently drops real digests.
func TestTLSHGuardMatchesParser(t *testing.T) {
	clause := tlshDigestGuard("x")
	// Recover the regex literal from `match(x, '<pattern>')`.
	open := strings.Index(clause, "'")
	closeIdx := strings.LastIndex(clause, "'")
	if open < 0 || closeIdx <= open {
		t.Fatalf("cannot extract pattern from %q", clause)
	}
	pattern := clause[open+1 : closeIdx]

	// ClickHouse's match() and Go's regexp are both RE2, so the same pattern
	// evaluates identically here and in the database.
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("guard pattern does not compile: %v", err)
	}

	good := "8aa32957b3e520f9e1b28a3884954a49d775f8361b219fef03b442961f237e48d3ab31"
	candidates := []string{
		good,
		strings.ToUpper(good),
		"T1" + good,
		"t1" + good,
		"T1" + strings.ToUpper(good),
		"",
		"   ",
		good[:69],
		good[:68],
		good + "a",
		good + "ab",
		"T1",
		"T2" + good,
		"T" + good,
		strings.Repeat("z", 70),
		good[:68] + "zz",
		strings.Repeat("a", 64), // sha256
		strings.Repeat("a", 32), // md5
		"null",
		"-",
	}

	for _, c := range candidates {
		sqlAccepts := re.MatchString(c)
		goAccepts := tlsh.Valid(c)
		if sqlAccepts != goAccepts {
			t.Errorf("guard disagrees with parser on %q: sql=%v go=%v", c, sqlAccepts, goAccepts)
		}
	}
}

func TestTLSHModelRequiresExactlyOneKeyField(t *testing.T) {
	cases := []struct {
		name string
		def  ModelDefinition
		ok   bool
	}{
		{"one field", ModelDefinition{KeyFields: []string{"tlsh"}}, true},
		{"none", ModelDefinition{}, false},
		{"empty string", ModelDefinition{KeyFields: []string{"  "}}, false},
		{"composite", ModelDefinition{KeyFields: []string{"tlsh", "image"}}, false},
	}
	for _, c := range cases {
		err := validateDefinitionShape(ModelTypeTLSH, c.def)
		if c.ok && err != nil {
			t.Errorf("%s: unexpected error %v", c.name, err)
		}
		if !c.ok && err == nil {
			t.Errorf("%s: expected rejection", c.name)
		}
	}
}

// The backfill must reproduce the MV exactly, guard included, or seeding history
// would admit digests the live stream rejects.
func TestTLSHBackfillCarriesGuard(t *testing.T) {
	sql, err := BuildBackfillInsert(tlshTestDef(), ModelTypeTLSH, "`t`", "logs_distributed",
		"timestamp >= 'a' AND timestamp < 'b'", "fractal-a")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, sql, "match(", "backfill carries the digest guard")
	mustContain(t, sql, "fractal_id = 'fractal-a'", "backfill scoped to owner")
	mustContain(t, sql, "logs_distributed", "backfill reads the given source table")
}

func TestTLSHModelTypeIsEnumerated(t *testing.T) {
	var mt ModelType
	found := false
	for _, v := range mt.EnumValues() {
		if v == string(ModelTypeTLSH) {
			found = true
		}
	}
	if !found {
		t.Error("tlsh missing from ModelType.EnumValues, so the API will not advertise it")
	}
	if ModelTypeTLSH.IsScheduled() || ModelTypeTLSH.IsNetwork() {
		t.Error("tlsh is a streaming index, not a scheduled or network model")
	}
}

// tlsh() filters logs with `<field> IN (<indexed digests>)`, so an indexed digest
// must be the verbatim value stored in that field. An extraction indexes a derived
// value (lowercased, if asked) that appears in no log field, so the filter would
// match nothing while looking correct.
func TestTLSHModelRejectsExtractions(t *testing.T) {
	def := ModelDefinition{
		KeyFields:   []string{"digest_out"},
		Extractions: []ExtractionStep{{FromField: "hash", Pattern: `TLSH=(\w+)`, OutputField: "digest_out"}},
	}
	if err := validateDefinitionShape(ModelTypeTLSH, def); err == nil {
		t.Fatal("expected a tlsh model with an extraction to be rejected")
	}
	// Other types keep using extractions.
	if err := validateDefinitionShape(ModelTypeFirstSeen, def); err != nil {
		t.Fatalf("extractions must remain valid for first_seen: %v", err)
	}
}
