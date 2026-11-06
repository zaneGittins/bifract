package models

import (
	"reflect"
	"testing"

	"bifract/pkg/parser"
)

// A rarity model with no group scores each value across every log it reads, so
// its state rows share one constant partition and only the value must be present.
func TestRarityWithoutGroupStoresOnePartition(t *testing.T) {
	cases := []struct {
		def   ModelDefinition
		guard string
	}{
		{ModelDefinition{ValueKey: "rmm_name"}, "AND fields.`rmm_name`::String != ''\nGROUP BY"},
		{ModelDefinition{ValueKey: "tld", Extractions: []ExtractionStep{{FromField: "norm_log", Pattern: "([a-z]+)$", OutputField: "tld"}}},
			"WHERE tld != ''\nGROUP BY"},
	}
	builders := []func(ModelDefinition, ModelType, string, string, string, string) (string, error){BuildBackfillInsert, BuildStateInsert}
	for _, c := range cases {
		for _, build := range builders {
			sql, err := build(c.def, ModelTypeRarity, "`m`", "logs", "", "f1")
			if err != nil {
				t.Fatal(err)
			}
			mustContain(t, sql, "'' AS partition_val", "constant partition")
			mustContain(t, sql, c.guard, "only the value is required present")
			mustContain(t, sql, "GROUP BY fractal_id, partition_val, value_val", "group by")
			mustNotContain(t, sql, "``", "no empty field reference")
		}
	}
}

// A value the source computes reaches the model through its projection.
func TestRarityWithoutGroupKeysOnAComputedValue(t *testing.T) {
	def, err := resolveSourceFor(ModelDefinition{ValueKey: "k", SourceBQL: `concat(image, user, as=k)`}, parser.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := BuildBackfillInsert(def, ModelTypeRarity, "`m`", "logs", "", "f1")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, sql, "'' AS partition_val", "constant partition")
	mustContain(t, sql, "k AS value_val", "computed value")
	mustContain(t, sql, "k != ''", "computed value guard")
}

func TestLookupKeys(t *testing.T) {
	cases := []struct {
		name string
		mt   ModelType
		def  ModelDefinition
		want []string
	}{
		{"rarity grouped", ModelTypeRarity, ModelDefinition{PartitionKey: "user", ValueKey: "rmm_name"}, []string{"user", "rmm_name"}},
		{"rarity global", ModelTypeRarity, ModelDefinition{ValueKey: "rmm_name"}, []string{"rmm_name"}},
		{"first_seen", ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user", "rmm_name"}}, []string{"user", "rmm_name"}},
		{"volume", ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"host"}}, []string{"host"}},
		{"beacon", ModelTypeBeacon, ModelDefinition{}, []string{"src_ip", "dst_ip", "dst_port"}},
		{"long connection, mapped fields", ModelTypeLongConnection, ModelDefinition{Network: &NetworkFieldMap{SrcField: "a", DstField: "b", PortField: "c"}}, []string{"a", "b", "c"}},
		{"tlsh has no lookup", ModelTypeTLSH, ModelDefinition{KeyFields: []string{"tlsh"}}, nil},
	}
	for _, c := range cases {
		if got := c.def.LookupKeys(c.mt); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestGenerateQueryRarityWithoutGroup(t *testing.T) {
	q := GenerateQuery("rare_rmm", ModelDefinition{ValueKey: "rmm_name", Alert: &AlertConfig{CoverageThreshold: 0.9}}, ModelTypeRarity)
	mustContain(t, q, `| modelLookup(model="rare_rmm", key=[rmm_name])`, "single key")
	mustContain(t, q, "| coverage > 0.90", "coverage still applies")
}

// The query paths hand modelLookup() the keys a model was built on, so it can
// refuse a lookup naming a different number of fields.
func TestParserInfosCarryLookupKeys(t *testing.T) {
	infos := map[string]ModelInfo{"m": newModelInfo("id1", ModelTypeRarity, ModelDefinition{ValueKey: "v", MinSample: 3}, "f1", "model_id1", false)}
	got := ParserInfos(infos)["m"]
	if !reflect.DeepEqual(got.Keys, []string{"v"}) || got.ModelType != "rarity" || got.MinSample != 3 || got.TableName != "model_id1" {
		t.Errorf("unexpected parser info %+v", got)
	}
}

func TestFindingsBasisNamesWhatARarityTrendCounts(t *testing.T) {
	alert := &AlertConfig{CoverageThreshold: 0.9}
	grouped := &Model{ModelType: ModelTypeRarity, Definition: ModelDefinition{PartitionKey: "h", ValueKey: "v", Alert: alert}}
	mustContain(t, findingsBasis(grouped), "new value pairs per day", "grouped")
	global := &Model{ModelType: ModelTypeRarity, Definition: ModelDefinition{ValueKey: "v", Alert: alert}}
	mustContain(t, findingsBasis(global), "new values per day", "ungrouped")
}
