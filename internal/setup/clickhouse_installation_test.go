package setup

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestClickHouseInstallationUsersConfig guards against re-introducing the Altinity
// "user/key" slash-path convention into the official ClickHouse operator CHI
// (clickhouse.com/v1alpha1). That operator writes extraUsersConfig as literal nested
// YAML, so a key like "default/access_management" becomes a bogus username with no
// authentication method and crashes ClickHouse on startup (Code 36/347). The value
// MUST be nested: users.default.access_management.
func TestClickHouseInstallationUsersConfig(t *testing.T) {
	out, err := renderK8sTemplate("templates/k8s/clickhouse-installation.yaml.tmpl", k8sTemplateData{
		CHShards:          2,
		CHStorageStr:      "100Gi",
		CHMaxServerMemory: 1 << 30,
		CHMaxBytesToMerge: 1 << 30,
		CH:                ResourceProfile{CPURequest: "1", MemRequest: "2Gi", CPULimit: "2", MemLimit: "4Gi"},
		CHKeeper:          ResourceProfile{CPURequest: "1", MemRequest: "1Gi", CPULimit: "1", MemLimit: "1Gi"},
	})
	if err != nil {
		t.Fatalf("render CHI template: %v", err)
	}

	// Both YAML documents (KeeperCluster, ClickHouseCluster) must parse.
	dec := yaml.NewDecoder(strings.NewReader(out))
	var chCluster map[string]any
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			break
		}
		if doc["kind"] == "ClickHouseCluster" {
			chCluster = doc
		}
	}
	if chCluster == nil {
		t.Fatalf("no ClickHouseCluster document rendered:\n%s", out)
	}

	// Walk spec.settings.extraUsersConfig.users.default.access_management.
	users := nestedMap(t, chCluster, "spec", "settings", "extraUsersConfig", "users")
	def, ok := users["default"].(map[string]any)
	if !ok {
		t.Fatalf("extraUsersConfig.users.default is not a nested map (Altinity slash-path leaked back in?): users=%#v", users)
	}
	if def["access_management"] == nil {
		t.Fatalf("users.default.access_management not set: default=%#v", def)
	}
	// Explicitly reject a literal "default/access_management" username key.
	if _, bad := users["default/access_management"]; bad {
		t.Fatalf("literal slash-path username 'default/access_management' present -- this crashes the official ClickHouse operator")
	}
}

func nestedMap(t *testing.T, m map[string]any, keys ...string) map[string]any {
	t.Helper()
	cur := m
	for _, k := range keys {
		next, ok := cur[k].(map[string]any)
		if !ok {
			t.Fatalf("path %v: key %q is missing or not a map", keys, k)
		}
		cur = next
	}
	return cur
}
