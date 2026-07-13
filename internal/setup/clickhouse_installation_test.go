package setup

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestClickHouseInstallationUsersConfig guards the CHI users config for the official
// ClickHouse operator (clickhouse.com/v1alpha1). Two footguns broke prod here:
//
//  1. Altinity's "user/key" slash-path convention: a key like
//     "default/access_management" becomes a bogus username with no auth method
//     (Code 36 -> 347, "must specify an authentication type").
//  2. Setting `access_management` on the `default` user: this operator defines
//     `default` with a SQL `grants:` block (GRANT ALL, which already includes ACCESS
//     MANAGEMENT), and ClickHouse rejects mixing access-control settings with grants
//     (Code 36 -> 347, "Any other access control settings can't be specified with grants").
//
// So the CHI must NOT define a `default` user override at all. The template renders
// must stay valid YAML and free of both footguns.
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

	// A literal slash-path username must never appear (footgun 1).
	if strings.Contains(out, "default/access_management") {
		t.Fatalf("slash-path key 'default/access_management' present -- crashes the official ClickHouse operator")
	}

	// Both YAML documents (KeeperCluster, ClickHouseCluster) must still parse, and
	// extraUsersConfig must not carry a users override at all: setting anything on the
	// `default` user (e.g. access_management) collides with its grants (footgun 2).
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
	euc := nestedMap(t, chCluster, "spec", "settings", "extraUsersConfig")
	if _, hasUsers := euc["users"]; hasUsers {
		t.Fatalf("extraUsersConfig.users must not be set (the operator owns the default user via grants): %#v", euc)
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
