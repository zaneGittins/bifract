package setup

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestK8sManifestsAreValidYAML renders every wizard manifest and parses each document.
// Cheap guard for the class of mistake that only surfaces at kubectl apply time: a
// mis-indented block, an unresolved template field, or a stray tab.
func TestK8sManifestsAreValidYAML(t *testing.T) {
	data := k8sTemplateData{
		ImageTag:       "test",
		Domain:         "example.com",
		CHHostsList:    "host1,host2",
		IngestReplicas: 2,
		SpoolPVCSize:   "32Gi",
		SpoolMaxBytes:  27487790694,
		BifractRes:     ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		ArchiverRes:    ResourceProfile{"500m", "1", "512Mi", "1Gi"},

		ArchiveMaintainRes: ResourceProfile{"500m", "2", "3Gi", "5Gi"},

		// The conditional manifests below only render for a bundled ClickHouse,
		// and the lb-service needs more than one shard to exist.
		CHBundled:    true,
		CHShards:     2,
		CHStorageStr: "100Gi",
		CHEnvApp:     []EnvVar{{Name: "CLICKHOUSE_HOST", Value: "clickhouse"}},
		CHEnvIngest:  []EnvVar{{Name: "CLICKHOUSE_HOST", Value: "clickhouse"}},
		CH:           ResourceProfile{"1", "2", "4Gi", "8Gi"},
		CHKeeper:     ResourceProfile{"500m", "1", "512Mi", "1Gi"},
	}

	// Iterate both sets: moving the ClickHouseInstallation into the conditional
	// list would otherwise silently drop it from this test, and the lb-service and
	// mTLS templates were never covered here at all.
	for _, m := range append(append([]k8sManifestFile{}, k8sManifests...), k8sConditionalManifests...) {
		t.Run(m.output, func(t *testing.T) {
			out, err := renderK8sTemplate(m.template, data)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if strings.Contains(out, "<no value>") {
				t.Errorf("contains <no value> (unresolved template field)")
			}
			for i, doc := range strings.Split(out, "\n---") {
				if strings.TrimSpace(doc) == "" {
					continue
				}
				var parsed map[string]interface{}
				if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
					t.Errorf("document %d is not valid YAML: %v", i, err)
					continue
				}
				if parsed["kind"] == nil {
					t.Errorf("document %d has no kind", i)
				}
			}
		})
	}
}

// The app Deployment must stay at one replica with the Recreate strategy while the
// maxmind PVC defaults to ReadWriteOnce: a cloud block-storage volume attaches to one
// node, so a second replica never schedules and every RollingUpdate deadlocks on a
// volume the outgoing pod still holds.
func TestAppDeploymentSingleReplicaRecreate(t *testing.T) {
	out, err := renderK8sTemplate("templates/k8s/bifract-deployment.yaml.tmpl", k8sTemplateData{
		ImageTag: "test", Domain: "example.com", CHHostsList: "host1",
		BifractRes: ResourceProfile{"500m", "1", "512Mi", "1Gi"},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	var dep struct {
		Spec struct {
			Replicas int `yaml:"replicas"`
			Strategy struct {
				Type string `yaml:"type"`
			} `yaml:"strategy"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal([]byte(strings.Split(out, "\n---")[0]), &dep); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if dep.Spec.Replicas != 1 {
		t.Errorf("replicas = %d, want 1 (RWO maxmind PVC)", dep.Spec.Replicas)
	}
	if dep.Spec.Strategy.Type != "Recreate" {
		t.Errorf("strategy = %q, want Recreate (RWO maxmind PVC)", dep.Spec.Strategy.Type)
	}
}

// 9001 is the port the ClickHouse operator advertises in remote_servers, so both
// cross-shard queries and the app's direct shard lookups use it. Omitting it from either
// rule is silent: queries fall back or time out rather than erroring.
func TestNetworkPolicyAllowsClickHousePort9001(t *testing.T) {
	out, err := renderK8sTemplate("templates/k8s/network-policies.yaml.tmpl", k8sTemplateData{
		ImageTag: "test", Domain: "example.com", CHHostsList: "host1", CHBundled: true,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	for _, doc := range strings.Split(out, "\n---") {
		var np struct {
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				Ingress []struct {
					From []struct {
						PodSelector struct {
							MatchLabels map[string]string `yaml:"matchLabels"`
						} `yaml:"podSelector"`
					} `yaml:"from"`
					Ports []struct {
						Port int `yaml:"port"`
					} `yaml:"ports"`
				} `yaml:"ingress"`
			} `yaml:"spec"`
		}
		if err := yaml.Unmarshal([]byte(doc), &np); err != nil || !strings.Contains(np.Metadata.Name, "clickhouse") {
			continue
		}

		appRule, chRule := false, false
		for _, ing := range np.Spec.Ingress {
			has9001 := false
			for _, p := range ing.Ports {
				if p.Port == 9001 {
					has9001 = true
				}
			}
			for _, f := range ing.From {
				switch {
				case f.PodSelector.MatchLabels["app"] == "bifract":
					appRule = true
					if !has9001 {
						t.Errorf("%s: app -> ClickHouse rule is missing port 9001", np.Metadata.Name)
					}
				case f.PodSelector.MatchLabels["app.kubernetes.io/instance"] != "":
					chRule = true
					if !has9001 {
						t.Errorf("%s: ClickHouse -> ClickHouse rule is missing port 9001", np.Metadata.Name)
					}
				}
			}
		}
		if !appRule || !chRule {
			t.Errorf("%s: expected both an app rule and a ClickHouse peer rule (app=%v ch=%v)",
				np.Metadata.Name, appRule, chRule)
		}
		return
	}
	t.Error("no ClickHouse NetworkPolicy found in rendered output")
}

// An external ClickHouse has no in-cluster pods to select. Every policy in this
// file is Ingress-only, so reaching out to it needs no rule either; a leftover
// policy selecting a workload that does not exist is dead configuration.
func TestNetworkPolicyOmitsClickHouseWhenExternal(t *testing.T) {
	out, err := renderK8sTemplate("templates/k8s/network-policies.yaml.tmpl", k8sTemplateData{
		ImageTag: "test", Domain: "example.com",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, name := range []string{"allow-clickhouse-from-bifract", "allow-keeper-from-clickhouse"} {
		if strings.Contains(out, name) {
			t.Errorf("%s rendered for an external ClickHouse", name)
		}
	}
	// The policies that do not depend on ClickHouse must survive.
	if !strings.Contains(out, "policyTypes") {
		t.Error("no NetworkPolicy rendered at all")
	}
	for _, doc := range strings.Split(out, "\n---") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var parsed map[string]interface{}
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
			t.Errorf("document is not valid YAML: %v", err)
		}
	}
}
