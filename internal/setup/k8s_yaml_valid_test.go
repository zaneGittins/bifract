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
	}

	for _, m := range k8sManifests {
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
		ImageTag: "test", Domain: "example.com", CHHostsList: "host1",
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
