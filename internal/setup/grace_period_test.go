package setup

import (
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// minShutdownGrace covers the server's 30s HTTP drain, the ingest queue's 15s
// drain deadline and its 5s minimum attempt. Docker's 10s and Kubernetes' 30s
// defaults would kill the process mid-drain and lose acked logs.
const minShutdownGrace = 60 * time.Second

func TestComposeGivesShutdownTimeToDrain(t *testing.T) {
	rendered, err := RenderDockerCompose(composeSetupConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	dev, err := os.ReadFile("../../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"rendered": rendered, "dev": string(dev)} {
		var doc struct {
			Services map[string]struct {
				StopGracePeriod string `yaml:"stop_grace_period"`
			}
		}
		if err := yaml.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, svc := range []string{"bifract", "bifract-ingest", "bifract-archiver"} {
			s, ok := doc.Services[svc]
			if !ok {
				continue
			}
			d, err := time.ParseDuration(s.StopGracePeriod)
			if err != nil || d < minShutdownGrace {
				t.Errorf("%s compose: %s stop_grace_period = %q, want >= %v", name, svc, s.StopGracePeriod, minShutdownGrace)
			}
		}
		if _, ok := doc.Services["bifract-ingest"]; !ok {
			t.Errorf("%s compose: no bifract-ingest service", name)
		}
	}
}

func TestK8sGivesShutdownTimeToDrain(t *testing.T) {
	data := k8sTemplateData{
		ImageTag: "test", Domain: "example.com", CHHostsList: "host1", IngestReplicas: 1,
		SpoolPVCSize: "32Gi", SpoolMaxBytes: 1 << 30,
		BifractRes:  ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		ArchiverRes: ResourceProfile{"500m", "1", "512Mi", "1Gi"},
	}
	for _, tmpl := range []string{"bifract-deployment.yaml.tmpl", "bifract-ingest-deployment.yaml.tmpl"} {
		out, err := renderK8sTemplate("templates/k8s/"+tmpl, data)
		if err != nil {
			t.Fatalf("%s: %v", tmpl, err)
		}
		dec := yaml.NewDecoder(strings.NewReader(out))
		found := false
		for {
			var doc struct {
				Kind string
				Spec struct {
					Template struct {
						Spec struct {
							TerminationGracePeriodSeconds int `yaml:"terminationGracePeriodSeconds"`
						}
					}
				}
			}
			if dec.Decode(&doc) != nil {
				break
			}
			if doc.Kind != "Deployment" && doc.Kind != "StatefulSet" {
				continue
			}
			found = true
			if got := time.Duration(doc.Spec.Template.Spec.TerminationGracePeriodSeconds) * time.Second; got < minShutdownGrace {
				t.Errorf("%s: terminationGracePeriodSeconds = %v, want >= %v", tmpl, got, minShutdownGrace)
			}
		}
		if !found {
			t.Errorf("%s: no Deployment or StatefulSet rendered", tmpl)
		}
	}
}
