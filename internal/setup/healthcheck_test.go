package setup

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// On first start the Postgres and ClickHouse images run a temporary server for
// their init scripts: Postgres on its Unix socket only, ClickHouse on 127.0.0.1
// only. A health check that reaches that server passes before the real one
// listens, so Bifract starts, is refused, and a first install fails. Each check
// must reach the server the way other containers do.
func TestHealthChecksWaitForTheRealServer(t *testing.T) {
	rendered, err := RenderDockerCompose(composeSetupConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	dev, err := os.ReadFile("../../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"postgres": "pg_isready -h 127.0.0.1", "clickhouse": "hostname -i"}
	for name, body := range map[string]string{"rendered": rendered, "dev": string(dev)} {
		var doc struct {
			Services map[string]struct {
				Healthcheck struct {
					Test []string `yaml:"test"`
				} `yaml:"healthcheck"`
			}
		}
		if err := yaml.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for svc, check := range want {
			test := strings.Join(doc.Services[svc].Healthcheck.Test, " ")
			if !strings.Contains(test, check) {
				t.Errorf("%s compose: %s health check %q does not contain %q", name, svc, test, check)
			}
		}
	}

	probe, err := renderK8sTemplate("templates/k8s/postgres-statefulset.yaml.tmpl", k8sTemplateData{PostgresRes: ResourceProfile{"1", "1", "1Gi", "1Gi"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(probe, `"pg_isready", "-h", "127.0.0.1"`) {
		t.Errorf("k8s postgres readiness probe does not check over TCP:\n%s", probe)
	}
}
