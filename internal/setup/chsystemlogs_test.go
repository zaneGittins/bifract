package setup

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// chSystemLogSettings is the docker XML fragment reduced to the values that matter:
// the logger level and every system.*_log TTL.
type chSystemLogSettings struct {
	loggerLevel string
	ttls        map[string]string
}

// parseCHSystemLogsXML walks the fragment generically so a table added to the XML
// but not to the k8s manifest is caught rather than silently ignored.
func parseCHSystemLogsXML(t *testing.T, doc []byte) chSystemLogSettings {
	t.Helper()

	var root struct {
		Logger struct {
			Level string `xml:"level"`
		} `xml:"logger"`
		Children []struct {
			XMLName xml.Name
			TTL     string `xml:"ttl"`
		} `xml:",any"`
	}
	if err := xml.Unmarshal(doc, &root); err != nil {
		t.Fatalf("parse clickhouse-system-logs.xml: %v", err)
	}

	got := chSystemLogSettings{loggerLevel: root.Logger.Level, ttls: map[string]string{}}
	for _, c := range root.Children {
		if c.XMLName.Local == "logger" {
			continue
		}
		if c.TTL == "" {
			t.Errorf("XML element <%s> has no <ttl>", c.XMLName.Local)
			continue
		}
		got.ttls[c.XMLName.Local] = c.TTL
	}
	return got
}

// TestClickHouseSystemLogConfigMatchesK8s pins the docker fragment and the
// ClickHouseInstallation's extraConfig to the same values. The two deployment paths
// express this config in different formats (a mounted config.d XML vs the operator's
// settings map), so nothing but a test stops one from being tuned and the other left
// with ClickHouse's unbounded defaults.
func TestClickHouseSystemLogConfigMatchesK8s(t *testing.T) {
	raw, err := TemplateFS.ReadFile("templates/clickhouse-system-logs.xml")
	if err != nil {
		t.Fatalf("read embedded fragment: %v", err)
	}
	want := parseCHSystemLogsXML(t, raw)

	if want.loggerLevel != "information" {
		t.Errorf("docker logger level = %q, want %q: ClickHouse's default (trace) is what fills system.text_log", want.loggerLevel, "information")
	}
	if len(want.ttls) == 0 {
		t.Fatal("docker fragment declares no system log TTLs")
	}

	rendered, err := renderTemplate("templates/k8s/clickhouse-installation.yaml.tmpl", k8sTemplateData{
		CHShards:     1,
		CHStorageStr: "100Gi",
		CH:           ResourceProfile{"1", "2", "4Gi", "8Gi"},
		CHKeeper:     ResourceProfile{"500m", "1", "512Mi", "1Gi"},
	})
	if err != nil {
		t.Fatalf("render ClickHouseInstallation: %v", err)
	}

	// The CHI is the second document in the file; the first is the KeeperCluster.
	var extra map[string]any
	for _, doc := range strings.Split(rendered, "\n---\n") {
		var parsed struct {
			Spec struct {
				Settings struct {
					ExtraConfig map[string]any `yaml:"extraConfig"`
				} `yaml:"settings"`
			} `yaml:"spec"`
		}
		if err := yaml.Unmarshal([]byte(doc), &parsed); err != nil {
			t.Fatalf("parse rendered manifest: %v", err)
		}
		if parsed.Spec.Settings.ExtraConfig != nil {
			extra = parsed.Spec.Settings.ExtraConfig
			break
		}
	}
	if extra == nil {
		t.Fatal("rendered ClickHouseInstallation has no spec.settings.extraConfig")
	}

	logger, _ := extra["logger"].(map[string]any)
	if got, _ := logger["level"].(string); got != want.loggerLevel {
		t.Errorf("k8s logger level = %q, docker = %q", got, want.loggerLevel)
	}

	for table, ttl := range want.ttls {
		entry, ok := extra[table].(map[string]any)
		if !ok {
			t.Errorf("k8s extraConfig is missing %q (docker sets ttl %q)", table, ttl)
			continue
		}
		if got, _ := entry["ttl"].(string); got != ttl {
			t.Errorf("k8s %s.ttl = %q, docker = %q", table, got, ttl)
		}
	}
}

// TestComposeBindMountsAreWritten asserts every ./-relative bind mount in the
// rendered compose resolves to something WriteAllFiles actually put on disk.
//
// A mount whose source is missing does not fail loudly: Docker creates a directory
// at that path, and the container silently gets an empty mount instead of the file
// it needed. That is one mistake away any time a template gains a mount, so this
// checks all of them rather than any single file.
func TestComposeBindMountsAreWritten(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.InstallDir = dir
	cfg.Domain = "example.com"
	cfg.PostgresPassword = "pgpassword123"
	cfg.ClickHousePassword = "chpassword123"

	if err := WriteAllFiles(cfg); err != nil {
		t.Fatalf("WriteAllFiles: %v", err)
	}

	compose, err := RenderDockerCompose(cfg)
	if err != nil {
		t.Fatalf("render docker-compose: %v", err)
	}

	var checked int
	for _, line := range strings.Split(compose, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "- ./") {
			continue
		}
		src, _, ok := strings.Cut(strings.TrimPrefix(line, "- "), ":")
		if !ok {
			continue
		}
		checked++
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(src))); err != nil {
			t.Errorf("compose bind-mounts %s but the installer never writes it: %v", src, err)
		}
	}
	if checked == 0 {
		t.Fatal("found no ./-relative bind mounts to check; the scan is broken")
	}
}

// TestUpgradeWritesSameStaticFilesAsInstall pins the two paths together. RunUpgrade
// renders its own config rather than calling WriteAllFiles, so a file added to only
// one of them reaches fresh installs and never reaches existing ones.
func TestUpgradeWritesSameStaticFilesAsInstall(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.InstallDir = dir
	cfg.Domain = "example.com"
	cfg.PostgresPassword = "pgpassword123"
	cfg.ClickHousePassword = "chpassword123"

	if err := WriteAllFiles(cfg); err != nil {
		t.Fatalf("WriteAllFiles: %v", err)
	}
	for _, f := range staticInstallFiles {
		if _, err := os.Stat(filepath.Join(dir, filepath.Join(f.dest...))); err != nil {
			t.Errorf("WriteAllFiles did not write %s: %v", filepath.Join(f.dest...), err)
		}
	}
}
