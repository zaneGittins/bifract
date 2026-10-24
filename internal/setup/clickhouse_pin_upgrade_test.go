package setup

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// An upgrade is the only way an existing install moves to a newer ClickHouse, so it
// must replace an older pin rather than keep whatever the install was rendered with.
const olderClickHouseTag = "26.6.2.81-alpine"

// pinnedClickHouseTag reads the tag the templates render. ruletest's
// TestClickHousePinsMatch holds every template to the same one.
func pinnedClickHouseTag(t *testing.T) string {
	t.Helper()
	body, err := TemplateFS.ReadFile("templates/docker-compose.yml.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`clickhouse/clickhouse-server:([\w.-]+)`).FindSubmatch(body)
	if m == nil {
		t.Fatal("no ClickHouse pin in templates/docker-compose.yml.tmpl")
	}
	return string(m[1])
}

func rewritePin(t *testing.T, path string) {
	t.Helper()
	body, pinned := readFile(t, path), pinnedClickHouseTag(t)
	if !strings.Contains(body, pinned) {
		t.Fatalf("%s does not pin ClickHouse %s", path, pinned)
	}
	if err := os.WriteFile(path, []byte(strings.ReplaceAll(body, pinned, olderClickHouseTag)), 0644); err != nil {
		t.Fatal(err)
	}
}

func assertPinned(t *testing.T, path string, want int) {
	t.Helper()
	body, pinned := readFile(t, path), pinnedClickHouseTag(t)
	if strings.Contains(body, olderClickHouseTag) {
		t.Errorf("%s still pins ClickHouse %s after the upgrade", path, olderClickHouseTag)
	}
	if got := strings.Count(body, pinned); got != want {
		t.Errorf("%s pins ClickHouse %s %d times, want %d", path, pinned, got, want)
	}
}

func TestRunUpgradeMovesClickHousePin(t *testing.T) {
	cfg := installedConfig(t)
	writeComposeInstall(t, cfg, "v0.9.0", nil)
	compose := filepath.Join(cfg.InstallDir, "docker-compose.yml")
	rewritePin(t, compose)
	newFakeExec(t, composeUpFails)

	if _, err := runUpgrade(t, cfg.InstallDir); err == nil || !strings.Contains(err.Error(), "docker compose up") {
		t.Fatalf("err = %v, want the restart failure", err)
	}
	assertPinned(t, compose, 1)
}

func TestRunUpgradeK8sMovesClickHouseAndKeeperPins(t *testing.T) {
	withoutExternalCommands(t)
	setVersion(t, "v1.5.0")
	dir := t.TempDir()
	cfg := tunedK8sConfig(dir)
	cfg.ImageTag = "v1.4.0"
	writeK8sInstall(t, cfg)
	installation := filepath.Join(dir, "clickhouse", "clickhouse-installation.yaml")
	rewritePin(t, installation)

	var err error
	out := captureStdout(t, func() { err = RunUpgradeK8s(dir, K8sUpgradeOpts{}) })
	if err != nil {
		t.Fatalf("RunUpgradeK8s: %v\n%s", err, out)
	}
	// One tag for the ClickHouse server, one for Keeper.
	assertPinned(t, installation, 2)
}
