package ruletest

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// clickHousePinFiles holds every ClickHouse server or Keeper pin, relative to the repo
// root. The setup templates are what --upgrade and --upgrade-k8s render onto existing
// installs, so a missed one silently leaves those installs on the old version.
var clickHousePinFiles = []string{
	"docker-compose.yml",
	"internal/setup/templates/docker-compose.yml.tmpl",
	"internal/setup/templates/k8s/clickhouse-installation.yaml.tmpl",
	"test/integration/docker-compose.yml",
	"test/chcluster/docker-compose.yml",
	"docs/alerting/testing-rules.md",
}

var (
	imagePinRe = regexp.MustCompile(`clickhouse/clickhouse-(?:server|keeper):([\w.-]+)`)
	// The ClickHouse operator CRDs split the reference into repository and tag.
	operatorPinRe = regexp.MustCompile(`repository:\s*clickhouse/clickhouse-(?:server|keeper)\s*\n\s*tag:\s*"?([\w.-]+)"?`)
)

// The throwaway container must run the same ClickHouse version a real deployment
// does, or a rule could pass in CI and behave differently in production.
func TestClickHousePinsMatch(t *testing.T) {
	want := strings.TrimPrefix(ClickHouseImage, "clickhouse/clickhouse-server:")
	for _, rel := range clickHousePinFiles {
		data, err := os.ReadFile(filepath.Join("..", "..", rel))
		if err != nil {
			t.Fatal(err)
		}
		var tags [][]byte
		for _, re := range []*regexp.Regexp{imagePinRe, operatorPinRe} {
			for _, m := range re.FindAllSubmatch(data, -1) {
				tags = append(tags, m[1])
			}
		}
		if len(tags) == 0 {
			t.Errorf("%s: no ClickHouse pin found; drop it from clickHousePinFiles if it no longer has one", rel)
		}
		for _, tag := range tags {
			if string(tag) != want {
				t.Errorf("%s pins ClickHouse %s, want %s (ClickHouseImage in internal/ruletest/clickhouse.go)", rel, tag, want)
			}
		}
	}
}
