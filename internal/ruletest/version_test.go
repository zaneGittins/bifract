package ruletest

import (
	"os"
	"regexp"
	"testing"
)

// The throwaway container must run the same ClickHouse version a real deployment
// does, or a rule could pass in CI and behave differently in production. Nothing
// else links these two constants, so assert it.
func TestClickHouseImageMatchesCompose(t *testing.T) {
	data, err := os.ReadFile("../../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	re := regexp.MustCompile(`image:\s*(clickhouse/clickhouse-server:\S+)`)
	m := re.FindSubmatch(data)
	if m == nil {
		t.Fatal("no clickhouse-server image found in docker-compose.yml")
	}

	if got := string(m[1]); got != ClickHouseImage {
		t.Errorf("ClickHouseImage = %q, but docker-compose.yml uses %q.\n"+
			"Update ClickHouseImage in internal/ruletest/clickhouse.go to match.", ClickHouseImage, got)
	}
}
