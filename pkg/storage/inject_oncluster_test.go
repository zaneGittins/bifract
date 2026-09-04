package storage

import (
	"strings"
	"testing"
)

// Every DDL shape the app issues must pick up ON CLUSTER on a cluster. A statement
// that silently misses the clause is created on one node only, which surfaces much
// later as a missing table or dictionary on some other shard.
func TestInjectOnCluster(t *testing.T) {
	c := &ClickHouseClient{topo: Topology{DDLCluster: "bftest"}}
	cases := []struct{ name, sql, want string }{
		{"create table", "CREATE TABLE IF NOT EXISTS dict_x (a String) ENGINE = MergeTree() ORDER BY a",
			"CREATE TABLE IF NOT EXISTS dict_x ON CLUSTER 'bftest' ("},
		{"create or replace table", "CREATE OR REPLACE TABLE `dict_x_distributed` AS `dict_x` ENGINE = Distributed('bftest', currentDatabase(), 'dict_x', 0)",
			"CREATE OR REPLACE TABLE `dict_x_distributed` ON CLUSTER 'bftest' AS"},
		{"alter table", "ALTER TABLE dict_x DELETE WHERE k IN ('a')", "ALTER TABLE dict_x ON CLUSTER 'bftest' DELETE"},
		{"drop table", "DROP TABLE IF EXISTS dict_x", "DROP TABLE IF EXISTS dict_x ON CLUSTER 'bftest'"},
		{"create or replace dictionary", "CREATE OR REPLACE DICTIONARY `lookup_x` (\n a String)", "CREATE OR REPLACE DICTIONARY `lookup_x` ON CLUSTER 'bftest' ("},
		{"drop dictionary", "DROP DICTIONARY IF EXISTS `lookup_x`", "DROP DICTIONARY IF EXISTS `lookup_x` ON CLUSTER 'bftest'"},
		{"truncate", "TRUNCATE TABLE geoip_city", "TRUNCATE TABLE geoip_city ON CLUSTER 'bftest'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := c.InjectOnCluster(tc.sql)
			if !strings.Contains(got, tc.want) {
				t.Fatalf("got %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

// A single-node deployment names no cluster, so nothing is injected.
func TestInjectOnClusterSingleNode(t *testing.T) {
	c := &ClickHouseClient{}
	sql := "CREATE OR REPLACE TABLE x AS y ENGINE = Distributed('c', currentDatabase(), 'y', 0)"
	if got := c.InjectOnCluster(sql); got != sql {
		t.Fatalf("single node must not inject: %q", got)
	}
}
