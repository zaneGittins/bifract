//go:build chcluster

package storage

import (
	"testing"
	"time"

	"bifract/pkg/parser"
)

// The builder and the filters through Distributed tables: lineage lands on random shards, so a
// process's parent is often on the other one, and ancestry rows follow their process's key.
func TestProcAncestryAcrossShards(t *testing.T) {
	dropEverything(t)
	initialize(t)
	c := newTestClusterClient(t)
	defer c.Close()
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	rows := ancestryForest(base)
	seedAncestry(t, c.conn, "_distributed", rows)
	compareAncestry(t, c, rows, base, c.topo.Table(procAncestryTable), func(fractal string) parser.QueryOptions {
		return parser.QueryOptions{FractalID: fractal, TableName: c.ReadTable(), ProcLineageTable: c.ProcLineageReadTable(),
			ProcAncestryTable: c.topo.Table(procAncestryTable), ProcAncestryMarkTable: c.topo.Table(procAncestryWatermarkTable)}
	})
}
