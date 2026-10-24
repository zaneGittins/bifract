package storage

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

// Search field filters hint the norm_log n-gram index on the premise that norm_log
// is always its column DEFAULT, toString(fields). A writer that sets it from code
// could store text missing a field's value, and hinted searches would then skip
// rows that match. These tests fail on any such writer.

func TestLogsInsertLeavesNormLogToItsDefault(t *testing.T) {
	topos := map[string]Topology{
		"single node": {Kind: DeploymentSingleNode},
		"cluster":     {Kind: DeploymentSelfManagedCluster, DistributedTables: true, ShardRouting: true, DDLCluster: "c1", FanoutCluster: "c1"},
	}
	for name, topo := range topos {
		t.Run(name, func(t *testing.T) {
			f := fakedb.NewCH()
			c := NewClickHouseClientFromConn(f, "logs", topo)
			if err := c.InsertLogs(context.Background(), []LogEntry{{LogID: "a", FractalID: "F", Fields: map[string]string{"k": "v"}}}); err != nil {
				t.Fatal(err)
			}
			for _, b := range f.Batches() {
				if strings.Contains(b.Query, "norm_log") {
					t.Errorf("insert writes norm_log: %s", b.Query)
				}
			}
		})
	}
}

var (
	insertInto = regexp.MustCompile(`(?i)INSERT\s+INTO\b`)
	// columnListEnd closes an INSERT's column list: a ")" followed by the end of
	// its string literal or the statement's next clause, not a call like Table().
	columnListEnd     = regexp.MustCompile("(?i)\\)\\s*(?:\"|`|SELECT\\b|VALUES\\b|FORMAT\\b|SETTINGS\\b)")
	normLogColumnName = regexp.MustCompile(`\bnorm_log\b`)
)

// insertHeads returns each INSERT statement's text up to the end of its column
// list, however the statement is assembled in source.
func insertHeads(src []byte) [][]byte {
	var heads [][]byte
	for _, loc := range insertInto.FindAllIndex(src, -1) {
		rest := src[loc[0]:min(loc[0]+600, len(src))]
		if end := columnListEnd.FindIndex(rest); end != nil {
			rest = rest[:end[0]+1]
		}
		heads = append(heads, rest)
	}
	return heads
}

// Reading norm_log (an archive restore rebuilds fields from it) is fine; naming it
// as an INSERT column is not.
func TestNoCodeInsertsNormLog(t *testing.T) {
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "web":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, head := range insertHeads(src) {
			if normLogColumnName.Match(head) {
				t.Errorf("%s writes norm_log in an INSERT: %q", path, head)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
