// Package ruleeval evaluates a BQL rule against sample events without touching the
// logs table. It is the engine shared by the `bifract --test` CLI and by the alert
// editor's in-app tests, so a rule behaves identically in CI and in the UI.
package ruleeval

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"bifract/pkg/storage"
)

// DefaultDatabase is the database holding the log schema.
const DefaultDatabase = "logs"

// Scratch is a private clone of the logs table that an evaluation run writes to.
//
// Cloning rather than writing to `logs` is what makes this safe against a live
// deployment: sample events never reach real data, never fire a materialized view,
// never enter the archive tee or cold tiering, and cleanup is a DROP rather than a
// mutation. The clone carries the real column types, JSON type hints, the norm_log
// DEFAULT expression and the skip indexes, so a rule sees the schema it will see in
// production.
type Scratch struct {
	client   *storage.ClickHouseClient
	database string
	table    string
}

// NewScratch creates a scratch table on the given connection. The caller owns the
// client; Drop removes only the table.
//
// On a cluster the client must be pinned to one node: the table is local, so a
// load-balanced connection can create it on one host and then query another.
func NewScratch(ctx context.Context, client *storage.ClickHouseClient) (*Scratch, error) {
	if client == nil {
		return nil, fmt.Errorf("ruleeval: nil ClickHouse client")
	}

	database := client.Database
	if database == "" {
		database = DefaultDatabase
	}

	name, err := scratchName()
	if err != nil {
		return nil, err
	}
	s := &Scratch{client: client, database: database, table: name}

	keys, err := s.logsKeys(ctx)
	if err != nil {
		return nil, err
	}

	// The engine is stated rather than inherited, for two reasons.
	//
	// A bare `CREATE TABLE x AS logs` copies the engine definition, which on a cluster
	// means copying ReplicatedMergeTree along with its Keeper path: the scratch table
	// would register as another replica of `logs` itself. It also copies any table
	// TTL, so sample events older than the retention window could be collected
	// mid-run.
	//
	// Naming the engine avoids both. It costs nothing that matters: the column types,
	// JSON type hints, norm_log DEFAULT and every skip index still come across, and
	// only replication and TTL are left behind, neither of which a scratch table wants.
	stmt := fmt.Sprintf("CREATE TABLE %s AS %s.logs ENGINE = MergeTree", s.Qualified(), database)
	if keys.partition != "" {
		stmt += " PARTITION BY " + keys.partition
	}
	if keys.sorting != "" {
		stmt += " ORDER BY (" + keys.sorting + ")"
	} else {
		stmt += " ORDER BY tuple()"
	}

	if err := client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("creating scratch table: %w", err)
	}
	return s, nil
}

type logsKeys struct {
	sorting   string
	partition string
}

// logsKeys reads the real table's keys so the scratch clone partitions and orders
// identically, which keeps a rule reading the same shape it reads in production.
func (s *Scratch) logsKeys(ctx context.Context) (logsKeys, error) {
	rows, err := s.client.Query(ctx, fmt.Sprintf(
		"SELECT sorting_key, partition_key FROM system.tables WHERE database = '%s' AND name = 'logs'",
		storage.EscCHStr(s.database)))
	if err != nil {
		return logsKeys{}, fmt.Errorf("reading logs table keys: %w", err)
	}
	if len(rows) == 0 {
		return logsKeys{}, fmt.Errorf("no logs table in database %s", s.database)
	}

	keys := logsKeys{}
	if v, ok := rows[0]["sorting_key"].(string); ok {
		keys.sorting = v
	}
	if v, ok := rows[0]["partition_key"].(string); ok {
		keys.partition = v
	}
	return keys, nil
}

func scratchName() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating scratch table name: %w", err)
	}
	return "ruletest_" + hex.EncodeToString(b), nil
}

// Table is the unqualified scratch table name, which is what the BQL translator takes.
func (s *Scratch) Table() string { return s.table }

// Qualified is the database-qualified scratch table name.
func (s *Scratch) Qualified() string { return s.database + "." + s.table }

// Client exposes the underlying connection for callers that need to run their own
// statements against the scratch table.
func (s *Scratch) Client() *storage.ClickHouseClient { return s.client }

// Drop removes the scratch table. Safe to call more than once.
func (s *Scratch) Drop(ctx context.Context) error {
	if s == nil || s.client == nil || s.table == "" {
		return nil
	}
	if err := s.client.Exec(ctx, "DROP TABLE IF EXISTS "+s.Qualified()); err != nil {
		return fmt.Errorf("dropping scratch table %s: %w", s.table, err)
	}
	s.table = ""
	return nil
}
