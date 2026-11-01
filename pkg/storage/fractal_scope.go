package storage

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// Fractal isolation has two layers. The translator adds a fractal_id predicate to
// every query it builds; this file is the second layer, enforced by ClickHouse, so a
// query that loses that predicate (an alias that shadows fractal_id, a raw SQL
// fragment) still cannot read another fractal's rows.
//
// The scope rides the query_id. A query_id is set by the client in the protocol
// header, so nothing in the SQL text can change it: no SETTINGS clause, subquery or
// table function reaches it. initialQueryID() is propagated to every shard a
// distributed read fans out to, even when the shard runs the read as the cluster's
// own user, so the policy holds in cluster mode too. A custom setting would not:
// a SETTINGS clause in the query overrides it, and custom settings need a server
// config change on every deployment.
//
// The policies live on the local tables. ClickHouse refuses a row policy on a
// Distributed table that it cannot push down to the shards, so the underlying
// tables are the only place one can be enforced.

// FractalScope is the set of fractals a query may read.
type FractalScope struct {
	FractalIDs []string
	// Unassigned admits rows stored with an empty fractal_id, which belong to the
	// default fractal.
	Unassigned bool
}

// fractalScopeMarker separates the caller's query id from the encoded scope.
const fractalScopeMarker = "~bfscope~"

// fractalScopeTables are the local tables that hold log content. Each read path
// against them carries the scope of the predicate it was translated with.
var fractalScopeTables = []string{"logs", "logs_hot", "logs_raw"}

// Policy names carry a version: a changed expression ships under a new name so
// IF NOT EXISTS never leaves an old definition in place.
const (
	fractalScopePolicy    = "bifract_fractal_scope_v1"
	scopeRequiredPolicy   = "bifract_scope_required_v1"
	fractalScopeListSQL   = "arrayPopBack(splitByChar(',', splitByString('" + fractalScopeMarker + "', initialQueryID())[2]))"
	fractalScopeMarkedSQL = "position(initialQueryID(), '" + fractalScopeMarker + "') > 0"
)

// fractalScopeFilter applies to every user and is a no-op for a query that carries
// no scope: ingest, merges, alerts on unscoped rules and admin tooling read as before.
const fractalScopeFilter = "NOT " + fractalScopeMarkedSQL + " OR has(" + fractalScopeListSQL + ", fractal_id)"

type scopeKey struct{}
type queryIDKey struct{}
type sentQueryIDKey struct{}

// ScopeContext attaches the fractals every read on ctx is confined to.
func ScopeContext(ctx context.Context, scope FractalScope) context.Context {
	return context.WithValue(ctx, scopeKey{}, scope)
}

func contextScope(ctx context.Context) (FractalScope, bool) {
	s, ok := ctx.Value(scopeKey{}).(FractalScope)
	return s, ok
}

// withQueryID records the caller's query id. It is applied when the read runs,
// after the scope is appended, so the two can never be set out of step.
func withQueryID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, queryIDKey{}, id)
}

// encode renders the scope as the policy parses it: every element is terminated by
// a comma and the trailing empty element is dropped, so an empty scope is an empty
// list rather than one empty id, which would admit the unassigned rows.
func (s FractalScope) encode() (string, error) {
	var b strings.Builder
	for _, id := range s.FractalIDs {
		if id == "" || strings.ContainsAny(id, ",~") {
			return "", fmt.Errorf("invalid fractal id %q in query scope", id)
		}
		b.WriteString(id)
		b.WriteByte(',')
	}
	if s.Unassigned {
		b.WriteByte(',')
	}
	return b.String(), nil
}

// scopedQueryID is the query id a read on ctx must run under. A search without a
// scope is refused here rather than sent: the search identity would read nothing,
// and the caller is a bug that should fail loudly.
func scopedQueryID(ctx context.Context) (string, error) {
	id, _ := ctx.Value(queryIDKey{}).(string)
	scope, ok := contextScope(ctx)
	if !ok {
		if contextWorkload(ctx) == QuerySearchWorkload {
			return "", fmt.Errorf("search query has no fractal scope")
		}
		return id, nil
	}
	enc, err := scope.encode()
	if err != nil {
		return "", err
	}
	if id == "" {
		id = uuid.NewString()
	}
	return id + fractalScopeMarker + enc, nil
}

// scopeCacheSettings keys the query cache by scope. The cache ignores query_id, so
// without a tag a result cached under one scope would be served to another that
// sent the same SQL, which is exactly the case this backstop exists for.
func scopeCacheSettings(ctx context.Context, settings clickhouse.Settings) clickhouse.Settings {
	scope, ok := contextScope(ctx)
	if !ok {
		return settings
	}
	enc, err := scope.encode()
	if err != nil {
		return settings // readConn refuses the read
	}
	if settings == nil {
		settings = clickhouse.Settings{}
	}
	settings["query_cache_tag"] = fractalScopeMarker + enc
	return settings
}

// SentQueryIDOf returns the query id a read was sent with, scope included. Test
// drivers read it, since clickhouse-go keeps its own copy private.
func SentQueryIDOf(ctx context.Context) string {
	id, _ := ctx.Value(sentQueryIDKey{}).(string)
	return id
}

// QueryIDMatchSQL matches a query_id column against an id the caller assigned,
// whether or not a fractal scope was appended to it.
func QueryIDMatchSQL(column, id string) string {
	e := EscCHStr(id)
	return fmt.Sprintf("(%s = '%s' OR startsWith(%s, '%s%s'))", column, e, column, e, fractalScopeMarker)
}

// ensureFractalScopePolicies creates the row policies when any is missing. Never
// fatal: without them isolation rests on the translator's predicate alone, which is
// how every release before this one ran.
func (c *ClickHouseClient) ensureFractalScopePolicies(ctx context.Context, searchIdentity bool) {
	want := map[string]bool{fractalScopePolicy: true}
	if searchIdentity {
		want[scopeRequiredPolicy] = true
	}
	present := c.presentScopePolicies(ctx)
	for _, stmt := range c.fractalScopePolicyDDL() {
		name := stmt.name
		if !want[name] || present[name] {
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
		err := c.conn.Exec(sctx, stmt.sql)
		cancel()
		if err != nil {
			log.Printf("[ClickHouse] Row policy %s unavailable, fractal isolation relies on query predicates only: %v", name, err)
			continue
		}
		log.Printf("[ClickHouse] Row policy %s ensured", name)
	}
}

type scopePolicyStmt struct{ name, sql string }

// fractalScopePolicyDDL is restrictive on purpose: a restrictive policy narrows
// whatever else an operator has defined, where a permissive one would widen it.
func (c *ClickHouseClient) fractalScopePolicyDDL() []scopePolicyStmt {
	db := "`" + EscCHIdent(c.logsDatabase()) + "`"
	tables := make([]string, len(fractalScopeTables))
	for i, t := range fractalScopeTables {
		tables[i] = db + ".`" + t + "`"
	}
	on := strings.Join(tables, ", ")
	cl := c.OnClusterSQL()
	return []scopePolicyStmt{
		{fractalScopePolicy, fmt.Sprintf("CREATE ROW POLICY IF NOT EXISTS %s%s ON %s AS RESTRICTIVE FOR SELECT USING %s TO ALL",
			fractalScopePolicy, cl, on, fractalScopeFilter)},
		// The search identity only ever runs scoped reads, so an unmarked one is a
		// bug and reads nothing.
		{scopeRequiredPolicy, fmt.Sprintf("CREATE ROW POLICY IF NOT EXISTS %s%s ON %s AS RESTRICTIVE FOR SELECT USING %s TO %s",
			scopeRequiredPolicy, cl, on, fractalScopeMarkedSQL, SearchCHUser)},
	}
}

// presentScopePolicies reports which policies already cover every scoped table.
// A read is cheaper than DDL, which on a cluster goes through the replicated queue
// on every settings save.
func (c *ClickHouseClient) presentScopePolicies(ctx context.Context) map[string]bool {
	present := map[string]bool{}
	sctx, cancel := context.WithTimeout(ctx, queryIdentityDDLTimeout)
	defer cancel()
	quoted := make([]string, len(fractalScopeTables))
	for i, t := range fractalScopeTables {
		quoted[i] = "'" + EscCHStr(t) + "'"
	}
	rows, err := c.conn.Query(sctx, fmt.Sprintf(
		"SELECT short_name, count() AS n FROM system.row_policies WHERE database = '%s' AND short_name IN ('%s', '%s') AND table IN (%s) GROUP BY short_name",
		EscCHStr(c.logsDatabase()), fractalScopePolicy, scopeRequiredPolicy, strings.Join(quoted, ", ")))
	if err != nil {
		return present
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var n uint64
		if err := rows.Scan(&name, &n); err != nil {
			return map[string]bool{}
		}
		present[name] = n == uint64(len(fractalScopeTables))
	}
	return present
}
