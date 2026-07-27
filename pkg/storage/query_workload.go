package storage

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Query classes that cost the cluster real CPU and memory run under their own
// ClickHouse workload, so no one class can consume the machine and starve
// ingestion. Only queries explicitly tagged with a workload are scheduled:
// everything else (inserts, merges, mutations, alert evaluation) resolves to the
// 'default' workload, which is deliberately never created, and ClickHouse leaves
// unknown workloads unscheduled while throw_on_unknown_workload is false (the
// server default).
//
// The workloads form a tree because ClickHouse permits exactly one root. The root
// carries no limits of its own; it exists so the classes are siblings that share
// fairly rather than one class nesting inside another's budget.
//
//	bifract                 (root, unlimited)
//	├── bifract_search      interactive searches, dashboards, notebooks
//	└── bifract_recall      archive (Iceberg) scans, lower priority than search
const (
	QueryRootWorkload   = "bifract"
	QuerySearchWorkload = "bifract_search"
	QueryRecallWorkload = "bifract_recall"

	// QueryCPUResource and QueryMemoryResource are the scheduled resources. They are
	// global and may be shared with operator-defined workloads, so they are created
	// but never dropped.
	QueryCPUResource    = "cpu"
	QueryMemoryResource = "memory"

	// Default shares of each node's cores and ClickHouse memory budget.
	DefaultQueryCPUPercent    = 50
	DefaultQueryMemoryPercent = 50
	// Recall defaults lower than search: an archive scan is asynchronous, already
	// slow against object storage, and must never be what makes interactive search
	// feel slow.
	DefaultRecallCPUPercent    = 25
	DefaultRecallMemoryPercent = 25

	// MinQueryLimitPercent floors every share so a mistype cannot leave a class with
	// a sliver of the machine.
	MinQueryLimitPercent = 10

	// MaxCombinedQueryMemoryPercent is the most of a node's ClickHouse memory budget
	// that all query classes together may reserve, leaving the remainder for the work
	// this whole mechanism exists to protect: inserts, their materialized-view
	// cascade, merges and mutations. Without it the per-class shares are independent
	// caps that can sum past 100% (search 90 + recall 75 reserves 165% of the budget),
	// and queries alone could exhaust the server, which is the merge/insert collapse
	// this is meant to prevent.
	//
	// CPU is deliberately not bounded this way. Merges run unscheduled, so they keep
	// competing for cores at the OS level regardless of how many slots queries hold
	// (measured: an unscheduled query finished 6.6x faster than saturated, throttled
	// searches). Memory has no such fallback -- an exhausted budget is an error, not
	// a slowdown.
	MaxCombinedQueryMemoryPercent = 75

	// recallPriority puts recall behind search when both want the same core.
	// Lower value wins in ClickHouse, and search is left at the default 0.
	recallPriority = 1

	// workloadDDLTimeout bounds one workload statement on one node. These are
	// metadata-only, so a slow one means the node is in trouble.
	workloadDDLTimeout = 30 * time.Second
)

// WorkloadLimits is the per-class resource budget, as percentages of each node's
// cores and ClickHouse memory budget. 0 disables that class's limit.
type WorkloadLimits struct {
	SearchCPUPercent    int
	SearchMemoryPercent int
	RecallCPUPercent    int
	RecallMemoryPercent int
}

// ClampQueryLimitPercent bounds an admin-supplied share. 0 disables the limit, and
// 100 or more is treated as disabled since a whole-machine share is no limit at all.
func ClampQueryLimitPercent(percent int) int {
	if percent <= 0 || percent >= 100 {
		return 0
	}
	if percent < MinQueryLimitPercent {
		return MinQueryLimitPercent
	}
	return percent
}

// ClampCombinedMemory scales the per-class memory shares down proportionally when
// they would together reserve more than MaxCombinedQueryMemoryPercent of the node's
// budget, and reports whether it had to. Shares that already fit are returned
// unchanged. This is the backstop for values that reached the database from an older
// build or a manual edit; the settings API rejects them up front so an admin is told
// rather than silently scaled.
func ClampCombinedMemory(limits WorkloadLimits) (WorkloadLimits, bool) {
	total := limits.SearchMemoryPercent + limits.RecallMemoryPercent
	if total <= MaxCombinedQueryMemoryPercent {
		return limits, false
	}
	scale := float64(MaxCombinedQueryMemoryPercent) / float64(total)
	limits.SearchMemoryPercent = ClampQueryLimitPercent(int(float64(limits.SearchMemoryPercent) * scale))
	limits.RecallMemoryPercent = ClampQueryLimitPercent(int(float64(limits.RecallMemoryPercent) * scale))
	return limits, true
}

// ReconcileQueryWorkloads provisions the workload tree from the admin's shares.
// Idempotent, so it runs at startup and again whenever a share changes; ClickHouse
// has no ALTER WORKLOAD, so updates are applied with CREATE OR REPLACE and take
// effect for the next query.
//
// A class with both shares at 0 is dropped rather than created, which returns that
// class to unscheduled (pre-workload) behavior. The resources are left in place:
// they are global, an operator may have their own workloads scheduling against
// them, and an unreferenced resource constrains nothing.
//
// Failure is not fatal to querying. When provisioning does not succeed the client
// stops tagging, so queries run exactly as they did before this existed.
func (c *ClickHouseClient) ReconcileQueryWorkloads(ctx context.Context, limits WorkloadLimits) error {
	limits.SearchCPUPercent = ClampQueryLimitPercent(limits.SearchCPUPercent)
	limits.SearchMemoryPercent = ClampQueryLimitPercent(limits.SearchMemoryPercent)
	limits.RecallCPUPercent = ClampQueryLimitPercent(limits.RecallCPUPercent)
	limits.RecallMemoryPercent = ClampQueryLimitPercent(limits.RecallMemoryPercent)
	if clamped, did := ClampCombinedMemory(limits); did {
		log.Printf("[ClickHouse] Query memory shares totalled more than %d%%; scaled to search %d%% / recall %d%% so merges and ingestion keep headroom",
			MaxCombinedQueryMemoryPercent, clamped.SearchMemoryPercent, clamped.RecallMemoryPercent)
		limits = clamped
	}

	searchOn := limits.SearchCPUPercent > 0 || limits.SearchMemoryPercent > 0
	recallOn := limits.RecallCPUPercent > 0 || limits.RecallMemoryPercent > 0

	if !searchOn && !recallOn {
		c.setActiveWorkloads(nil)
		if err := c.dropWorkloadTree(ctx); err != nil {
			return err
		}
		log.Printf("[ClickHouse] Query resource limits disabled; all query classes run unscheduled")
		return nil
	}

	// Resources first: a workload referencing an absent resource is not scheduled.
	if limits.SearchCPUPercent > 0 || limits.RecallCPUPercent > 0 {
		if err := c.execWorkloadDDL(ctx, fmt.Sprintf("CREATE RESOURCE IF NOT EXISTS %s (MASTER THREAD, WORKER THREAD)", QueryCPUResource)); err != nil {
			c.setActiveWorkloads(nil)
			return fmt.Errorf("create cpu resource: %w", err)
		}
	}
	var memBudget int64
	if limits.SearchMemoryPercent > 0 || limits.RecallMemoryPercent > 0 {
		if err := c.execWorkloadDDL(ctx, fmt.Sprintf("CREATE RESOURCE IF NOT EXISTS %s (MEMORY RESERVATION)", QueryMemoryResource)); err != nil {
			c.setActiveWorkloads(nil)
			return fmt.Errorf("create memory resource: %w", err)
		}
		// max_memory is an absolute byte count, so it is derived from the server's own
		// budget rather than asked of the admin: the same percentage then means the
		// same thing on a laptop and on a 48GB node.
		budget, err := c.serverMemoryBudget(ctx)
		if err != nil {
			c.setActiveWorkloads(nil)
			return err
		}
		memBudget = budget
	}

	if err := c.ensureRootWorkload(ctx); err != nil {
		c.setActiveWorkloads(nil)
		return err
	}

	active := map[string]bool{}
	classes := []struct {
		name     string
		cpu, mem int
		priority int
		on       bool
	}{
		{QuerySearchWorkload, limits.SearchCPUPercent, limits.SearchMemoryPercent, 0, searchOn},
		{QueryRecallWorkload, limits.RecallCPUPercent, limits.RecallMemoryPercent, recallPriority, recallOn},
	}
	for _, cl := range classes {
		if !cl.on {
			if err := c.execWorkloadDDL(ctx, fmt.Sprintf("DROP WORKLOAD IF EXISTS %s", cl.name)); err != nil {
				return fmt.Errorf("drop %s workload: %w", cl.name, err)
			}
			continue
		}
		ddl := classWorkloadDDL(cl.name, cl.cpu, cl.mem, cl.priority, memBudget)
		if err := c.execWorkloadDDL(ctx, ddl); err != nil {
			c.setActiveWorkloads(nil)
			return fmt.Errorf("create %s workload: %w", cl.name, err)
		}
		active[cl.name] = true
	}

	c.setActiveWorkloads(active)
	log.Printf("[ClickHouse] Query limits per node: search CPU %s / memory %s, recall CPU %s / memory %s",
		percentLabel(limits.SearchCPUPercent), percentLabel(limits.SearchMemoryPercent),
		percentLabel(limits.RecallCPUPercent), percentLabel(limits.RecallMemoryPercent))
	return nil
}

// ensureRootWorkload creates the tree root. ClickHouse allows only one root and
// refuses both a second root and re-parenting an existing one, so an existing root
// of ours is dropped and rebuilt rather than moved.
func (c *ClickHouseClient) ensureRootWorkload(ctx context.Context) error {
	root, err := c.existingWorkloadRoot(ctx)
	if err != nil {
		return fmt.Errorf("read workload hierarchy: %w", err)
	}
	switch {
	case root == QueryRootWorkload, root == "":
		// Ours already, or no hierarchy yet.
	case root == QuerySearchWorkload || root == QueryRecallWorkload:
		// One of ours sitting at the root from the pre-tree layout. Rebuild rather
		// than nest the tree under a leaf; children must be dropped first.
		if err := c.dropWorkloadTree(ctx); err != nil {
			return err
		}
	default:
		// An operator defined their own root; hang our tree beneath it rather than
		// fighting over the single root slot.
		return c.execWorkloadDDL(ctx, fmt.Sprintf("CREATE OR REPLACE WORKLOAD %s IN %s", QueryRootWorkload, root))
	}
	return c.execWorkloadDDL(ctx, fmt.Sprintf("CREATE OR REPLACE WORKLOAD %s", QueryRootWorkload))
}

// dropWorkloadTree removes our workloads, children before parents (ClickHouse
// refuses to drop a workload that still has children).
func (c *ClickHouseClient) dropWorkloadTree(ctx context.Context) error {
	for _, name := range []string{QuerySearchWorkload, QueryRecallWorkload, QueryRootWorkload} {
		if err := c.execWorkloadDDL(ctx, fmt.Sprintf("DROP WORKLOAD IF EXISTS %s", name)); err != nil {
			return fmt.Errorf("drop workload %s: %w", name, err)
		}
	}
	return nil
}

// classWorkloadDDL builds the CREATE OR REPLACE statement for one class. A share of
// 0 omits that limit rather than setting it to zero, leaving the resource unbounded
// for the class. memBudget is the node's ClickHouse memory budget in bytes.
func classWorkloadDDL(name string, cpuPercent, memPercent, priority int, memBudget int64) string {
	var settings []string
	if cpuPercent > 0 {
		settings = append(settings, fmt.Sprintf("max_concurrent_threads_ratio_to_cores = %.2f", float64(cpuPercent)/100))
	}
	if memPercent > 0 && memBudget > 0 {
		settings = append(settings, fmt.Sprintf("max_memory = %d", memBudget/100*int64(memPercent)))
	}
	if priority != 0 {
		settings = append(settings, fmt.Sprintf("priority = %d", priority))
	}
	return fmt.Sprintf("CREATE OR REPLACE WORKLOAD %s IN %s SETTINGS %s",
		name, QueryRootWorkload, strings.Join(settings, ", "))
}

func percentLabel(percent int) string {
	if percent == 0 {
		return "uncapped"
	}
	return strconv.Itoa(percent) + "%"
}

// serverMemoryBudget reads the node's max_server_memory_usage. In cluster mode this
// is the node the client is connected to; the workload limit applies per node, so a
// uniform cluster gets a uniform share.
func (c *ClickHouseClient) serverMemoryBudget(ctx context.Context) (int64, error) {
	qctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	rows, err := c.Query(qctx, "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
	if err != nil {
		return 0, fmt.Errorf("read server memory budget: %w", err)
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("read server memory budget: max_server_memory_usage not reported")
	}
	s, _ := rows[0]["value"].(string)
	budget, perr := strconv.ParseInt(s, 10, 64)
	if perr != nil || budget <= 0 {
		return 0, fmt.Errorf("read server memory budget: unusable value %q", s)
	}
	return budget, nil
}

// existingWorkloadRoot returns the name of the current root workload (the one with
// no parent), or "" when the hierarchy is empty.
func (c *ClickHouseClient) existingWorkloadRoot(ctx context.Context) (string, error) {
	qctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	rows, err := c.Query(qctx, "SELECT name FROM system.workloads WHERE parent = '' ORDER BY name LIMIT 1")
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", nil
	}
	name, _ := rows[0]["name"].(string)
	return name, nil
}

// execWorkloadDDL applies one workload statement to every node.
//
// ON CLUSTER is deliberately never used. The bundled ClickHouseInstallation sets
// workload_zookeeper_path, which makes Keeper replicate workload entities itself, and
// ClickHouse then rejects the clause outright:
//
//	Code: 80. ON CLUSTER is not allowed because workload entities are replicated
//	automatically. (INCORRECT_QUERY)
//
// That failure is only logged, so every cluster silently ran with no query limits at
// all. Executing the statement directly on each node is correct under either storage
// mode: with Keeper-backed entities the first node's write propagates and the rest are
// idempotent no-ops (every statement here is IF NOT EXISTS, OR REPLACE, or IF EXISTS),
// and with node-local entities each node genuinely needs its own copy.
func (c *ClickHouseClient) execWorkloadDDL(ctx context.Context, stmt string) error {
	if !c.IsCluster() {
		sctx, cancel := context.WithTimeout(ctx, workloadDDLTimeout)
		defer cancel()
		return c.conn.Exec(sctx, stmt)
	}

	pool := ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1, DialTimeout: 10 * time.Second}
	for _, addr := range c.addrs {
		conn, err := openClickHouseConn([]string{addr}, c.Database, c.User, c.Password, pool)
		if err != nil {
			return fmt.Errorf("connect to %s: %w", addr, err)
		}
		sctx, cancel := context.WithTimeout(ctx, workloadDDLTimeout)
		execErr := conn.Exec(sctx, stmt)
		cancel()
		conn.Close()
		if execErr != nil {
			return fmt.Errorf("on %s: %w", addr, execErr)
		}
	}
	return nil
}

// setActiveWorkloads records which workloads are provisioned. Queries are tagged
// only for workloads in this set, so a failed or disabled reconcile leaves them
// unscheduled rather than naming a workload that does not exist.
func (c *ClickHouseClient) setActiveWorkloads(active map[string]bool) {
	c.activeWorkloads.Store(&active)
}

func (c *ClickHouseClient) workloadActive(name string) bool {
	m := c.activeWorkloads.Load()
	if m == nil || *m == nil {
		return false
	}
	return (*m)[name]
}

// workloadKey marks a context as belonging to a scheduled query class.
type workloadKey struct{}

// WorkloadContext marks ctx as belonging to the named workload. Query helpers tag
// such queries, so limits apply to that class without touching ingestion, alerting,
// or model queries that share the same helpers.
func WorkloadContext(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, workloadKey{}, name)
}

// UserSearchContext marks ctx as an interactive user search.
func UserSearchContext(ctx context.Context) context.Context {
	return WorkloadContext(ctx, QuerySearchWorkload)
}

// RecallContext marks ctx as an archive (Iceberg) scan. This is the only enforcement
// recall has: ClickHouse does not apply max_bytes_to_read to iceberg table
// functions, so the byte ceiling can only gate admission, never a running scan.
func RecallContext(ctx context.Context) context.Context {
	return WorkloadContext(ctx, QueryRecallWorkload)
}

// contextWorkload returns the workload ctx was marked with, or "".
func contextWorkload(ctx context.Context) string {
	name, _ := ctx.Value(workloadKey{}).(string)
	return name
}

// applyWorkload adds the workload tag to settings when ctx is marked and that
// workload is provisioned. Callers pass the settings map they were going to send
// anyway, since clickhouse-go's WithSettings replaces rather than merges.
func (c *ClickHouseClient) applyWorkload(ctx context.Context, settings clickhouse.Settings) clickhouse.Settings {
	name := contextWorkload(ctx)
	if name == "" || !c.workloadActive(name) {
		return settings
	}
	if settings == nil {
		settings = clickhouse.Settings{}
	}
	settings["workload"] = name
	return settings
}
