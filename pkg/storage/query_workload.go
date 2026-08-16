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

// Query classes that cost the cluster real CPU and memory are limited so no one
// class can consume the machine and starve ingestion. CPU is scheduled through a
// ClickHouse workload; memory is capped per query with max_memory_usage.
//
// Only queries explicitly tagged with a workload are scheduled: everything else
// (inserts, merges, mutations, alert evaluation) resolves to the 'default'
// workload, which is deliberately never created, and ClickHouse leaves unknown
// workloads unscheduled while throw_on_unknown_workload is false (the server
// default).
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

	// QueryCPUResource is the scheduled resource. It is global and may be shared with
	// operator-defined workloads, so it is created but never dropped.
	QueryCPUResource = "cpu"

	// QueryMemoryResource is the MEMORY RESERVATION resource earlier builds created to
	// back the workloads' max_memory. It is now dropped on sight; see
	// dropMemoryReservationResource.
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

	// MaxCombinedQueryMemoryPercent bounds what the query classes' ceilings may add up
	// to, leaving the remainder for the work this whole mechanism exists to protect:
	// inserts, their materialized-view cascade, merges and mutations. The shares are
	// per-query ceilings rather than reservations, so this is a sanity bound on how
	// much of the node one search may claim, not an allocation.
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

// ReconcileQueryWorkloads provisions the CPU workload tree and the per-query memory
// ceilings from the admin's shares. Idempotent, so it runs at startup and again
// whenever a share changes; ClickHouse has no ALTER WORKLOAD, so updates are applied
// with CREATE OR REPLACE and take effect for the next query.
//
// A class whose CPU share is 0 has its workload dropped rather than created, which
// returns that class to unscheduled (pre-workload) behavior. Its memory ceiling is
// independent and still applies. The cpu resource is left in place: it is global, an
// operator may have their own workloads scheduling against it, and an unreferenced
// resource constrains nothing.
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

	// Memory ceilings are per query and independent of the workload tree, so they are
	// resolved first and apply even to a class whose CPU share is off. The share is an
	// absolute byte count derived from the server's own budget rather than asked of the
	// admin: the same percentage then means the same thing on a laptop and a 48GB node.
	memCaps := map[string]int64{}
	if limits.SearchMemoryPercent > 0 || limits.RecallMemoryPercent > 0 {
		budget, err := c.serverMemoryBudget(ctx)
		if err != nil {
			c.setQueryMemoryCaps(nil)
			c.setActiveWorkloads(nil)
			return err
		}
		if limits.SearchMemoryPercent > 0 {
			memCaps[QuerySearchWorkload] = budget / 100 * int64(limits.SearchMemoryPercent)
		}
		if limits.RecallMemoryPercent > 0 {
			memCaps[QueryRecallWorkload] = budget / 100 * int64(limits.RecallMemoryPercent)
		}
	}
	c.setQueryMemoryCaps(memCaps)

	// The per-query ceiling above bounds one search; the class identity bounds every
	// search at once. Both carry the same byte count, which together reproduce what the
	// workload's max_memory was meant to do.
	c.reconcileQueryIdentities(ctx, memCaps)

	searchOn := limits.SearchCPUPercent > 0
	recallOn := limits.RecallCPUPercent > 0

	if !searchOn && !recallOn {
		c.setActiveWorkloads(nil)
		if err := c.dropWorkloadTree(ctx); err != nil {
			return err
		}
		c.dropMemoryReservationResource(ctx)
		log.Printf("[ClickHouse] Query CPU limits disabled; all query classes run unscheduled (memory: search %s, recall %s)",
			percentLabel(limits.SearchMemoryPercent), percentLabel(limits.RecallMemoryPercent))
		return nil
	}

	// Resource first: a workload referencing an absent resource is not scheduled.
	// This is also the capability verdict for workload scheduling as a whole: it is
	// the first statement the feature needs, so its outcome answers the question
	// without a second, synthetic probe that could disagree.
	resErr := c.execWorkloadDDL(ctx, fmt.Sprintf("CREATE RESOURCE IF NOT EXISTS %s (MASTER THREAD, WORKER THREAD)", QueryCPUResource))
	c.recordCapability(CapWorkloadScheduling, resErr)
	if resErr != nil {
		c.setActiveWorkloads(nil)
		return fmt.Errorf("create cpu resource: %w", resErr)
	}

	if err := c.ensureRootWorkload(ctx); err != nil {
		c.setActiveWorkloads(nil)
		return err
	}

	active := map[string]bool{}
	classes := []struct {
		name     string
		cpu      int
		priority int
		on       bool
	}{
		{QuerySearchWorkload, limits.SearchCPUPercent, 0, searchOn},
		{QueryRecallWorkload, limits.RecallCPUPercent, recallPriority, recallOn},
	}
	for _, cl := range classes {
		if !cl.on {
			if err := c.execWorkloadDDL(ctx, fmt.Sprintf("DROP WORKLOAD IF EXISTS %s", cl.name)); err != nil {
				return fmt.Errorf("drop %s workload: %w", cl.name, err)
			}
			continue
		}
		if err := c.execWorkloadDDL(ctx, classWorkloadDDL(cl.name, cl.cpu, cl.priority)); err != nil {
			c.setActiveWorkloads(nil)
			return fmt.Errorf("create %s workload: %w", cl.name, err)
		}
		active[cl.name] = true
	}

	c.setActiveWorkloads(active)
	// Only once no workload still names max_memory, or the DROP is refused.
	c.dropMemoryReservationResource(ctx)
	log.Printf("[ClickHouse] Query limits per node: search CPU %s / memory %s, recall CPU %s / memory %s",
		percentLabel(limits.SearchCPUPercent), percentLabel(limits.SearchMemoryPercent),
		percentLabel(limits.RecallCPUPercent), percentLabel(limits.RecallMemoryPercent))
	return nil
}

// dropMemoryReservationResource removes the MEMORY RESERVATION resource that earlier
// builds created to back the workloads' max_memory. It is dropped rather than left
// unreferenced because ClickHouse builds a MemoryReservation for every query whose
// workload resolves a link in that resource, whether or not any workload sets
// max_memory. MemoryReservation::syncWithMemoryTracker can then deadlock: an increase
// sized against an allocated_size that a concurrent in-flight decrease afterwards
// lowers leaves the waiter permanently short of its target, with no path to re-request.
// Its wait has no timeout and breaks only on the scheduler's own kill, never on the
// query's cancellation flag, so KILL QUERY cannot free it -- the query sits in
// system.processes as "Stopping" forever, holding its reservation and a connection
// thread until ClickHouse restarts. Verified unfixed as of 26.6.2.81 and master.
//
// Best effort: an operator's own workloads may still reference the resource, and
// failing to drop it must not take query limits down with it.
func (c *ClickHouseClient) dropMemoryReservationResource(ctx context.Context) {
	if err := c.execWorkloadDDL(ctx, fmt.Sprintf("DROP RESOURCE IF EXISTS %s", QueryMemoryResource)); err != nil {
		log.Printf("[ClickHouse] Could not drop the %s resource (queries may still hit the reservation deadlock): %v", QueryMemoryResource, err)
	}
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

// classWorkloadDDL builds the CREATE OR REPLACE statement for one class. Callers only
// reach here with a CPU share above 0, so the settings list is never empty.
//
// max_memory is deliberately absent: it is what engages the MEMORY RESERVATION
// scheduler, whose deadlock is described on dropMemoryReservationResource. Memory is
// capped per query with max_memory_usage in applyQuerySettings instead.
func classWorkloadDDL(name string, cpuPercent, priority int) string {
	settings := []string{fmt.Sprintf("max_concurrent_threads_ratio_to_cores = %.2f", float64(cpuPercent)/100)}
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

// serverMemoryBudget reads the node's memory budget, which the admin's percentage
// shares are resolved against. In cluster mode this is the node the client is
// connected to; the limit applies per node, so a uniform cluster gets a uniform share.
//
// max_server_memory_usage is the right answer where it is readable. Where it is not,
// falling back to the cgroup or host total keeps the per-query memory ceilings, which
// are the part that still works on a server that exposes no server settings. Losing
// the whole feature because one system table is unreadable would be a worse trade.
func (c *ClickHouseClient) serverMemoryBudget(ctx context.Context) (int64, error) {
	qctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	rows, err := c.Query(qctx, "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
	if err == nil && len(rows) > 0 {
		s, _ := rows[0]["value"].(string)
		if budget, perr := strconv.ParseInt(s, 10, 64); perr == nil && budget > 0 {
			c.caps.set(CapServerMemoryBudget, CapAvailable, "")
			return budget, nil
		}
	}

	mrows, merr := c.Query(qctx, SystemMemoryMetricsSQL)
	if merr != nil {
		c.recordCapability(CapServerMemoryBudget, merr)
		return 0, fmt.Errorf("read server memory budget: %w", merr)
	}
	m := MetricRowsToMap(mrows)
	for _, src := range []struct{ metric, label string }{
		{"CGroupMemoryTotal", "cgroup limit"},
		{"OSMemoryTotal", "host memory"},
	} {
		if v := m[src.metric]; v > 0 {
			c.caps.set(CapServerMemoryBudget, CapAvailable,
				"max_server_memory_usage is not readable; shares are resolved against the "+src.label)
			return int64(v), nil
		}
	}
	c.caps.set(CapServerMemoryBudget, CapUnavailable,
		"this server reports neither max_server_memory_usage nor a memory total, so percentage shares cannot be resolved")
	return 0, fmt.Errorf("read server memory budget: no usable memory total reported")
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
	if !c.topo.PerNodeAdmin {
		sctx, cancel := context.WithTimeout(ctx, workloadDDLTimeout)
		defer cancel()
		return c.conn.Exec(sctx, stmt)
	}

	pool := adminNodePool
	for _, addr := range c.addrs {
		conn, err := openClickHouseConn(c.nodeConnOptions(addr, pool))
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

// setQueryMemoryCaps records each class's per-query memory ceiling in bytes.
func (c *ClickHouseClient) setQueryMemoryCaps(caps map[string]int64) {
	c.queryMemoryCaps.Store(&caps)
}

// queryMemoryCap returns the class's per-query memory ceiling in bytes, or 0 when it
// is uncapped or reconcile has not succeeded.
func (c *ClickHouseClient) queryMemoryCap(name string) int64 {
	m := c.queryMemoryCaps.Load()
	if m == nil || *m == nil {
		return 0
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

// budgetKey carries a server-side execution ceiling for a query.
type budgetKey struct{}

// QueryBudgetContext attaches a server-side execution ceiling, in seconds, to
// every query run on ctx. A Go context deadline only stops the client reading:
// ClickHouse keeps executing the abandoned query, which is how a search the user
// gave up on lingers in system.processes burning CPU. max_execution_time is
// enforced by the server itself, so the scan dies even if this process does.
//
// clickhouse-go derives max_execution_time from a context deadline on its own, but
// only for queries that already carry settings -- a plain query on a deadline'd
// context (an unprovisioned search workload is enough to produce one) reaches
// ClickHouse with no ceiling at all. Setting it explicitly closes that gap.
func QueryBudgetContext(ctx context.Context, seconds int) context.Context {
	if seconds <= 0 {
		return ctx
	}
	return context.WithValue(ctx, budgetKey{}, seconds)
}

// contextQueryBudget returns the execution ceiling ctx carries, or 0.
func contextQueryBudget(ctx context.Context) int {
	sec, _ := ctx.Value(budgetKey{}).(int)
	return sec
}

// applyQuerySettings adds the settings ctx implies -- the workload tag when it is
// marked and that workload is provisioned, the class's per-query memory ceiling, and
// the execution ceiling when one was attached. Callers pass the settings map they were
// going to send anyway, since clickhouse-go's WithSettings replaces rather than merges.
//
// The memory ceiling does not depend on the workload being provisioned: it is enforced
// by the server per query, not by the scheduler, so it still holds for a class whose
// CPU share is off or whose workload DDL failed.
func (c *ClickHouseClient) applyQuerySettings(ctx context.Context, settings clickhouse.Settings) clickhouse.Settings {
	name := contextWorkload(ctx)
	tag := name != "" && c.workloadActive(name)
	var memCap int64
	if name != "" {
		memCap = c.queryMemoryCap(name)
	}
	budget := contextQueryBudget(ctx)
	if !tag && budget <= 0 && memCap <= 0 {
		return settings
	}
	if settings == nil {
		settings = clickhouse.Settings{}
	}
	if tag {
		settings["workload"] = name
	}
	if memCap > 0 {
		settings["max_memory_usage"] = memCap
	}
	if budget > 0 {
		settings["max_execution_time"] = budget
		settings["timeout_overflow_mode"] = "throw"
	}
	return settings
}
