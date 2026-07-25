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

// Interactive user searches run under a dedicated ClickHouse workload so a single
// expensive search cannot consume the machine and starve ingestion. Only queries
// explicitly tagged with this workload are scheduled: everything else (inserts,
// merges, mutations, alert evaluation) resolves to the 'default' workload, which is
// deliberately never created, and ClickHouse leaves unknown workloads unscheduled
// while throw_on_unknown_workload is false (the server default).
//
// Both limits are per node and shared across all running searches, so they bound
// what search costs the cluster in aggregate rather than what any one query costs.
const (
	// QuerySearchWorkload is the workload name user searches are tagged with.
	QuerySearchWorkload = "bifract_search"
	// QueryCPUResource and QueryMemoryResource are the scheduled resources. They are
	// global and may be shared with operator-defined workloads, so they are created
	// but never dropped.
	QueryCPUResource    = "cpu"
	QueryMemoryResource = "memory"
	// DefaultQueryCPUPercent is the share of each node's cores searches may use.
	DefaultQueryCPUPercent = 50
	// DefaultQueryMemoryPercent is the share of each node's ClickHouse memory budget
	// searches may reserve. Exceeding it kills the largest search in the workload
	// (MEMORY_RESERVATION_KILLED) rather than letting the server approach an OOM.
	DefaultQueryMemoryPercent = 50
	// MinQueryLimitPercent floors both settings so a mistype cannot leave searches
	// with a sliver of the machine.
	MinQueryLimitPercent = 10
)

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

// ReconcileQueryWorkload provisions the workload user searches run under, sized to
// cpuPercent of each node's cores and memPercent of its ClickHouse memory budget.
// Idempotent, so it runs at startup and again whenever an admin changes either
// setting; ClickHouse has no ALTER WORKLOAD, so an updated share is applied with
// CREATE OR REPLACE and takes effect for the next query.
//
// Both percentages at 0 disables scheduling entirely by dropping the workload. The
// resources are left in place: they are global, an operator may have their own
// workloads scheduling against them, and an unreferenced resource constrains nothing.
//
// Failure is not fatal to search. When provisioning does not succeed the client stops
// tagging queries, which returns them to unscheduled (pre-workload) behavior.
func (c *ClickHouseClient) ReconcileQueryWorkload(ctx context.Context, cpuPercent, memPercent int) error {
	cpuPercent = ClampQueryLimitPercent(cpuPercent)
	memPercent = ClampQueryLimitPercent(memPercent)
	onCluster := ""
	if c.IsCluster() {
		onCluster = " ON CLUSTER '" + EscCHStr(c.Cluster) + "'"
	}

	if cpuPercent == 0 && memPercent == 0 {
		c.searchWorkloadActive.Store(false)
		if err := c.execWorkloadDDL(ctx, fmt.Sprintf("DROP WORKLOAD IF EXISTS %s%s", QuerySearchWorkload, onCluster)); err != nil {
			return fmt.Errorf("drop query workload: %w", err)
		}
		log.Printf("[ClickHouse] Search resource limits disabled; user searches run unscheduled")
		return nil
	}

	var workloadSettings []string
	if cpuPercent > 0 {
		if err := c.execWorkloadDDL(ctx, fmt.Sprintf("CREATE RESOURCE IF NOT EXISTS %s%s (MASTER THREAD, WORKER THREAD)", QueryCPUResource, onCluster)); err != nil {
			c.searchWorkloadActive.Store(false)
			return fmt.Errorf("create cpu resource: %w", err)
		}
		workloadSettings = append(workloadSettings,
			fmt.Sprintf("max_concurrent_threads_ratio_to_cores = %.2f", float64(cpuPercent)/100))
	}
	if memPercent > 0 {
		if err := c.execWorkloadDDL(ctx, fmt.Sprintf("CREATE RESOURCE IF NOT EXISTS %s%s (MEMORY RESERVATION)", QueryMemoryResource, onCluster)); err != nil {
			c.searchWorkloadActive.Store(false)
			return fmt.Errorf("create memory resource: %w", err)
		}
		// max_memory is an absolute byte count, so it is derived from the server's own
		// budget rather than asked of the admin: the same percentage then means the
		// same thing on a laptop and on a 48GB node.
		budget, err := c.serverMemoryBudget(ctx)
		if err != nil {
			c.searchWorkloadActive.Store(false)
			return err
		}
		workloadSettings = append(workloadSettings,
			fmt.Sprintf("max_memory = %d", budget/100*int64(memPercent)))
	}

	// ClickHouse allows a single root workload. If an operator already defined one,
	// attach underneath it rather than failing on a second root.
	parent, err := c.existingWorkloadRoot(ctx)
	if err != nil {
		log.Printf("[ClickHouse] Warning: could not read workload hierarchy: %v", err)
	}

	ddl := searchWorkloadDDL(workloadSettings, onCluster, parent)
	if err := c.execWorkloadDDL(ctx, ddl); err != nil {
		c.searchWorkloadActive.Store(false)
		return fmt.Errorf("create query workload: %w", err)
	}

	c.searchWorkloadActive.Store(true)
	log.Printf("[ClickHouse] Search limits per node: CPU %s, memory %s (workload %q)",
		percentLabel(cpuPercent), percentLabel(memPercent), QuerySearchWorkload)
	return nil
}

// searchWorkloadDDL builds the CREATE OR REPLACE statement for the search workload.
// parent is the existing root workload to attach under, or "" for a new root.
func searchWorkloadDDL(workloadSettings []string, onCluster, parent string) string {
	in := ""
	if parent != "" && parent != QuerySearchWorkload {
		in = " IN " + parent
	}
	return fmt.Sprintf("CREATE OR REPLACE WORKLOAD %s%s%s SETTINGS %s",
		QuerySearchWorkload, onCluster, in, strings.Join(workloadSettings, ", "))
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

// existingWorkloadRoot returns the name of the current root workload (the one with no
// parent), or "" when the hierarchy is empty.
func (c *ClickHouseClient) existingWorkloadRoot(ctx context.Context) (string, error) {
	qctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	rows, err := c.Query(qctx, "SELECT name FROM system.workloads WHERE parent = '' LIMIT 1")
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", nil
	}
	name, _ := rows[0]["name"].(string)
	return name, nil
}

func (c *ClickHouseClient) execWorkloadDDL(ctx context.Context, stmt string) error {
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return c.conn.Exec(sctx, stmt)
}

// userSearchKey marks a context as belonging to an interactive user search.
type userSearchKey struct{}

// UserSearchContext marks ctx as an interactive user search. Query helpers tag such
// queries with the search workload, so the limits apply to searches without touching
// ingestion, alerting, or model queries that share the same helpers.
func UserSearchContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, userSearchKey{}, true)
}

// isUserSearch reports whether ctx was marked by UserSearchContext.
func isUserSearch(ctx context.Context) bool {
	v, _ := ctx.Value(userSearchKey{}).(bool)
	return v
}

// applyUserSearchWorkload adds the workload tag to settings when ctx is a user search
// and the workload is provisioned. Callers pass the settings map they were going to
// send anyway, since clickhouse-go's WithSettings replaces rather than merges.
func (c *ClickHouseClient) applyUserSearchWorkload(ctx context.Context, settings clickhouse.Settings) clickhouse.Settings {
	if !isUserSearch(ctx) || !c.searchWorkloadActive.Load() {
		return settings
	}
	if settings == nil {
		settings = clickhouse.Settings{}
	}
	settings["workload"] = QuerySearchWorkload
	return settings
}
