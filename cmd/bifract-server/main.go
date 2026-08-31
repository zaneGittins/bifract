package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	// The runtime image is FROM scratch and carries no /usr/share/zoneinfo, so
	// time.LoadLocation has no database to read. Per-user display timezones are
	// validated against this embedded copy.
	_ "time/tzdata"

	dbsql "bifract/db"
	"bifract/pkg/alerts"
	"bifract/pkg/apikeys"
	"bifract/pkg/archive"
	"bifract/pkg/auth"
	"bifract/pkg/chat"
	"bifract/pkg/comments"
	"bifract/pkg/contextlinks"
	"bifract/pkg/dashboards"
	"bifract/pkg/deeplink"
	"bifract/pkg/dictionaries"
	"bifract/pkg/feeds"
	"bifract/pkg/fractals"
	"bifract/pkg/groups"
	"bifract/pkg/ingest"
	"bifract/pkg/ingesttokens"
	"bifract/pkg/instructions"
	"bifract/pkg/maxmind"
	"bifract/pkg/metrics"
	"bifract/pkg/models"
	"bifract/pkg/normalizers"
	"bifract/pkg/notebooks"
	"bifract/pkg/notifications"
	"bifract/pkg/objstore"
	"bifract/pkg/oidc"
	"bifract/pkg/parser"
	"bifract/pkg/pgrcal"
	"bifract/pkg/prisms"
	"bifract/pkg/query"
	"bifract/pkg/queryhistory"
	"bifract/pkg/rbac"
	"bifract/pkg/savedqueries"
	"bifract/pkg/schemafields"
	"bifract/pkg/settings"
	"bifract/pkg/spool"
	"bifract/pkg/sse"
	"bifract/pkg/storage"

	// automemlimit derives GOMEMLIMIT from the container's cgroup so the GC works
	// harder as the process approaches the limit instead of being OOM-killed at it.
	// Matters most in ingest mode, which buffers up to 500K logs plus the archive
	// spool. No-op where no cgroup limit applies.
	_ "github.com/KimMachineGun/automemlimit"
	_ "go.uber.org/automaxprocs"
)

// Version is set at build time via -ldflags
var Version = "dev"

// fractalAccessAdapter adapts *rbac.Resolver to the HasFractalAccess interface
// used by chat and savedqueries handlers.
type fractalAccessAdapter struct {
	resolver *rbac.Resolver
}

func (a *fractalAccessAdapter) HasFractalAccess(ctx context.Context, user *storage.User, fractalID string) bool {
	if user == nil {
		return false
	}
	if user.IsAdmin {
		return true
	}
	role := a.resolver.ResolveRole(ctx, user, fractalID)
	return role != rbac.RoleNone
}

// logMemoryLimit reports the GOMEMLIMIT automemlimit derived from the container's
// cgroup. A deployment with no memory limit is worth surfacing: the GC then has no
// ceiling to work against and the container is OOM-killed rather than collecting.
func logMemoryLimit() {
	limit := debug.SetMemoryLimit(-1)
	if limit <= 0 || limit == math.MaxInt64 {
		log.Println("[Runtime] No container memory limit detected; GOMEMLIMIT unset")
		return
	}
	log.Printf("[Runtime] GOMEMLIMIT %d MiB (from cgroup)", limit>>20)
}

func main() {
	// Quick health probe for Docker HEALTHCHECK.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		resp, err := http.Get("http://localhost:8080/api/v1/health")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Logged before anything else so a support bundle always carries the exact
	// build, even when the image tag is a moving target like "latest".
	log.Printf("Bifract %s (%s/%s, %s)", Version, runtime.GOOS, runtime.GOARCH, runtime.Version())

	logMemoryLimit()

	// Ingest-only data-plane mode (`bifract-server ingest`), matching the archiver's
	// command-arg dispatch. No arg = the full server below (control plane + query + UI).
	if len(os.Args) > 1 && os.Args[1] == "ingest" {
		runIngestServer()
		return
	}

	// Load configuration from environment
	config := loadConfig()

	// Initialize PostgreSQL client
	pg := mustConnectPostgres(config)
	defer pg.Close()

	log.Println("Initializing PostgreSQL schema...")
	pgInitSQL := dbsql.PostgresSQL
	// If a pre-computed admin hash is provided (K8s/production), replace the default
	// hash in the init SQL so the admin user is created with the correct password
	// from the very first boot. No migration step needed.
	if adminHash := os.Getenv("BIFRACT_ADMIN_PASSWORD_HASH"); adminHash != "" {
		const defaultHash = "$2a$10$6qlugatnTUiTnVhThGK.l.g241wHWktjOAPykPJpHOh8RbxkApQvG"
		pgInitSQL = strings.Replace(pgInitSQL, defaultHash, adminHash, 1)
		pgInitSQL = strings.Replace(pgInitSQL, "TRUE,\n    TRUE\n)\nON CONFLICT", "TRUE,\n    FALSE\n)\nON CONFLICT", 1)
	}
	if err := pg.Initialize(context.Background(), pgInitSQL); err != nil {
		log.Fatalf("Failed to initialize PostgreSQL schema: %v", err)
	}
	log.Println("PostgreSQL schema ready")

	notifWriter := notifications.New(pg)
	notificationHandler := notifications.NewHandler(pg)
	log.Println("Health notification writer initialized")

	// Initialize ClickHouse clients (separate pools for ingest vs queries)
	db, dbIngest := mustConnectClickHouse(config)
	defer db.Close()
	defer dbIngest.Close()

	// Version floor before migrations, because migrations are what need it. A
	// pinned self-managed image below the floor is a fixable misconfiguration and
	// is fatal; a managed service below it means our floor is wrong rather than
	// their server, so that only warns. Failing to read the version never blocks
	// boot either way.
	if v, ok, err := db.CheckVersionFloor(context.Background()); err != nil {
		log.Printf("Warning: could not read ClickHouse version: %v", err)
	} else if !ok {
		if db.Topology().ManagedStorage {
			log.Printf("Warning: ClickHouse %s is below the tested minimum %s; this is a managed service, so the floor may simply be out of date", v, storage.MinClickHouseVersion)
		} else {
			log.Fatalf("ClickHouse %s is below the minimum supported version %s. Update the ClickHouse image and restart.", v, storage.MinClickHouseVersion)
		}
	} else {
		log.Printf("ClickHouse server version %s", v)
	}

	log.Println("Initializing ClickHouse schema...")
	if err := db.Initialize(context.Background(), dbsql.ClickHouseSQL, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		log.Fatalf("Failed to initialize ClickHouse schema: %v", err)
	}
	log.Println("ClickHouse schema ready")

	// Read-only capability probes. Everything else is recorded by the reconcilers
	// below, which perform the real operation.
	db.ProbeCapabilities(context.Background())

	// Native cold tiering has been replaced by the Iceberg archive. Migrate any
	// logs table still on the legacy 'tiered' storage policy back to the default
	// policy. No-op when tiering was never enabled. Must run while the tiered
	// policy/cold disk are still defined in config; safe to remove that config
	// only after this succeeds.
	if err := db.RevertTieredStoragePolicy(context.Background()); err != nil {
		log.Printf("Warning: failed to revert tiered storage policy (will retry next start): %v", err)
	}

	// Reclaim system.*_log_N tables stranded by past ClickHouse version upgrades.
	// Runs here rather than in the installer because a ClickHouse image bump strands
	// them on every deployment mode, and this is the one path all of them share.
	if err := db.DropOrphanedSystemLogTables(context.Background()); err != nil {
		log.Printf("Warning: failed to drop stranded system log tables (will retry next start): %v", err)
	}

	// Start hot table cleaner: drops expired logs_hot partitions every 5 minutes.
	hotCleanerCtx, hotCleanerCancel := context.WithCancel(context.Background())
	defer hotCleanerCancel()
	db.StartHotTableCleaner(hotCleanerCtx)
	log.Println("Hot table cleaner started")

	// Load custom schema fields from Postgres and reconcile ClickHouse schema.
	// SetCustomTypeHintedFields runs synchronously so the parser is ready before
	// the first query. ReconcileSchemaFields (MODIFY COLUMN) runs in the background
	// because it can block for minutes on large datasets.
	schemaFieldsManager := schemafields.NewManager(pg)
	if customFields, err := schemaFieldsManager.List(context.Background()); err != nil {
		log.Printf("Warning: Failed to load custom schema fields: %v", err)
	} else {
		customMap := make(map[string]bool, len(customFields))
		for _, f := range customFields {
			customMap[f.FieldName] = true
		}
		parser.SetCustomTypeHintedFields(customMap)
		allFields := append(append([]schemafields.SchemaField{}, schemafields.ProjectDefaultFields...), customFields...)
		go func() {
			// Wait for Initialize's migrations and cluster column sync to finish.
			// Reconciling concurrently with them raced the DDL that creates and
			// aligns the logs tables, so the MODIFY COLUMN could read a half-built
			// schema. The reconcile itself is metadata-only, so the wait costs
			// nothing but ordering.
			ctx := context.Background()
			db.WaitForSchemaReady(ctx)

			res, err := db.ReconcileSchemaFields(ctx, schemafields.ToSpecs(allFields))
			if err != nil {
				log.Printf("Warning: Schema field reconciliation failed: %v", err)
				// Leave sync_status alone: it is the UI's only signal that these
				// fields are not live, and overwriting it here would claim success.
				return
			}
			log.Println("Schema fields reconciled")
			// Record the real per-field outcome. Previously every custom field was
			// stamped active unconditionally, which reported success even for fields
			// whose skip index had failed to create.
			for _, f := range customFields {
				status, errMsg := schemafields.SyncStatusActive, ""
				if e, bad := res.IndexErrors[f.FieldName]; bad {
					status, errMsg = schemafields.SyncStatusError, e
				}
				if err := schemaFieldsManager.UpdateSyncStatus(ctx, f.FieldName, status, errMsg); err != nil {
					log.Printf("Warning: update schema field status %q: %v", f.FieldName, err)
				}
			}
		}()
	}

	// proc_lineage retention is decoupled from logs and kept long for DFIR (year-old
	// process trees). BIFRACT_PROC_LINEAGE_TTL_DAYS overrides the 365-day DDL default via
	// a metadata-only MODIFY TTL. Runs in the background; the table exists after init.
	if days := getEnvInt("BIFRACT_PROC_LINEAGE_TTL_DAYS", 0); days > 0 {
		go func() {
			if err := db.ReconcileProcLineageTTL(context.Background(), days); err != nil {
				log.Printf("Warning: proc_lineage TTL reconcile (%d days) failed: %v", days, err)
			} else {
				log.Printf("proc_lineage TTL set to %d days", days)
			}
		}()
	}

	// proc_freq is the aggregated pgr() frequency baseline; a longer window gives more
	// stable rarity. BIFRACT_PROC_FREQ_TTL_DAYS overrides the 180-day DDL default.
	if days := getEnvInt("BIFRACT_PROC_FREQ_TTL_DAYS", 0); days > 0 {
		go func() {
			if err := db.ReconcileProcFreqTTL(context.Background(), days); err != nil {
				log.Printf("Warning: proc_freq TTL reconcile (%d days) failed: %v", days, err)
			} else {
				log.Printf("proc_freq TTL set to %d days", days)
			}
		}()
	}

	// Initialize settings from database
	if err := settings.Init(pg); err != nil {
		log.Printf("Warning: Failed to initialize settings: %v", err)
	}
	log.Println("Settings initialized")

	// Reconcile the "Advanced endpoint analysis" MVs (proc_lineage/proc_freq) to the
	// persisted toggle before ingest starts, so the heavy per-insert triggers only fire
	// when the operator has opted in. Off by default; runs on every startup (idempotent).
	{
		eaEnabled := false
		if v, _ := pg.GetSetting(context.Background(), storage.AdvancedEndpointAnalysisSetting); v == "true" {
			eaEnabled = true
		}
		// Wait for schema work to finish first: in cluster mode migrations run in a
		// background goroutine, so reconciling too early could miss (and then race with)
		// an MV the migration is about to create attached.
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		db.WaitForSchemaReady(waitCtx)
		waitCancel()
		if err := db.ReconcileEndpointAnalysisMVs(context.Background(), eaEnabled); err != nil {
			log.Printf("Warning: failed to reconcile advanced endpoint analysis MVs: %v", err)
		}
	}

	// Ensure every logs materialized view runs with DEFINER privileges, then provision
	// the least-privilege ingest ClickHouse user. Together these let the ingest tier
	// insert (through the MVs) with no read access to log data, so a compromised ingest
	// container cannot exfiltrate logs. MV security runs unconditionally (harmless);
	// the user is provisioned only when the ingest password is configured. The ingest
	// tier tolerates the brief startup window before this completes via insert retry.
	if err := db.ReconcileMaterializedViewSecurity(context.Background()); err != nil {
		log.Printf("Warning: failed to reconcile materialized view security: %v", err)
	}
	if err := db.EnsureIngestUser(context.Background(), getEnv("BIFRACT_INGEST_CLICKHOUSE_PASSWORD", "")); err != nil {
		log.Printf("Warning: failed to ensure ingest ClickHouse user: %v", err)
	}
	if err := pg.EnsureIngestRole(context.Background(), getEnv("BIFRACT_INGEST_POSTGRES_PASSWORD", "")); err != nil {
		log.Printf("Warning: failed to ensure ingest Postgres role: %v", err)
	}

	// Bound each query class (interactive search, archive recall) to a share of each
	// node's CPU and memory so no one class can consume the machine and stall
	// ingestion. Non-fatal: on failure queries run unscheduled, as they did before.
	cur := settings.Get()
	if err := db.ReconcileQueryWorkloads(context.Background(), storage.WorkloadLimits{
		SearchCPUPercent:    cur.QueryCPUPercent,
		SearchMemoryPercent: cur.QueryMemoryPercent,
		RecallCPUPercent:    cur.RecallCPUPercent,
		RecallMemoryPercent: cur.RecallMemoryPercent,
	}); err != nil {
		// State the consequence, not just the failure: the cluster comes up healthy and
		// search works, so a bare "failed to reconcile" reads as cosmetic when it actually
		// means a single heavy query can now take the node down.
		log.Printf("Warning: query resource limits are NOT active, searches run with no CPU or memory ceiling: %v", err)
	}
	settings.RegisterQueryLimitsApplier(func(limits storage.WorkloadLimits) error {
		return db.ReconcileQueryWorkloads(context.Background(), limits)
	})
	// So the settings page reports a share the server has refused as unavailable,
	// instead of showing the stored percentage as though it were in force.
	settings.RegisterCapabilityReporter(db.Capabilities)

	// Initialize fractal management system
	log.Println("Initializing fractal management system...")
	fractalManager := fractals.NewManager(pg, db)
	log.Println("Fractal management system initialized")

	// Initialize prism manager
	prismManager := prisms.NewManager(pg)

	// Refresh fractal statistics on startup
	log.Println("Refreshing fractal statistics on startup...")
	if err := fractalManager.RefreshFractalStats(context.Background()); err != nil {
		log.Printf("Warning: Failed to refresh fractal stats on startup: %v", err)
	} else {
		log.Println("Fractal statistics refreshed successfully")
	}

	// Start background goroutine for periodic stats refresh
	go func() {
		ticker := time.NewTicker(10 * time.Minute) // Refresh every 10 minutes
		defer ticker.Stop()

		log.Println("Started background fractal statistics refresh (every 10 minutes)")

		for {
			select {
			case <-ticker.C:
				log.Println("Starting periodic fractal statistics refresh...")
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				if err := fractalManager.RefreshFractalStats(ctx); err != nil {
					log.Printf("Warning: Periodic fractal stats refresh failed: %v", err)
				} else {
					log.Println("Periodic fractal statistics refresh completed successfully")
				}
				cancel()
			}
		}
	}()

	// Start background goroutine for periodic retention enforcement
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		log.Println("Started background retention enforcement (every hour)")

		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			// Delete logs past each fractal's retention window. This is the sole
			// retention mechanism for the main logs table and bounds the local hot
			// store; the Iceberg archive (when enabled) retains full history.
			if err := fractalManager.EnforceRetention(ctx); err != nil {
				log.Printf("Warning: Retention enforcement failed: %v", err)
			}
			// Age out Recall search jobs so Postgres never accumulates result
			// blobs: drop the heavy payload after 24h, delete the row after 14d.
			if _, err := pg.Exec(ctx, `UPDATE archive_search_jobs SET results = NULL, field_order = NULL WHERE finished_at < NOW() - INTERVAL '24 hours' AND results IS NOT NULL`); err != nil {
				log.Printf("Warning: Recall results cleanup failed: %v", err)
			}
			if _, err := pg.Exec(ctx, `DELETE FROM archive_search_jobs WHERE created_at < NOW() - INTERVAL '14 days'`); err != nil {
				log.Printf("Warning: Recall job cleanup failed: %v", err)
			}
			cancel()
		}
	}()

	// Initialize dictionary manager (before alert engine so dict actions can be wired in)
	log.Println("Initializing dictionary manager...")
	dictionaryManager := dictionaries.NewManager(pg, db)
	dictionaryHandler := dictionaries.NewHandler(dictionaryManager, fractalManager)
	log.Println("Dictionary manager initialized")

	// Initialize analytics model manager
	log.Println("Initializing analytics model manager...")
	modelManager := models.NewManager(pg, db)
	modelHandler := models.NewHandler(modelManager, fractalManager)
	log.Println("Analytics model manager initialized")

	// Initialize MaxMind GeoIP (optional, enabled by MAXMIND_LICENSE_KEY)
	// Downloads happen in the background so they don't block server startup.
	var maxmindManager *maxmind.Manager
	if mmCfg := maxmind.LoadConfigFromEnv(); mmCfg != nil {
		log.Println("MaxMind GeoIP configured, will load databases in background...")
		maxmindManager = maxmind.NewManager(db, mmCfg)
	}

	// Initialize alert system with background cursor-based evaluation
	log.Println("Initializing alert system...")
	alertBaseURL := config.BaseURL
	if alertBaseURL == "" {
		// Derive from BIFRACT_DOMAIN if available (set by bifract)
		if domain := getEnv("BIFRACT_DOMAIN", ""); domain != "" {
			alertBaseURL = "https://" + domain
		} else {
			alertBaseURL = fmt.Sprintf("http://localhost:%d", config.Port)
		}
		log.Printf("BIFRACT_BASE_URL not set, derived alert link base: %s", alertBaseURL)
	}
	// Initialize normalizer system (before alerts, since alert manager uses it for Sigma translation)
	normalizerManager := normalizers.NewManager(pg)
	normalizerHandler := normalizers.NewHandler(normalizerManager, db)

	schemaFieldsHandler := schemafields.NewHandler(schemaFieldsManager, db, func(custom map[string]bool) {
		parser.SetCustomTypeHintedFields(custom)
	})

	// Measures the schema in the background: per-field distribution from a bounded
	// sample, storage and column capacity from part metadata, query usage from
	// saved BQL, and paths that have spilled past max_dynamic_paths. Advisory-locked
	// to one replica. The schema tab reads only what this writes, so no schema
	// query ever runs on a request.
	schemaSweeper := schemafields.NewSweeper(db, pg, schemaFieldsManager, notifWriter)
	schemaFieldsHandler.SetSweeper(schemaSweeper)
	schemaSweeper.Start(context.Background())

	alertEngine := alerts.NewEngineWithDicts(pg, db, dictionaryManager, alertBaseURL)
	alertEngine.SetModelManager(modelManager)
	alertEngine.SetNotificationWriter(notifWriter)
	alertManager := alerts.NewManager(pg, alertEngine, normalizerManager)
	// Let models own a backing alert (their detection query) via the alerts manager.
	modelManager.SetAlertManager(alertManager)

	if err := alertEngine.RefreshAlerts(context.Background()); err != nil {
		log.Printf("Warning: Failed to load initial alerts cache: %v", err)
	}

	alertEngine.Start()
	log.Printf("Alert system initialized (evaluation interval is admin-configurable via Limits settings)")

	// Scheduled model scorer (network analysis: beacon / long_connection). Idle
	// no-op on deployments with no network models: one cheap Postgres SELECT per
	// heartbeat and nothing else. Single-replica via a Postgres advisory lock.
	scorerEngine := models.NewScorerEngine(pg, db, modelManager)
	scorerEngine.Start(time.Duration(config.ModelScoreInterval) * time.Second)
	log.Printf("Model scorer initialized (heartbeat interval: %ds)", config.ModelScoreInterval)

	// Initialize ingest token system
	log.Println("Initializing ingest token system...")
	ingestTokenStorage := ingesttokens.NewStorage(pg)
	tokenCache := ingesttokens.NewTokenCache(60 * time.Second)
	ingestTokenHandler := ingesttokens.NewHandler(ingestTokenStorage, fractalManager, tokenCache)

	// Wire fractal create hook to auto-create default ingest token
	fractalManager.SetOnCreateHook(func(ctx context.Context, f *fractals.Fractal) {
		if _, _, err := ingestTokenStorage.CreateDefaultToken(ctx, f.ID, f.Name, f.CreatedBy); err != nil {
			log.Printf("[IngestTokens] failed to create default token for fractal %s: %v", f.Name, err)
		} else {
			log.Printf("[IngestTokens] created default ingest token for fractal %s", f.Name)
		}
	})

	// Ensure existing fractals have default tokens
	if err := ingestTokenStorage.EnsureDefaultTokens(context.Background(), fractalManager); err != nil {
		log.Printf("Warning: Failed to ensure default ingest tokens: %v", err)
	}
	log.Println("Ingest token system initialized")

	// Initialize per-fractal disk quota manager
	quotaManager := ingest.NewQuotaManager(pg, dbIngest)
	// Rollover enforcement runs only in the app tier (admin ClickHouse identity),
	// using the all-shards query client. It trims over-quota rollover fractals by
	// dropping whole oldest partitions. A Postgres advisory lock makes it a single
	// runner across app-tier replicas. The ingest tier never runs this: its
	// INSERT-only ClickHouse user cannot DROP PARTITION.
	quotaManager.StartRolloverSweep(db)

	// Initialize ingestion queue and handlers (uses dedicated ingest pool)
	log.Println("Initializing ingestion queue...")
	ingestQueue := ingest.NewIngestQueue(dbIngest, config.IngestQueueSize, config.IngestWorkers)
	ingestQueue.SetQuotaManager(quotaManager)
	ingestQueue.SetNotificationWriter(notifWriter)

	// Archive spool tee (dormant-but-present). The spool is provisioned whenever
	// BIFRACT_ARCHIVE_SPOOL_PATH is set (compose/k8s mounts the shared volume);
	// fresh installs leave it unset, so ingest is pure-hot with zero overhead.
	// The tee only spools when the runtime archive_enabled flag is on; toggling
	// it (admin UI, or the BIFRACT_ARCHIVE_ENABLED env seed) takes effect within
	// the poll interval, no restart. Uses only pkg/spool so the server binary
	// never links the archiver's Arrow/Iceberg dependencies.
	startArchiveSpool(ingestQueue, pg)

	// Recall/restore job queues. Hosted here because both only resolve Iceberg
	// metadata and hand the scan to ClickHouse: they have no spool affinity, and
	// running them in the always-on app tier means scaling ingest down cannot
	// leave archive jobs stuck at 'pending'.
	stopArchiveJobWorkers, recallEstimator := startArchiveJobWorkers(db.Topology())

	// Queue depth at which alert evaluation is deferred to protect ingestion.
	// Clamp the configured percentage to a sane (1, 100] range so a bad value
	// can't disable alerts (0%) or wedge them permanently above 100%.
	deferPct := config.AlertIngestDeferPct
	if deferPct < 1 {
		deferPct = 1
	} else if deferPct > 100 {
		deferPct = 100
	}
	alertDeferThreshold := config.IngestQueueSize * deferPct / 100
	if sysFractal, err := fractalManager.GetFractalByName(context.Background(), "system"); err == nil {
		ingestQueue.SetSystemFractalID(sysFractal.ID)
		log.Printf("Ingest queue system fractal wired: %s", sysFractal.ID)
	} else {
		log.Printf("Warning: could not resolve system fractal for ingest monitoring: %v", err)
	}

	// Model backfills yield to the same CPU/disk backpressure that gates ingestion,
	// then resume any backfill interrupted by a prior crash.
	modelManager.SetBackfillHealth(ingestQueue)
	// Recreate any model whose ClickHouse objects went missing (a log-data reset
	// drops them by design). Without this the scorer fails on every tick forever.
	modelManager.ReconcileCHObjects(context.Background())
	modelManager.RecoverBackfills(context.Background())

	ingestHandler := ingest.NewIngestHandler(ingestQueue, config.MaxBodySize, tokenCache, ingestTokenStorage)
	ingestHandler.SetQuotaManager(quotaManager)
	elasticHandler := ingest.NewElasticBulkHandler(ingestHandler)
	otlpHandler := ingest.NewOTLPHandler(ingestHandler)
	internalIngestHandler := ingest.NewInternalIngestHandler(ingestQueue, config.MaxBodySize, fractalManager, normalizerManager)

	// Wire ingest pressure signal to the alert engine so it defers evaluation
	// during heavy ingestion. Cursor-based tracking ensures no logs are missed.
	alertEngine.SetIngestPressureFunc(func() bool {
		return ingestQueue.Depth() > alertDeferThreshold
	})
	// The scorer yields to ingest pressure the same way the alert engine does, so a
	// scoring pass never competes with heavy ingestion for ClickHouse resources.
	scorerEngine.SetIngestPressureFunc(func() bool {
		return ingestQueue.Depth() > alertDeferThreshold
	})

	// Distribution queue monitor (cluster mode only) — polls system.distribution_queue
	// every 60s and writes ch.distribution.* events to the system fractal on health changes.
	distMonitor := storage.NewDistributionMonitor(dbIngest, pg, func(event string, fields map[string]string) {
		ingestQueue.WriteSystemEvent(event, fields)
		switch event {
		case "ch.distribution.broken_data":
			go notifWriter.Write("ch.distribution.broken_data", "critical",
				"ClickHouse Distribution: Broken Data Files",
				fmt.Sprintf("%s broken file(s) detected", fields["broken_data_files"]))
		case "ch.distribution.degraded":
			go notifWriter.Write("ch.distribution.degraded", "warning",
				"ClickHouse Distribution Queue Degraded",
				fmt.Sprintf("Error count: %s", fields["error_count"]))
		}
	})
	distMonitor.Start()

	// DDL queue monitor (cluster mode only) — polls system.distributed_ddl_queue
	// every 60s and fires ch.ddl_queue.* events on threshold crossings.
	ddlMonitor := storage.NewDDLMonitor(dbIngest, pg, func(event string, fields map[string]string) {
		switch event {
		case "ch.ddl_queue.critical":
			go notifWriter.Write("ch.ddl_queue.critical", "critical",
				"ClickHouse DDL Queue Critical",
				fmt.Sprintf("%s pending tasks", fields["pending"]))
		case "ch.ddl_queue.warning":
			go notifWriter.Write("ch.ddl_queue.warning", "warning",
				"ClickHouse DDL Queue Building Up",
				fmt.Sprintf("%s pending tasks", fields["pending"]))
		}
	})
	ddlMonitor.Start()

	// Rate limiter for ingestion endpoints
	rateLimiter := ingest.NewRateLimiter(float64(config.IngestRateLimit), config.IngestRateBurst)
	log.Printf("Ingestion ready (workers: %d, queue: %d, rate limit: %d req/s, body limit: %d bytes)",
		config.IngestWorkers, config.IngestQueueSize, config.IngestRateLimit, config.MaxBodySize)

	queryHandler := query.NewQueryHandlerFull(db, config.MaxQueryRows, fractalManager, dictionaryManager, prismManager)
	queryHandler.SetPostgresClient(pg)
	queryHandler.SetModelManager(modelManager)

	// pgr() severity calibration: accumulate the rendered score distribution so the cutoffs come
	// from what this deployment actually produces, not a hard-coded 0.9.
	pgrRecorder := pgrcal.NewRecorder(pg.DB())
	queryHandler.SetPgrRecorder(pgrRecorder)
	pgrRecorder.Start(context.Background())

	// Launch MaxMind background load after queryHandler exists
	if maxmindManager != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()
			if err := maxmindManager.LoadAllInitial(ctx); err != nil {
				log.Printf("Warning: MaxMind GeoIP initialization failed: %v", err)
				log.Println("GeoIP lookups will be unavailable until next daily refresh")
			} else {
				log.Println("MaxMind GeoIP databases loaded successfully")
				queryHandler.SetGeoIPEnabled(true)
			}
			maxmindManager.Start()
		}()
	}
	statusHandler := query.NewStatusHandler(db, pg)
	statusHandler.SetQuotaClearer(quotaManager)
	performanceHandler := query.NewPerformanceHandler(db, pg, getEnvInt("BIFRACT_METRICS_RETENTION_DAYS", 30), func(dedup, severity, title, body string) {
		go notifWriter.Write(dedup, severity, title, body)
	})
	settingsHandler := settings.NewHandler(pg)

	// Create API key handler and storage
	apiKeyHandler := apikeys.NewHandler(pg)
	apiKeyStorage := apikeys.NewStorage(pg)

	// Create API key validator adapter to avoid circular dependencies
	apiKeyValidator := &APIKeyValidatorAdapter{storage: apiKeyStorage}

	// Initialize auth handler with API key validation support
	authHandler := auth.NewAuthHandlerWithAPIKeys(pg, db, fractalManager, apiKeyValidator)

	// Initialize OIDC handler (optional, enabled by env vars)
	var oidcHandler *oidc.Handler
	if oidcConfig := oidc.LoadConfigFromEnv(); oidcConfig != nil {
		log.Println("Initializing OIDC authentication...")
		var err error
		oidcHandler, err = oidc.NewHandler(oidcConfig, pg, authHandler.CreateSessionForUser, authHandler.LogAuthEvent, authHandler.IsSecureCookies())
		if err != nil {
			log.Printf("Warning: OIDC initialization failed: %v", err)
			log.Println("OIDC authentication will be disabled")
		} else {
			log.Printf("OIDC authentication enabled (issuer: %s)", oidcConfig.IssuerURL)
		}
	}

	// Initialize fractal handler with auth support for session management
	prismHandler := prisms.NewHandler(prismManager, pg, authHandler)
	prismHandler.SetRBACResolver(authHandler.RBACResolver())
	fractalHandler := fractals.NewHandler(fractalManager, authHandler, prismManager)
	fractalHandler.SetRBAC(pg, authHandler.RBACResolver())
	deepLinkHandler := deeplink.NewHandler(fractalManager, prismManager, authHandler, authHandler.RBACResolver())

	// Wire RBAC into handlers that need per-fractal permission checks
	apiKeyHandler.SetRBAC(authHandler.RBACResolver())
	apiKeyHandler.SetPrismResolver(prismManager)
	ingestTokenHandler.SetRBAC(authHandler.RBACResolver())
	queryHandler.SetRBACResolver(authHandler.RBACResolver())

	// Groups handler (tenant admin only)
	groupHandler := groups.NewHandler(pg)

	commentHandler := comments.NewCommentHandlerWithFractals(pg, db, fractalManager, prismManager)
	notebookHandler := notebooks.NewNotebookHandler(pg, db, fractalManager, config.LiteLLMURL, config.LiteLLMMasterKey)
	notebookHandler.SetRBACResolver(authHandler.RBACResolver())
	dashboardHandler := dashboards.NewDashboardHandler(pg, fractalManager)
	dashboardHandler.SetRBACResolver(authHandler.RBACResolver())

	// SSE hub for live updates in notebooks and dashboards
	sseHub := sse.NewHub()
	notebookHandler.SetSSEHub(sseHub)
	dashboardHandler.SetSSEHub(sseHub)
	commentHandler.SetSSEHub(sseHub)

	// The hub only reaches clients on this process. Attach the Postgres relay so
	// collaborators spread across replicas still see each other's edits and
	// presence. A failure here is not fatal: single-replica deployments keep
	// working, multi-replica ones degrade to per-pod delivery with a loud log.
	sseRelay, err := sse.NewRelay(pg.DB(), pg.ConnString(), sseHub)
	if err != nil {
		log.Printf("[SSE] Cross-replica relay unavailable, live updates will not span replicas: %v", err)
	}

	// Background dashboard executor: presence-gated, backpressure-aware periodic
	// refresh of widgets for dashboards that currently have live viewers. It
	// yields to ingestion via the ingest queue's health signal.
	dashboardExecutor := dashboards.NewExecutor(pg, queryHandler, sseHub, ingestQueue, dashboards.ExecutorConfig{
		Tick:        time.Duration(getEnvInt("BIFRACT_DASHBOARD_TICK", 5)) * time.Second,
		MinInterval: time.Duration(getEnvInt("BIFRACT_DASHBOARD_MIN_REFRESH", 10)) * time.Second,
		Workers:     getEnvInt("BIFRACT_DASHBOARD_WORKERS", 4),
	})
	dashboardHandler.SetExecutor(dashboardExecutor)
	dashboardExecutor.Start()
	alertHandler := alerts.NewHandlerWithFractals(alertManager, fractalManager)
	alertHandler.SetRBACResolver(authHandler.RBACResolver())

	chatManager := chat.NewManager(pg, db, fractalManager, normalizerManager, config.LiteLLMURL, config.LiteLLMMasterKey)
	rbacAdapter := &fractalAccessAdapter{resolver: authHandler.RBACResolver()}
	chatHandler := chat.NewHandler(chatManager, fractalManager, rbacAdapter)
	chatHandler.SetPrismResolver(prismManager)
	savedQueryHandler := savedqueries.NewHandler(pg, fractalManager)
	savedQueryHandler.SetRBACResolver(rbacAdapter)
	savedQueryHandler.SetRBACFull(authHandler.RBACResolver())
	queryHistoryHandler := queryhistory.NewHandler(pg, fractalManager)
	queryHistoryHandler.SetRBACResolver(rbacAdapter)
	queryHistoryHandler.SetRBACFull(authHandler.RBACResolver())
	contextLinkManager := contextlinks.NewManager(pg)
	contextLinkHandler := contextlinks.NewHandler(contextLinkManager)

	// Initialize feed system
	feedManager := feeds.NewManager(pg)
	feedSyncer := feeds.NewSyncer(feedManager, alertManager, normalizerManager)
	feedHandler := feeds.NewHandler(feedManager, alertManager, fractalManager, feedSyncer)
	feedSyncer.Start()
	log.Println("Feed sync system initialized")

	// Initialize instruction library system
	instructionManager := instructions.NewManager(pg)
	instructionSyncer := instructions.NewSyncer(instructionManager)
	instructionHandler := instructions.NewHandler(instructionManager, fractalManager, instructionSyncer)
	instructionHandler.SetRBACResolver(authHandler.RBACResolver())
	instructionSyncer.Start()
	chatManager.SetInstructionManager(instructionManager)
	chatManager.SetPrismFractalResolver(prismManager)
	log.Println("Instruction library system initialized")

	r, _ := buildRouter(routerDeps{
		config:   config,
		pg:       pg,
		db:       db,
		dbIngest: dbIngest,

		fractalManager:  fractalManager,
		ingestQueue:     ingestQueue,
		rateLimiter:     rateLimiter,
		distMonitor:     distMonitor,
		ddlMonitor:      ddlMonitor,
		recallEstimator: recallEstimator,

		alertDeferThreshold: alertDeferThreshold,

		alertHandler:          alertHandler,
		apiKeyHandler:         apiKeyHandler,
		authHandler:           authHandler,
		chatHandler:           chatHandler,
		commentHandler:        commentHandler,
		contextLinkHandler:    contextLinkHandler,
		dashboardHandler:      dashboardHandler,
		deepLinkHandler:       deepLinkHandler,
		dictionaryHandler:     dictionaryHandler,
		elasticHandler:        elasticHandler,
		feedHandler:           feedHandler,
		fractalHandler:        fractalHandler,
		groupHandler:          groupHandler,
		ingestHandler:         ingestHandler,
		ingestTokenHandler:    ingestTokenHandler,
		instructionHandler:    instructionHandler,
		internalIngestHandler: internalIngestHandler,
		modelHandler:          modelHandler,
		normalizerHandler:     normalizerHandler,
		notebookHandler:       notebookHandler,
		notificationHandler:   notificationHandler,
		oidcHandler:           oidcHandler,
		otlpHandler:           otlpHandler,
		performanceHandler:    performanceHandler,
		prismHandler:          prismHandler,
		queryHandler:          queryHandler,
		queryHistoryHandler:   queryHistoryHandler,
		savedQueryHandler:     savedQueryHandler,
		schemaFieldsHandler:   schemaFieldsHandler,
		settingsHandler:       settingsHandler,
		statusHandler:         statusHandler,
	})

	// Prometheus metrics server (separate listen address, disabled by default).
	var metricsServer *metrics.Server
	if config.MetricsEnabled {
		collector := metrics.New(Version)
		collector.AttachIngest(ingestQueue)
		collector.AttachAlerts(alertEngine)
		metricsServer = metrics.NewServer(config.MetricsAddr, collector)
		metricsServer.Start()
	}

	// HTTP server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", config.Port),
		Handler:      r,
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 0, // Disabled for SSE/streaming; chi timeout middleware covers regular endpoints
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Starting Bifract server on port %d...", config.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	if sseRelay != nil {
		sseRelay.Close()
	}
	sseHub.Close()

	// Stop accepting new HTTP connections
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	// Stop the background dashboard executor (waits for in-flight refreshes)
	dashboardExecutor.Stop()

	// Stop claiming archive recall/restore jobs
	stopArchiveJobWorkers()

	// Stop the distribution queue and DDL monitors
	distMonitor.Stop()
	ddlMonitor.Stop()

	// Drain the ingestion queue (finish pending inserts)
	ingestQueue.Shutdown()

	// Stop the quota manager refresh loop
	quotaManager.Stop()

	// Stop the feed syncer
	feedSyncer.Stop()

	// Stop the instruction library syncer
	instructionSyncer.Stop()

	// Stop MaxMind refresh
	if maxmindManager != nil {
		maxmindManager.Stop()
	}

	// Stop the background metrics collector
	performanceHandler.StopCollector()

	// Stop the alert engine
	alertEngine.Stop()
	scorerEngine.Stop()

	// Stop metrics server
	if metricsServer != nil {
		metricsServer.Shutdown()
	}

	log.Println("Server stopped gracefully")
}

type Config struct {
	PostgresHost     string
	PostgresPort     int
	PostgresDB       string
	PostgresUser     string
	PostgresPassword string
	// CH is the whole ClickHouse contract; see storage.ClickHouseEnvFromOS.
	CH               storage.ClickHouseEnv
	Port             int
	MaxQueryRows     int
	LiteLLMURL       string
	LiteLLMMasterKey string
	// Ingestion queue
	IngestQueueSize int
	IngestWorkers   int
	MaxBodySize     int64

	// Rate limiting
	IngestRateLimit int
	IngestRateBurst int

	// Model scorer heartbeat (scheduled network models). Per-model rescore cadence
	// is derived from each model's window; this is just the tick granularity.
	ModelScoreInterval int // seconds
	// Percentage of ingest queue depth at which alert evaluation is deferred
	// to protect ingestion. Deferred alerts catch up via cursor tracking.
	AlertIngestDeferPct int

	// ClickHouse pool sizing (0 = use defaults)
	CHQueryMaxConns  int
	CHIngestMaxConns int

	// Base URL for external links (e.g. webhook alert_link)
	BaseURL string

	// CORS
	CORSOrigins string

	// Prometheus metrics (disabled by default)
	MetricsEnabled bool
	MetricsAddr    string
}

func loadConfig() Config {
	// Fatal, not degraded: a misconfigured storage backend has no safe fallback.
	chEnv, err := storage.ClickHouseEnvFromOS()
	if err != nil {
		log.Fatalf("ClickHouse configuration: %v", err)
	}

	config := Config{
		PostgresHost:     getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:     getEnvInt("POSTGRES_PORT", 5432),
		PostgresDB:       getEnv("POSTGRES_DB", "bifract"),
		PostgresUser:     getEnv("POSTGRES_USER", "bifract"),
		PostgresPassword: getEnv("POSTGRES_PASSWORD", "bifract"),
		CH:               chEnv,
		Port:             getEnvInt("BIFRACT_PORT", 8080),
		MaxQueryRows:     getEnvInt("BIFRACT_MAX_QUERY_ROWS", 10000),
		LiteLLMURL:       getEnv("LITELLM_URL", "http://litellm:8000"),
		LiteLLMMasterKey: getEnv("LITELLM_MASTER_KEY", ""),
		// Ingestion queue defaults
		IngestQueueSize: getEnvInt("BIFRACT_INGEST_QUEUE_SIZE", 100),
		IngestWorkers:   getEnvInt("BIFRACT_INGEST_WORKERS", 4),
		MaxBodySize:     int64(getEnvInt("BIFRACT_MAX_BODY_SIZE", 209715200)), // 200MB

		// Rate limiting defaults
		IngestRateLimit: getEnvInt("BIFRACT_INGEST_RATE_LIMIT", 10000),
		IngestRateBurst: getEnvInt("BIFRACT_INGEST_RATE_BURST", 20000),

		// Alert evaluation default (interval itself is admin-configurable via Limits settings)
		ModelScoreInterval:  getEnvInt("BIFRACT_MODEL_SCORE_INTERVAL", 600),
		AlertIngestDeferPct: getEnvInt("BIFRACT_ALERT_INGEST_DEFER_PCT", 25),

		// ClickHouse pool sizing (0 = use package defaults)
		CHQueryMaxConns:  getEnvInt("BIFRACT_CH_QUERY_MAX_CONNS", 0),
		CHIngestMaxConns: getEnvInt("BIFRACT_CH_INGEST_MAX_CONNS", 0),

		// Base URL
		BaseURL: getEnv("BIFRACT_BASE_URL", ""),

		// CORS
		CORSOrigins: getEnv("BIFRACT_CORS_ORIGINS", "http://localhost:8080,http://127.0.0.1:8080"),

		// Prometheus metrics
		MetricsEnabled: os.Getenv("BIFRACT_METRICS_ENABLED") == "true",
		MetricsAddr:    getEnv("BIFRACT_METRICS_ADDR", ":9090"),
	}

	log.Printf("Configuration loaded:")
	log.Printf("  PostgreSQL: %s:%d", config.PostgresHost, config.PostgresPort)
	log.Printf("  ClickHouse: %s", config.CH)
	log.Printf("  Server Port: %d", config.Port)
	log.Printf("  Max Query Rows: %d", config.MaxQueryRows)
	log.Printf("  LiteLLM URL: %s", config.LiteLLMURL)
	log.Printf("  CH Query Pool Max Conns: %d (0=default)", config.CHQueryMaxConns)
	log.Printf("  CH Ingest Pool Max Conns: %d (0=default)", config.CHIngestMaxConns)
	log.Printf("  Ingest Queue: %d slots, %d workers", config.IngestQueueSize, config.IngestWorkers)
	log.Printf("  Max Body Size: %d bytes", config.MaxBodySize)
	log.Printf("  Rate Limit: %d req/s (burst: %d)", config.IngestRateLimit, config.IngestRateBurst)
	log.Printf("  Alert Eval Interval: admin-configurable via Limits settings")
	log.Printf("  Model Score Interval: %ds", config.ModelScoreInterval)
	log.Printf("  Alert Ingest Defer: %d%% of queue depth", config.AlertIngestDeferPct)
	if config.MetricsEnabled {
		log.Printf("  Prometheus Metrics: %s/metrics", config.MetricsAddr)
	}

	return config
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			return v
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

// archiveEnabledSetting is the Postgres settings key that toggles archiving at
// runtime. Shared by the server tee and the archiver sidecar so they agree.
const archiveEnabledSetting = "archive_enabled"

// recallBytesHuman renders a byte count for the recall scan-limit rejection
// message (binary units, one decimal above KB).

// startArchiveJobWorkers runs the recall and restore queues. Both are dispatchers
// (resolve Iceberg metadata, issue an icebergS3()/icebergAzure() query, wait), so
// they cost a few idle goroutines here rather than a workload of their own.
//
// Skipped unless an object-storage backend is configured: ClickHouse cannot read
// the pod-local disk backend, so neither queue can do anything without one, and
// skipping keeps a hot-only install free of the claim polling entirely.
// Concurrency is capped globally at claim time, so every app replica running this
// does not multiply concurrent archive scans against ClickHouse.
// Returns a stop function that halts claiming and cancels in-flight jobs (a
// cancelled job is left 'running' and picked up by the stale reaper, exactly as
// after a pod kill) plus a scan Estimator for the Recall pre-flight endpoint,
// nil when no object-storage backend is configured.
// archiveCHReadBlocked explains why ClickHouse cannot read the archive, or is
// empty when it can. Set once during startup, read by the recall endpoint.
var archiveCHReadBlocked string

func startArchiveJobWorkers(topo storage.Topology) (func(), *archive.Estimator) {
	cfg, err := archive.ConfigFromEnv()
	if err != nil {
		log.Printf("Warning: archive job workers disabled, bad config: %v", err)
		return func() {}, nil
	}
	if cfg.Obj.Backend == objstore.BackendDisk || cfg.Obj.Backend == "" {
		return func() {}, nil
	}
	// Restore and recall are read by ClickHouse, not by us. A managed ClickHouse
	// has no route to a self-hosted object store, so those queues would claim jobs
	// and fail every one. Archive writing is unaffected: that goes through the Go
	// SDK from this pod, so a MinIO-backed install keeps archiving either way.
	if cfg.Obj.RequiresCustomEndpoint() && topo.ManagedStorage {
		archiveCHReadBlocked = fmt.Sprintf(
			"the archive uses a self-hosted object endpoint that a managed ClickHouse cannot reach (backend %q); archiving continues, restore and recall are unavailable",
			cfg.Obj.Backend)
		log.Printf("Warning: archive restore/recall disabled: %s", archiveCHReadBlocked)
		return func() {}, nil
	}
	db, err := sql.Open("postgres", cfg.PGDSN)
	if err != nil {
		log.Printf("Warning: archive job workers disabled, cannot open postgres: %v", err)
		return func() {}, nil
	}
	// Sized to the peak these queues can demand: each in-flight job uses one
	// connection for its own writes and one for its heartbeat goroutine, and a
	// claim loop holds one briefly while it waits on the claim lock. The recall
	// pool is the larger of the two (RecallWorkerPool loops, gated live), so size
	// against it plus the restore loops. Undersizing starves heartbeats, which the
	// stale reaper would eventually read as a dead worker and fail a healthy job.
	restoreN := cfg.JobConcurrency
	if restoreN < 1 {
		restoreN = 1
	}
	maxConns := 2*(archive.RecallWorkerPool+restoreN) + 4
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(4)

	ctx, cancel := context.WithCancel(context.Background())
	archive.StartJobWorkers(ctx, cfg, db)
	log.Printf("Archive job workers started (restore concurrency %d; recall pool %d, live cap via recall_concurrency setting)", restoreN, archive.RecallWorkerPool)
	return func() {
		cancel()
		db.Close()
	}, archive.NewEstimator(cfg)
}

// startArchiveSpool provisions the ingest archive spool tee when a spool path is
// configured, and starts a poller that keeps the runtime enable flag in sync
// with the archive_enabled setting (seeded by BIFRACT_ARCHIVE_ENABLED). No-op
// when unprovisioned, so archiving stays off by default.
func startArchiveSpool(q *ingest.IngestQueue, pg *storage.PostgresClient) {
	spoolPath := getEnv("BIFRACT_ARCHIVE_SPOOL_PATH", "")
	if spoolPath == "" {
		return
	}
	segBytes := getEnvInt64("BIFRACT_ARCHIVE_ROLL_BYTES", 128<<20)
	maxBytes := getEnvInt64("BIFRACT_ARCHIVE_SPOOL_MAX_BYTES", 10<<30)
	w, err := spool.NewWriter(spool.WriterOptions{Dir: spoolPath, MaxSegmentBytes: segBytes})
	if err != nil {
		log.Printf("Warning: archive spool disabled, cannot open %s: %v", spoolPath, err)
		return
	}
	q.SetSpool(w, maxBytes)

	envSeed := getEnvBool("BIFRACT_ARCHIVE_ENABLED", false)
	refresh := func() {
		enabled := envSeed
		if v, err := pg.GetSetting(context.Background(), archiveEnabledSetting); err == nil && v != "" {
			enabled = v == "true"
		}
		q.SetArchiveEnabled(enabled)
		// Apply an admin "clear archive spool" request. Only while archiving is
		// disabled (so nothing is spooling) and only when the global clear
		// generation is ahead of what this pod's spool last applied: reset the local
		// spool, then stamp the marker the co-located archiver waits on before it
		// resumes draining. This is the Writer side of the clear handshake; the
		// archiver (Reader) re-syncs off the marker.
		if !enabled {
			if gen := spoolClearGeneration(pg); gen > spool.ReadClearGeneration(spoolPath) {
				if err := w.Reset(); err != nil {
					log.Printf("Warning: clear archive spool failed: %v", err)
				} else if err := spool.WriteClearGeneration(spoolPath, gen); err != nil {
					log.Printf("Warning: clear archive spool marker write failed: %v", err)
				} else {
					log.Printf("Archive spool cleared (generation %d)", gen)
				}
			}
		}
		// Publish spool state so the app/UI tier can report archive provisioning and
		// usage even when (in a split deployment) the spool lives in this separate
		// ingest container.
		_ = pg.PublishSpoolStatus(context.Background(), storage.SpoolStatus{
			Provisioned: q.SpoolProvisioned(),
			UsedBytes:   q.SpoolUsageBytes(),
			MaxBytes:    q.SpoolMaxBytes(),
			Pressure:    q.SpoolPressure(),
		})
	}
	refresh()
	log.Printf("Archive spool provisioned at %s (max %d bytes, enabled=%v)", spoolPath, maxBytes, q.ArchiveEnabled())
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for range t.C {
			refresh()
		}
	}()
}

// spoolClearGeneration reads the current global clear-spool generation from
// settings (0 when unset). An admin action increments it to request that every
// ingest pod reset its spool.
func spoolClearGeneration(pg *storage.PostgresClient) int64 {
	v, err := pg.GetSetting(context.Background(), spool.ClearGenerationSettingKey)
	if err != nil || v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// APIKeyValidatorAdapter adapts apikeys.Storage to auth.APIKeyValidator interface
type APIKeyValidatorAdapter struct {
	storage *apikeys.Storage
}

func (a *APIKeyValidatorAdapter) ValidateAPIKey(ctx context.Context, key string) (*auth.ValidatedAPIKey, error) {
	keyData, err := a.storage.ValidateAPIKey(ctx, key)
	if err != nil {
		return nil, err
	}

	return &auth.ValidatedAPIKey{
		ID:          keyData.ID,
		Name:        keyData.Name,
		KeyID:       keyData.KeyID,
		FractalID:   keyData.FractalID,
		FractalName: keyData.FractalName,
		PrismID:     keyData.PrismID,
		PrismName:   keyData.PrismName,
		CreatedBy:   keyData.CreatedBy,
		Role:        keyData.Role,
		TenantAdmin: keyData.TenantAdmin,
	}, nil
}

func (a *APIKeyValidatorAdapter) UpdateLastUsed(ctx context.Context, keyID string) error {
	return a.storage.UpdateLastUsed(ctx, keyID)
}
