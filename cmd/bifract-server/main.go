package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	dbsql "bifract/db"
	"bifract/pkg/alerts"
	"bifract/pkg/apikeys"
	"bifract/pkg/auth"
	"bifract/pkg/chat"
	"bifract/pkg/comments"
	"bifract/pkg/contextlinks"
	"bifract/pkg/dashboards"
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
	"bifract/pkg/oidc"
	"bifract/pkg/parser"
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

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/google/uuid"
	_ "go.uber.org/automaxprocs"
)

// Version is set at build time via -ldflags
var Version = "dev"

// noDirFS wraps an http.FileSystem and refuses to open directories. This makes
// http.FileServer return 404 for a directory path rather than rendering an
// index listing that enumerates the contents of ./web.
type noDirFS struct{ fs http.FileSystem }

func (n noDirFS) Open(name string) (http.File, error) {
	f, err := n.fs.Open(name)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.IsDir() {
		f.Close()
		return nil, fs.ErrNotExist
	}
	return f, nil
}

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

func main() {
	// Quick health probe for Docker HEALTHCHECK.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		resp, err := http.Get("http://localhost:8080/api/v1/health")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}

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
	log.Println("Health notification writer initialized")

	// Initialize ClickHouse clients (separate pools for ingest vs queries)
	db, dbIngest := mustConnectClickHouse(config)
	defer db.Close()
	defer dbIngest.Close()

	log.Println("Initializing ClickHouse schema...")
	if err := db.Initialize(context.Background(), dbsql.ClickHouseSQL, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		log.Fatalf("Failed to initialize ClickHouse schema: %v", err)
	}
	log.Println("ClickHouse schema ready")

	// Native cold tiering has been replaced by the Iceberg archive. Migrate any
	// logs table still on the legacy 'tiered' storage policy back to the default
	// policy. No-op when tiering was never enabled. Must run while the tiered
	// policy/cold disk are still defined in config; safe to remove that config
	// only after this succeeds.
	if err := db.RevertTieredStoragePolicy(context.Background()); err != nil {
		log.Printf("Warning: failed to revert tiered storage policy (will retry next start): %v", err)
	}

	// Start hot table cleaner: drops expired logs_hot partitions every 5 minutes.
	hotCleanerCtx, hotCleanerCancel := context.WithCancel(context.Background())
	defer hotCleanerCancel()
	db.StartHotTableCleaner(hotCleanerCtx)
	log.Println("Hot table cleaner started")

	// Backfill the lower(raw_log) n-gram index on parts that predate it. Runs
	// asynchronously (alter_sync=0, advisory-locked to one replica) so the heavy
	// MATERIALIZE INDEX never blocks startup or trips the readiness probe.
	db.StartNormLogIndexBackfill(context.Background(), pg)

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

	// Watches for JSON paths that have spilled past max_dynamic_paths and lost
	// their own column. Advisory-locked to one replica and deliberately off the
	// request path: this is the one schema query that reads the JSON column, so
	// it is far more expensive than the sampled stats the tab renders from.
	schemafields.NewOverflowMonitor(db, pg, notifWriter).Start(context.Background())

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

	// Wire RBAC into handlers that need per-fractal permission checks
	apiKeyHandler.SetRBAC(authHandler.RBACResolver())
	apiKeyHandler.SetPrismResolver(prismManager)
	ingestTokenHandler.SetRBAC(authHandler.RBACResolver())
	queryHandler.SetRBACResolver(authHandler.RBACResolver())

	// Groups handler (tenant admin only)
	groupHandler := groups.NewHandler(pg)

	commentHandler := comments.NewCommentHandlerWithFractals(pg, db, fractalManager)
	notebookHandler := notebooks.NewNotebookHandler(pg, db, fractalManager, config.LiteLLMURL, config.LiteLLMMasterKey)
	notebookHandler.SetRBACResolver(authHandler.RBACResolver())
	dashboardHandler := dashboards.NewDashboardHandler(pg, fractalManager)
	dashboardHandler.SetRBACResolver(authHandler.RBACResolver())

	// SSE hub for live updates in notebooks and dashboards
	sseHub := sse.NewHub()
	notebookHandler.SetSSEHub(sseHub)
	dashboardHandler.SetSSEHub(sseHub)

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

	// Setup router
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	// Timeout middleware, bypassed for SSE and chat streaming endpoints.
	r.Use(func(next http.Handler) http.Handler {
		timeoutHandler := middleware.Timeout(60 * time.Second)(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/events") || strings.HasSuffix(r.URL.Path, "/stream") {
				next.ServeHTTP(w, r)
				return
			}
			timeoutHandler.ServeHTTP(w, r)
		})
	})

	// Security headers
	secureCookies := os.Getenv("BIFRACT_SECURE_COOKIES") == "true"
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("X-Frame-Options", "DENY")
			w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
			w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
			w.Header().Set("Content-Security-Policy",
				"default-src 'self'; "+
					"script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "+
					"style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "+
					"font-src 'self'; "+
					"img-src 'self' data: https://*.basemaps.cartocdn.com https://*.tile.openstreetmap.org; "+
					"connect-src 'self'")
			if secureCookies {
				w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
			}
			next.ServeHTTP(w, r)
		})
	})

	// Gzip compression for text-based responses (JS, CSS, HTML, JSON).
	// Chi's Compress middleware automatically skips streaming responses
	// (text/event-stream) and respects client Accept-Encoding headers.
	compressor := middleware.NewCompressor(5, // level 5: good balance of speed and ratio
		"text/html",
		"text/css",
		"text/plain",
		"text/javascript",
		"application/javascript",
		"application/json",
		"image/svg+xml",
	)
	r.Use(compressor.Handler)

	// CORS middleware
	corsOrigins := strings.Split(config.CORSOrigins, ",")
	for i := range corsOrigins {
		corsOrigins[i] = strings.TrimSpace(corsOrigins[i])
	}
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token", "X-API-Key", "X-SSE-Client-ID"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// API routes
	r.Route("/api/v1", func(r chi.Router) {
		// Ingestion route (token-authenticated, no session required)
		r.Group(func(r chi.Router) {
			r.Use(ingest.RateLimitMiddleware(rateLimiter))
			r.Post("/ingest", ingestHandler.HandleIngest)
		})

		// Internal ingestion route (private-network only, no token required)
		r.Group(func(r chi.Router) {
			r.Use(ingest.InternalOnlyMiddleware)
			r.Use(ingest.RateLimitMiddleware(rateLimiter))
			r.Post("/internal/ingest/{fractal}", internalIngestHandler.HandleInternalIngest)
		})

		// Public routes (no auth required)
		// Shared Links: anonymous, read-only dashboard access. Serves ONLY cached
		// widget results (never executes BQL) and is gated by the global
		// shared_links_enabled toggle. Still sits behind Caddy mTLS/IP controls,
		// which are enforced in front of this app on the listener. Because it is
		// unauthenticated and each hit does several DB reads plus a keep-warm
		// registration, it gets its own conservative per-IP rate limit (a
		// legitimate wallboard polls every 30s+, so this is generous).
		sharedLinkLimiter := ingest.NewRateLimiter(5, 20)
		r.With(ingest.RateLimitMiddleware(sharedLinkLimiter)).
			Get("/shared/{token}", dashboardHandler.HandleSharedDashboard)
		r.Post("/auth/login", authHandler.HandleLogin)
		r.Get("/auth/invite/validate", authHandler.HandleValidateInvite)
		r.Post("/auth/invite/accept", authHandler.HandleAcceptInvite)
		r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"healthy"}`))
		})
		// OIDC routes (public, no auth required)
		if oidcHandler != nil {
			r.Get("/auth/oidc/config", oidcHandler.HandleConfig)
			r.Get("/auth/oidc/login", oidcHandler.HandleLogin)
			r.Get("/auth/oidc/callback", oidcHandler.HandleCallback)
		} else {
			r.Get("/auth/oidc/config", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"enabled": false})
			})
		}

		// Protected routes (auth required)
		r.Group(func(r chi.Router) {
			r.Use(authHandler.AuthMiddleware)

			// Body size limit for non-ingest API endpoints (1MB)
			r.Use(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB
					next.ServeHTTP(w, r)
				})
			})

			// Version
			r.Get("/version", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{"version": Version})
			})

			// Query and status
			r.Post("/query", queryHandler.HandleQuery)
			r.Post("/query/stream", queryHandler.HandleQueryStream)
			r.Post("/query/fieldstats", queryHandler.HandleFieldStats)
			r.Post("/query/validate", queryHandler.HandleValidate)
			r.Get("/query/reference", queryHandler.HandleReference)
			r.Get("/query/fields", schemaFieldsHandler.HandleCatalog)
			r.Get("/logs/recent", queryHandler.HandleGetRecentLogs)
			r.Get("/logs/histogram", queryHandler.HandleGetRecentHistogram)
			r.Post("/logs/by-timestamp", queryHandler.HandleGetLogByTimestamp)
			r.Get("/logs/fields", queryHandler.HandleGetLogFields)
			r.Get("/status", statusHandler.HandleStatus)
			r.Get("/health/clickhouse", statusHandler.HandleHealthCheck)
			r.Get("/system/pressure", func(w http.ResponseWriter, r *http.Request) {
				alertsDeferred := ingestQueue.Depth() > alertDeferThreshold
				resp := map[string]interface{}{
					"alerts_deferred": alertsDeferred,
				}
				if dbIngest.IsCluster() {
					s := distMonitor.Stats()
					resp["distribution_queue"] = map[string]interface{}{
						"healthy":           s.Healthy,
						"data_files":        s.DataFiles,
						"broken_data_files": s.BrokenDataFiles,
						"error_count":       s.ErrorCount,
					}
					since, bucket := query.MetricRange(r.URL.Query().Get("range"))
					resp["distribution_queue_history"] = distMonitor.History(r.Context(), since, bucket)
					resp["ddl_queue_history"] = ddlMonitor.History(r.Context(), since, bucket)
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(resp)
			})

			// Health notifications
			notifHandler := notifications.NewHandler(pg)
			r.Get("/notifications", notifHandler.HandleList)
			r.Get("/notifications/count", notifHandler.HandleCount)
			r.Post("/notifications/read", notifHandler.HandleMarkRead)

			// Settings
			r.Get("/settings", settingsHandler.HandleGet)
			r.Post("/settings", settingsHandler.HandleUpdate)

			// Iceberg archive status + enable toggle (admin only).
			r.Get("/system/archive", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				enabled := false
				if v, _ := pg.GetSetting(r.Context(), "archive_enabled"); v == "true" {
					enabled = true
				}
				var updatedAt, lastCommit sql.NullTime
				var fractalCount int
				var totalBytes, totalRecords int64
				_ = pg.QueryRow(r.Context(),
					"SELECT updated_at, last_commit_at, fractal_count, total_bytes, total_records FROM archive_status WHERE id = 1").
					Scan(&updatedAt, &lastCommit, &fractalCount, &totalBytes, &totalRecords)

				// Maintenance (compaction + snapshot expiry) CronJob's last-run summary --
				// a separate freshness signal from the archiver heartbeat above, since
				// this is a periodic batch job rather than an always-on process. last_run_at
				// is only set on a successful pass; last_attempt_at covers every invocation
				// (crash or lock-contention/disabled skip included), which is what "is this
				// still on schedule" should key off -- otherwise a crashed or skipped run
				// looks identical to a healthy one that had nothing to do.
				var maintainLastRun, maintainLastAttempt, maintainRunRequestedAt sql.NullTime
				var maintainOutcome string
				var maintainError, maintainRunRequestedBy sql.NullString
				var maintainDurationMs int64
				var maintainTables, maintainCompacted, maintainGroupsFailed, maintainExpired int
				var maintainCandidateBytes, maintainCompactedBytes int64
				_ = pg.QueryRow(r.Context(),
					`SELECT last_run_at, last_attempt_at, last_outcome, last_error, duration_ms, tables_seen, compacted,
					        groups_failed, expired, candidate_bytes, compacted_bytes, run_requested_at, run_requested_by
					 FROM archive_maintain_status WHERE id = 1`).
					Scan(&maintainLastRun, &maintainLastAttempt, &maintainOutcome, &maintainError, &maintainDurationMs,
						&maintainTables, &maintainCompacted, &maintainGroupsFailed, &maintainExpired,
						&maintainCandidateBytes, &maintainCompactedBytes, &maintainRunRequestedAt, &maintainRunRequestedBy)
				maintainResp := map[string]interface{}{
					"outcome":         maintainOutcome,
					"duration_ms":     maintainDurationMs,
					"tables_seen":     maintainTables,
					"compacted":       maintainCompacted,
					"groups_failed":   maintainGroupsFailed,
					"expired":         maintainExpired,
					"candidate_bytes": maintainCandidateBytes,
					"compacted_bytes": maintainCompactedBytes,
					// on_schedule mirrors archiver_alive's freshness check above, sized to
					// the maintainer's scheduled cadence instead of the archiver's ~30s
					// heartbeat interval. false with no prior attempt at all (maintainOutcome
					// == "never") reads correctly as "not yet run" rather than "overdue".
					"on_schedule": maintainLastAttempt.Valid && time.Since(maintainLastAttempt.Time) < maintainStaleAfter,
					// A pending or in-progress "Run now": run_requested is the queued state
					// (claimed but not yet started, or waiting for the next poll); outcome
					// == "running" is the live pass. Lets the UI show/disable the button.
					"run_requested": maintainRunRequestedAt.Valid,
				}
				if maintainError.Valid {
					maintainResp["error"] = maintainError.String
				}
				if maintainRunRequestedBy.Valid {
					maintainResp["run_requested_by"] = maintainRunRequestedBy.String
				}
				addTimeIfValid(maintainResp, "last_run_at", maintainLastRun)
				addTimeIfValid(maintainResp, "last_attempt_at", maintainLastAttempt)

				// Recent-run history so the panel can show a trend (backlog shrinking,
				// growing, or runs being skipped) instead of only the latest data point.
				var maintainHistory []map[string]interface{}
				if rows, herr := pg.Query(r.Context(),
					`SELECT ran_at, outcome, duration_ms, tables_seen, compacted, groups_failed, expired,
					        candidate_bytes, compacted_bytes, error
					 FROM archive_maintain_history ORDER BY ran_at DESC LIMIT 10`); herr == nil {
					defer rows.Close()
					for rows.Next() {
						var ranAt sql.NullTime
						var outcome string
						var durationMs int64
						var tables, compacted, groupsFailed, expired int
						var candidateBytes, compactedBytes int64
						var runErr sql.NullString
						if err := rows.Scan(&ranAt, &outcome, &durationMs, &tables, &compacted, &groupsFailed,
							&expired, &candidateBytes, &compactedBytes, &runErr); err != nil {
							continue
						}
						entry := map[string]interface{}{
							"outcome":         outcome,
							"duration_ms":     durationMs,
							"tables_seen":     tables,
							"compacted":       compacted,
							"groups_failed":   groupsFailed,
							"expired":         expired,
							"candidate_bytes": candidateBytes,
							"compacted_bytes": compactedBytes,
						}
						if runErr.Valid {
							entry["error"] = runErr.String
						}
						addTimeIfValid(entry, "ran_at", ranAt)
						maintainHistory = append(maintainHistory, entry)
					}
				}
				maintainResp["history"] = maintainHistory
				// Prefer this process's own spool (single-container/full-server); in a
				// split deployment the spool lives in the ingest tier, so fall back to the
				// state it publishes to Postgres.
				provisioned := ingestQueue.SpoolProvisioned()
				usedBytes, maxBytes := ingestQueue.SpoolUsageBytes(), ingestQueue.SpoolMaxBytes()
				pressure := ingestQueue.SpoolPressure()
				if !provisioned {
					st := pg.ReadSpoolStatus(r.Context())
					provisioned, usedBytes, maxBytes, pressure = st.Provisioned, st.UsedBytes, st.MaxBytes, st.Pressure
				}
				resp := map[string]interface{}{
					"enabled":     enabled,
					"provisioned": provisioned,
					"backend":     getEnv("BIFRACT_ARCHIVE_BACKEND", "disk"),
					"spool": map[string]interface{}{
						"used_bytes": usedBytes,
						"max_bytes":  maxBytes,
						"pressure":   pressure,
					},
					"fractal_count":  fractalCount,
					"total_bytes":    totalBytes,
					"total_records":  totalRecords,
					"archiver_alive": updatedAt.Valid && time.Since(updatedAt.Time) < 90*time.Second,
					"maintain":       maintainResp,
				}
				addTimeIfValid(resp, "last_commit_at", lastCommit)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(resp)
			})
			r.Put("/system/archive/enabled", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				// Guardrail: archiving can only be enabled when the spool machinery
				// is provisioned (dormant-but-present after --upgrade). In a split
				// deployment the spool lives in the ingest tier, so also accept its
				// published provisioned state.
				if !ingestQueue.SpoolProvisioned() && !pg.ReadSpoolStatus(r.Context()).Provisioned {
					http.Error(w, "Archive not provisioned. Run bifract --upgrade to add the archiver, then retry.", http.StatusBadRequest)
					return
				}
				var body struct {
					Enabled bool `json:"enabled"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, "Invalid JSON", http.StatusBadRequest)
					return
				}
				val := "false"
				if body.Enabled {
					val = "true"
				}
				if err := pg.SetSetting(r.Context(), "archive_enabled", val); err != nil {
					http.Error(w, "Failed to save", http.StatusInternalServerError)
					return
				}
				// Reflect immediately in the running tee (the poller also refreshes).
				ingestQueue.SetArchiveEnabled(body.Enabled)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "enabled": body.Enabled})
			})
			// "Run now": request an out-of-schedule maintenance pass. The server does
			// not link the archiver's Iceberg stack, so it merely records the request
			// on the shared archive_maintain_status row (raw SQL, like /clear above);
			// the always-on maintain-loop claims it on its next poll and runs the pass.
			// Platform-agnostic: the maintainer is a container (Docker) or a replicas:1
			// Deployment (k8s), both polling this same Postgres row -- no k8s API access
			// or RBAC for the app tier.
			r.Post("/system/archive/maintain/run", func(w http.ResponseWriter, r *http.Request) {
				u, ok := r.Context().Value("user").(*storage.User)
				if !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				// Same provisioning guard as enabling: until the archive machinery is
				// provisioned (--upgrade) there is no maintainer to service the request.
				if !ingestQueue.SpoolProvisioned() && !pg.ReadSpoolStatus(r.Context()).Provisioned {
					http.Error(w, "Archive not provisioned. Run bifract --upgrade to add the archiver, then retry.", http.StatusBadRequest)
					return
				}
				// A run-now while archiving is disabled would be claimed and then
				// skipped (skipped_disabled), a confusing no-op; reject up front so the
				// button's outcome is predictable.
				if v, _ := pg.GetSetting(r.Context(), archiveEnabledSetting); v != "true" {
					http.Error(w, "Enable archiving before running maintenance.", http.StatusBadRequest)
					return
				}
				if _, err := pg.Exec(r.Context(),
					`UPDATE archive_maintain_status SET run_requested_at = NOW(), run_requested_by = $1 WHERE id = 1`,
					u.Username); err != nil {
					http.Error(w, "Failed to request maintenance run", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
			})
			// Clear the Iceberg catalog (admin only): resets the archive to zero so a
			// fractal can start fresh. The Iceberg catalog is Postgres-backed
			// (iceberg_tables + iceberg_namespace_properties), so deleting every row
			// for our archive namespace drops all per-fractal tables and the namespace
			// in one shot -- no Iceberg/object-store client needed, which keeps this
			// runnable from the server tier (that deliberately does not link the
			// archiver's Arrow/Iceberg stack) and is uniform across docker and k8s (a
			// single shared Postgres). We scope by table_namespace/namespace = the
			// archive namespace ("bifract"), NOT catalog_name: iceberg-go's SQL catalog
			// persists catalog_name as its own default ("sql"), so a catalog_name filter
			// would match nothing. Object-storage data files are NOT purged here; the
			// admin empties the bucket/container to reclaim space and to avoid orphaned
			// files shadowing recreated tables (see the UI warning).
			r.Post("/system/archive/clear", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				// Require archiving disabled so the archiver is not concurrently
				// recreating tables/namespaces mid-clear (races a clean reset to zero).
				if v, _ := pg.GetSetting(r.Context(), archiveEnabledSetting); v == "true" {
					http.Error(w, "Disable archiving before clearing the catalog.", http.StatusConflict)
					return
				}
				// archiveNamespace matches archive.Namespace (the single namespace every
				// fractal table lives under). All three writes run in one transaction so
				// a mid-clear failure never leaves orphaned namespace_properties rows or a
				// stale footprint; the whole reset either lands or rolls back.
				const archiveNamespace = "bifract"
				tx, err := pg.Begin(r.Context())
				if err != nil {
					http.Error(w, "Failed to clear catalog", http.StatusInternalServerError)
					return
				}
				defer tx.Rollback(r.Context())
				if _, err := tx.Exec(r.Context(), "DELETE FROM iceberg_tables WHERE table_namespace = $1", archiveNamespace); err != nil {
					http.Error(w, "Failed to clear catalog tables", http.StatusInternalServerError)
					return
				}
				if _, err := tx.Exec(r.Context(), "DELETE FROM iceberg_namespace_properties WHERE namespace = $1", archiveNamespace); err != nil {
					http.Error(w, "Failed to clear catalog namespaces", http.StatusInternalServerError)
					return
				}
				// Zero the footprint the admin UI shows; the archiver heartbeat keeps it
				// at zero until new data is archived.
				if _, err := tx.Exec(r.Context(), "UPDATE archive_status SET fractal_count = 0, total_bytes = 0, total_records = 0, updated_at = NOW() WHERE id = 1"); err != nil {
					http.Error(w, "Failed to reset archive status", http.StatusInternalServerError)
					return
				}
				if err := tx.Commit(r.Context()); err != nil {
					http.Error(w, "Failed to commit catalog clear", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
			})

			// Advanced endpoint analysis toggle (admin only): gates the process
			// lineage/frequency materialized views (heavy per-insert triggers).
			r.Get("/system/endpoint-analysis", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				enabled := false
				if v, _ := pg.GetSetting(r.Context(), storage.AdvancedEndpointAnalysisSetting); v == "true" {
					enabled = true
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"enabled": enabled})
			})
			r.Post("/system/endpoint-analysis", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				var body struct {
					Enabled bool `json:"enabled"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, "Invalid JSON", http.StatusBadRequest)
					return
				}
				// Attach/detach the MVs first; only persist the setting if that succeeds,
				// so the stored flag always matches the actual ClickHouse state.
				if err := db.ReconcileEndpointAnalysisMVs(r.Context(), body.Enabled); err != nil {
					log.Printf("endpoint-analysis reconcile failed: %v", err)
					http.Error(w, "Failed to apply change to ClickHouse", http.StatusInternalServerError)
					return
				}
				val := "false"
				if body.Enabled {
					val = "true"
				}
				if err := pg.SetSetting(r.Context(), storage.AdvancedEndpointAnalysisSetting, val); err != nil {
					http.Error(w, "Failed to save", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "enabled": body.Enabled})
			})

			// Shared Links global toggle (admin only): master switch for public,
			// no-auth, read-only dashboard access. Default off (opt-in). When off,
			// every anonymous /shared/{token} request 404s and new links cannot be
			// created; existing links can still be listed and revoked for cleanup.
			// Readable by any authenticated user: the dashboard UI needs it to decide
			// whether to show the "Share" button. It is a non-sensitive feature flag.
			r.Get("/system/shared-links", func(w http.ResponseWriter, r *http.Request) {
				enabled := false
				if v, _ := pg.GetSetting(r.Context(), storage.SharedLinksEnabledSetting); v == "true" {
					enabled = true
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"enabled": enabled})
			})
			r.Post("/system/shared-links", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				var body struct {
					Enabled bool `json:"enabled"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, "Invalid JSON", http.StatusBadRequest)
					return
				}
				val := "false"
				if body.Enabled {
					val = "true"
				}
				if err := pg.SetSetting(r.Context(), storage.SharedLinksEnabledSetting, val); err != nil {
					http.Error(w, "Failed to save", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "enabled": body.Enabled})
			})

			// Enqueue restore/reconcile jobs (async). The bifract-archiver run
			// process claims and executes them; this handler only writes the queue.
			r.Post("/system/archive/restore", func(w http.ResponseWriter, r *http.Request) {
				u, ok := r.Context().Value("user").(*storage.User)
				if !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				var body struct {
					FractalIDs []string `json:"fractal_ids"`
					From       string   `json:"from"`
					To         string   `json:"to"`
					Mode       string   `json:"mode"`
					Dedup      *bool    `json:"dedup"`
					// AcknowledgeRetention confirms the operator has been shown that
					// the restored window falls outside the fractal's retention and
					// will be deleted again. See retentionConflicts.
					AcknowledgeRetention bool `json:"acknowledge_retention"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, "Invalid JSON", http.StatusBadRequest)
					return
				}
				if len(body.FractalIDs) == 0 {
					http.Error(w, "Select at least one fractal", http.StatusBadRequest)
					return
				}
				if len(body.FractalIDs) > 200 {
					http.Error(w, "Too many fractals in one request", http.StatusBadRequest)
					return
				}
				from, err := parseArchiveTime(body.From)
				if err != nil {
					http.Error(w, "Invalid 'from' time", http.StatusBadRequest)
					return
				}
				to, err := parseArchiveTime(body.To)
				if err != nil {
					http.Error(w, "Invalid 'to' time", http.StatusBadRequest)
					return
				}
				if !to.After(from) {
					http.Error(w, "'to' must be after 'from'", http.StatusBadRequest)
					return
				}
				mode := body.Mode
				if mode == "" {
					mode = "restore"
				}
				if mode != "restore" && mode != "reconcile" {
					http.Error(w, "mode must be 'restore' or 'reconcile'", http.StatusBadRequest)
					return
				}
				dedup := true
				if body.Dedup != nil {
					dedup = *body.Dedup
				}

				// Restoring past a fractal's retention horizon puts rows back that
				// the hourly retention pass deletes again, typically within the
				// hour. Block it unless the operator has been told and said yes;
				// silently doing the work and throwing it away is the worst outcome.
				if !body.AcknowledgeRetention {
					conflicts, err := retentionConflicts(r.Context(), pg, body.FractalIDs, from)
					if err != nil {
						http.Error(w, "Failed to check fractal retention", http.StatusInternalServerError)
						return
					}
					if len(conflicts) > 0 {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusConflict)
						json.NewEncoder(w).Encode(map[string]interface{}{
							"error":                 "retention_conflict",
							"message":               "This window starts before the retention horizon of the fractal(s) below. Restored logs will be deleted again by the next retention pass. Raise retention first, or confirm to restore anyway.",
							"conflicts":             conflicts,
							"acknowledge_parameter": "acknowledge_retention",
						})
						return
					}
				}

				batchID := uuid.NewString()
				ids := make([]int64, 0, len(body.FractalIDs))
				for _, fid := range body.FractalIDs {
					if fid == "" {
						continue
					}
					var id int64
					err := pg.QueryRow(r.Context(),
						`INSERT INTO archive_restore_jobs (batch_id, fractal_id, mode, from_ts, to_ts, dedup, requested_by)
						 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
						batchID, fid, mode, from, to, dedup, u.Username).Scan(&id)
					if err != nil {
						http.Error(w, "Failed to enqueue restore job", http.StatusInternalServerError)
						return
					}
					ids = append(ids, id)
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": true, "batch_id": batchID, "job_ids": ids,
				})
			})

			// List recent restore jobs for the admin UI (newest first).
			// Archive completeness: recent (fractal, ingest day) comparisons of
			// hot-store vs archive row counts, gaps first. A gap means the hot store
			// holds rows the archive never received (see pkg/archive/completeness.go);
			// the reverse just means retention has aged those rows out of ClickHouse.
			r.Get("/system/archive/completeness", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				gapsOnly := r.URL.Query().Get("gaps_only") == "true"
				where := ""
				if gapsOnly {
					where = "WHERE c.ch_count > c.ice_count"
				}
				rows, err := pg.Query(r.Context(), `
					SELECT c.fractal_id, COALESCE(f.name, ''), c.ingest_day, c.ch_count, c.ice_count, c.checked_at
					FROM archive_completeness c
					LEFT JOIN fractals f ON f.id::text = c.fractal_id `+where+`
					ORDER BY (c.ch_count > c.ice_count) DESC, c.ingest_day DESC, f.name
					LIMIT 200`)
				if err != nil {
					http.Error(w, "Failed to load completeness", http.StatusInternalServerError)
					return
				}
				defer rows.Close()
				days := make([]map[string]interface{}, 0, 64)
				var totalGap int64
				for rows.Next() {
					var (
						fid, fname      string
						day, checkedAt  time.Time
						chCount, iceCnt int64
					)
					if err := rows.Scan(&fid, &fname, &day, &chCount, &iceCnt, &checkedAt); err != nil {
						continue
					}
					gap := int64(0)
					if chCount > iceCnt {
						gap = chCount - iceCnt
						totalGap += gap
					}
					days = append(days, map[string]interface{}{
						"fractal_id": fid, "fractal_name": fname,
						"ingest_day": day.UTC().Format("2006-01-02"),
						"ch_count":   chCount, "ice_count": iceCnt, "gap": gap,
						"checked_at": checkedAt.UTC(),
					})
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": true, "days": days, "total_gap": totalGap,
				})
			})

			r.Get("/system/archive/restore", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				// Pagination + optional status filter.
				q := r.URL.Query()
				page, _ := strconv.Atoi(q.Get("page"))
				if page < 1 {
					page = 1
				}
				pageSize, _ := strconv.Atoi(q.Get("page_size"))
				if pageSize < 1 {
					pageSize = 20
				}
				if pageSize > 100 {
					pageSize = 100
				}
				status := q.Get("status")
				validStatus := map[string]bool{"pending": true, "running": true, "succeeded": true, "failed": true, "canceled": true}
				where := ""
				var args []interface{}
				if validStatus[status] {
					where = "WHERE status = $1"
					args = append(args, status)
				}
				var total int
				if err := pg.QueryRow(r.Context(), "SELECT count(*) FROM archive_restore_jobs "+where, args...).Scan(&total); err != nil {
					http.Error(w, "Failed to load jobs", http.StatusInternalServerError)
					return
				}
				offset := (page - 1) * pageSize
				rows, err := pg.Query(r.Context(),
					`SELECT id, batch_id, fractal_id, mode, from_ts, to_ts, dedup, status,
					        target_rows, rows_restored, chunks_total, chunks_done, cursor_ts,
					        COALESCE(error, ''), COALESCE(requested_by, ''),
					        created_at, started_at, finished_at
					 FROM archive_restore_jobs `+where+
						fmt.Sprintf(" ORDER BY created_at DESC LIMIT %d OFFSET %d", pageSize, offset), args...)
				if err != nil {
					http.Error(w, "Failed to load jobs", http.StatusInternalServerError)
					return
				}
				defer rows.Close()
				jobs := make([]map[string]interface{}, 0, 32)
				for rows.Next() {
					var (
						id                         int64
						batchID, fid, mode, status string
						errMsg, reqBy              string
						from, to, created          time.Time
						started, finished, cursor  sql.NullTime
						target, restored           int64
						chunksTotal, chunksDone    int
						dedup                      bool
					)
					if err := rows.Scan(&id, &batchID, &fid, &mode, &from, &to, &dedup, &status,
						&target, &restored, &chunksTotal, &chunksDone, &cursor,
						&errMsg, &reqBy, &created, &started, &finished); err != nil {
						continue
					}
					j := map[string]interface{}{
						"id": id, "batch_id": batchID, "fractal_id": fid, "mode": mode,
						"from": from.UTC(), "to": to.UTC(), "dedup": dedup, "status": status,
						"target_rows": target, "rows_restored": restored,
						"chunks_total": chunksTotal, "chunks_done": chunksDone,
						"requested_by": reqBy, "created_at": created.UTC(),
					}
					if cursor.Valid {
						j["cursor_ts"] = cursor.Time.UTC()
					}
					if errMsg != "" {
						j["error"] = errMsg
					}
					if started.Valid {
						j["started_at"] = started.Time.UTC()
					}
					if finished.Valid {
						j["finished_at"] = finished.Time.UTC()
					}
					jobs = append(jobs, j)
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": true, "jobs": jobs, "total": total, "page": page, "page_size": pageSize,
				})
			})

			// Cancel a pending or running restore job. Moving the row out of
			// 'running' is the cancel signal: the owning worker notices on its next
			// heartbeat, issues KILL QUERY against the insert's query_id, and stops.
			// Rows already inserted stay put; re-running in dedup mode resumes safely.
			r.Post("/system/archive/restore/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				id := chi.URLParam(r, "id")
				res, err := pg.Exec(r.Context(),
					`UPDATE archive_restore_jobs SET status = 'canceled', finished_at = NOW(), updated_at = NOW()
					 WHERE id = $1 AND status IN ('pending', 'running')`, id)
				if err != nil {
					http.Error(w, "Failed to cancel", http.StatusInternalServerError)
					return
				}
				if n, _ := res.RowsAffected(); n == 0 {
					http.Error(w, "Job has already finished", http.StatusConflict)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
			})

			// Resume a failed or cancelled restore. cursor_ts survives on the row, so
			// requeueing picks up at the first unfinished ingest-day chunk instead of
			// replaying the whole window. Safe to use even if the cursor is stale:
			// restores run in dedup mode by default and are idempotent.
			r.Post("/system/archive/restore/{id}/resume", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil || !u.IsAdmin {
					http.Error(w, "Admin access required", http.StatusForbidden)
					return
				}
				id := chi.URLParam(r, "id")
				res, err := pg.Exec(r.Context(),
					`UPDATE archive_restore_jobs
					 SET status = 'pending', error = NULL, finished_at = NULL, started_at = NULL, updated_at = NOW()
					 WHERE id = $1 AND status IN ('failed', 'canceled')`, id)
				if err != nil {
					http.Error(w, "Failed to resume", http.StatusInternalServerError)
					return
				}
				if n, _ := res.RowsAffected(); n == 0 {
					http.Error(w, "Only a failed or canceled job can be resumed", http.StatusConflict)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
			})

			// Recall: per-fractal async BQL search over the Iceberg archive.
			// Analyst+ (unlike the admin-only Restore under /system/archive):
			// searching cold storage is a normal-user read, scoped by fractal RBAC.
			recallAnalystOK := func(w http.ResponseWriter, r *http.Request, fractalID string) (*storage.User, bool) {
				u, ok := r.Context().Value("user").(*storage.User)
				if !ok || u == nil {
					http.Error(w, "Authentication required", http.StatusUnauthorized)
					return nil, false
				}
				role, err := authHandler.RBACResolver().ResolveFractalRole(r.Context(), u.Username, fractalID)
				if err != nil || !rbac.HasAccess(u, role, rbac.RoleAnalyst) {
					http.Error(w, "Analyst access required", http.StatusForbidden)
					return nil, false
				}
				return u, true
			}

			// Whether Recall is available (archive enabled + spool provisioned).
			// Any authenticated user may check this so the tab can be gated for
			// analysts, who cannot read the admin-only /system/archive status.
			r.Get("/recall/available", func(w http.ResponseWriter, r *http.Request) {
				if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil {
					http.Error(w, "Authentication required", http.StatusUnauthorized)
					return
				}
				enabled := false
				if v, _ := pg.GetSetting(r.Context(), "archive_enabled"); v == "true" {
					enabled = true
				}
				// Provisioned in-process (full server) or in the split ingest tier.
				provisioned := ingestQueue.SpoolProvisioned() || pg.ReadSpoolStatus(r.Context()).Provisioned
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{
					"available": enabled && provisioned,
				})
			})

			// Submit a Recall search (returns the job id to poll).
			r.Post("/recall/{fractalID}", func(w http.ResponseWriter, r *http.Request) {
				fractalID := chi.URLParam(r, "fractalID")
				u, ok := recallAnalystOK(w, r, fractalID)
				if !ok {
					return
				}
				var body struct {
					Query   string `json:"query"`
					From    string `json:"from"`
					To      string `json:"to"`
					MaxRows int    `json:"max_rows"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, "Invalid JSON", http.StatusBadRequest)
					return
				}
				if strings.TrimSpace(body.Query) == "" {
					http.Error(w, "Query is required", http.StatusBadRequest)
					return
				}
				from, err := parseArchiveTime(body.From)
				if err != nil {
					http.Error(w, "Invalid 'from' time", http.StatusBadRequest)
					return
				}
				to, err := parseArchiveTime(body.To)
				if err != nil {
					http.Error(w, "Invalid 'to' time", http.StatusBadRequest)
					return
				}
				if !to.After(from) {
					http.Error(w, "'to' must be after 'from'", http.StatusBadRequest)
					return
				}
				if err := validateRecallQuery(body.Query); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				// Recall mirrors the query page's search UX: you narrow the range or
				// query rather than scroll. Default and hard-cap at 250 rows.
				maxRows := body.MaxRows
				if maxRows <= 0 {
					maxRows = 250
				}
				if maxRows > 250 {
					maxRows = 250
				}
				var inflight int
				if err := pg.QueryRow(r.Context(),
					`SELECT count(*) FROM archive_search_jobs WHERE requested_by = $1 AND status IN ('pending','running')`,
					u.Username).Scan(&inflight); err == nil && inflight >= 3 {
					http.Error(w, "Too many searches in progress; wait for one to finish", http.StatusTooManyRequests)
					return
				}
				var id int64
				if err := pg.QueryRow(r.Context(),
					`INSERT INTO archive_search_jobs (fractal_id, query, from_ts, to_ts, max_rows, requested_by)
					 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
					fractalID, body.Query, from, to, maxRows, u.Username).Scan(&id); err != nil {
					http.Error(w, "Failed to enqueue search", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "id": id})
			})

			// List recent Recall jobs for a fractal (newest first, no results payload).
			r.Get("/recall/{fractalID}", func(w http.ResponseWriter, r *http.Request) {
				fractalID := chi.URLParam(r, "fractalID")
				if _, ok := recallAnalystOK(w, r, fractalID); !ok {
					return
				}
				limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
				if limit < 1 || limit > 50 {
					limit = 20
				}
				rows, err := pg.Query(r.Context(),
					`SELECT id, query, from_ts, to_ts, status, row_count, is_aggregated, limit_hit,
					        read_rows, read_bytes, COALESCE(error, ''), created_at, started_at, finished_at
					 FROM archive_search_jobs WHERE fractal_id = $1 ORDER BY created_at DESC LIMIT $2`,
					fractalID, limit)
				if err != nil {
					http.Error(w, "Failed to list searches", http.StatusInternalServerError)
					return
				}
				defer rows.Close()
				jobs := make([]map[string]interface{}, 0, 32)
				for rows.Next() {
					var (
						id                    int64
						query, status, errMsg string
						from, to, created     time.Time
						started, finished     sql.NullTime
						rowCount              int64
						readRows, readBytes   int64
						isAgg, limitHit       bool
					)
					if err := rows.Scan(&id, &query, &from, &to, &status, &rowCount, &isAgg, &limitHit,
						&readRows, &readBytes, &errMsg, &created, &started, &finished); err != nil {
						continue
					}
					j := map[string]interface{}{
						"id": id, "query": query, "from": from.UTC(), "to": to.UTC(),
						"status": status, "row_count": rowCount, "is_aggregated": isAgg,
						"limit_hit": limitHit, "created_at": created.UTC(),
						"read_rows": readRows, "read_bytes": readBytes,
					}
					if errMsg != "" {
						j["error"] = errMsg
					}
					if started.Valid {
						j["started_at"] = started.Time.UTC()
					}
					if finished.Valid {
						j["finished_at"] = finished.Time.UTC()
					}
					jobs = append(jobs, j)
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "jobs": jobs})
			})

			// Fetch one Recall job with its results (inline render / reattach).
			r.Get("/recall/{fractalID}/{id}", func(w http.ResponseWriter, r *http.Request) {
				fractalID := chi.URLParam(r, "fractalID")
				if _, ok := recallAnalystOK(w, r, fractalID); !ok {
					return
				}
				id := chi.URLParam(r, "id")
				var (
					status, query, errMsg string
					rowCount              int64
					readRows, readBytes   int64
					isAgg, limitHit       bool
					fieldOrder, results   sql.NullString
					from, to, created     time.Time
					started, finished     sql.NullTime
				)
				err := pg.QueryRow(r.Context(),
					`SELECT query, from_ts, to_ts, status, row_count, is_aggregated, limit_hit,
					        read_rows, read_bytes,
					        field_order, results, COALESCE(error, ''), created_at, started_at, finished_at
					 FROM archive_search_jobs WHERE id = $1 AND fractal_id = $2`,
					id, fractalID).Scan(&query, &from, &to, &status, &rowCount, &isAgg, &limitHit,
					&readRows, &readBytes,
					&fieldOrder, &results, &errMsg, &created, &started, &finished)
				if err == sql.ErrNoRows {
					http.Error(w, "Search not found", http.StatusNotFound)
					return
				}
				if err != nil {
					http.Error(w, "Failed to load search", http.StatusInternalServerError)
					return
				}
				resp := map[string]interface{}{
					"success": true, "id": id, "query": query, "from": from.UTC(), "to": to.UTC(),
					"status": status, "row_count": rowCount, "is_aggregated": isAgg, "limit_hit": limitHit,
					"created_at": created.UTC(),
					"read_rows":  readRows, "read_bytes": readBytes,
				}
				if errMsg != "" {
					resp["error"] = errMsg
				}
				if started.Valid {
					resp["started_at"] = started.Time.UTC()
				}
				if finished.Valid {
					resp["finished_at"] = finished.Time.UTC()
				}
				if fieldOrder.Valid {
					resp["field_order"] = json.RawMessage(fieldOrder.String)
				}
				if results.Valid {
					resp["results"] = json.RawMessage(results.String)
				} else if status == "succeeded" {
					resp["results_expired"] = true
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(resp)
			})

			// Cancel a Recall job. Works while pending (never claimed) or running:
			// flipping the row out of 'running' is the signal the archiver worker's
			// watcher polls for -- it then kills the ClickHouse query. Terminal jobs
			// (succeeded/failed/canceled) return 409.
			r.Post("/recall/{fractalID}/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
				fractalID := chi.URLParam(r, "fractalID")
				if _, ok := recallAnalystOK(w, r, fractalID); !ok {
					return
				}
				id := chi.URLParam(r, "id")
				res, err := pg.Exec(r.Context(),
					`UPDATE archive_search_jobs SET status = 'canceled', finished_at = NOW(), updated_at = NOW()
					 WHERE id = $1 AND fractal_id = $2 AND status IN ('pending', 'running')`, id, fractalID)
				if err != nil {
					http.Error(w, "Failed to cancel", http.StatusInternalServerError)
					return
				}
				if n, _ := res.RowsAffected(); n == 0 {
					http.Error(w, "Search already finished", http.StatusConflict)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
			})

			// Auth
			r.Post("/auth/logout", authHandler.HandleLogout)
			r.Get("/auth/user", authHandler.HandleCurrentUser)
			r.Post("/auth/change-password", authHandler.HandleChangePassword)

			// Comments
			r.Post("/comments", commentHandler.HandleCreateComment)
			r.Get("/comments/flat", commentHandler.HandleGetFlatComments)
			r.Post("/comments/bulk-add-tag", commentHandler.HandleBulkAddTag)
			r.Post("/comments/bulk-remove-tag", commentHandler.HandleBulkRemoveTag)
			r.Post("/comments/bulk-delete", commentHandler.HandleBulkDeleteComments)
			r.Get("/comments/tags", commentHandler.HandleGetTags)
			r.Post("/comments/graph/log-fields", commentHandler.HandleGetLogFields)
			r.Get("/comments/{id}", commentHandler.HandleGetComment)
			r.Put("/comments/{id}", commentHandler.HandleUpdateComment)
			r.Delete("/comments/{id}", commentHandler.HandleDeleteComment)
			r.Get("/logs/{log_id}/comments", commentHandler.HandleGetLogComments)
			r.Delete("/logs/{log_id}/comments", commentHandler.HandleDeleteCommentsByLogID)
			r.Get("/logs/commented", commentHandler.HandleGetCommentedLogs)

			// Notebooks (API keys require "notebook" permission)
			r.Group(func(r chi.Router) {
				r.Use(auth.RequireAPIKeyPermission("notebook"))
				r.Get("/notebooks", notebookHandler.HandleListNotebooks)
				r.Post("/notebooks", notebookHandler.HandleCreateNotebook)
				r.Get("/notebooks/ai-status", notebookHandler.HandleAIStatus)
				r.Post("/notebooks/import", notebookHandler.HandleImportNotebook)
				r.Post("/notebooks/generate-from-comments", notebookHandler.HandleGenerateFromComments)
				r.Get("/notebooks/{id}", notebookHandler.HandleGetNotebook)
				r.Put("/notebooks/{id}", notebookHandler.HandleUpdateNotebook)
				r.Delete("/notebooks/{id}", notebookHandler.HandleDeleteNotebook)
				r.Post("/notebooks/{id}/sections", notebookHandler.HandleCreateSection)
				r.Put("/notebooks/{id}/sections/{section_id}", notebookHandler.HandleUpdateSection)
				r.Delete("/notebooks/{id}/sections/{section_id}", notebookHandler.HandleDeleteSection)
				r.Post("/notebooks/{id}/sections/{section_id}/execute", notebookHandler.HandleExecuteQuerySection)
				r.Post("/notebooks/{id}/sections/{section_id}/summarize", notebookHandler.HandleGenerateAISummary)
				r.Put("/notebooks/{id}/sections/{section_id}/results", notebookHandler.HandleUpdateSectionResults)
				r.Post("/notebooks/{id}/sections/reorder", notebookHandler.HandleReorderSections)
				r.Put("/notebooks/{id}/variables", notebookHandler.HandleUpdateVariables)
				r.Post("/notebooks/{id}/presence", notebookHandler.HandleUpdatePresence)
				r.Get("/notebooks/{id}/presence", notebookHandler.HandleGetPresence)
				r.Get("/notebooks/{id}/tags", notebookHandler.HandleGetNotebookTags)
				r.Get("/notebooks/{id}/export", notebookHandler.HandleExportNotebook)
				r.Get("/notebooks/{id}/events", notebookHandler.HandleSSE)
			})

			// Dashboards (API keys require "dashboard" permission)
			r.Group(func(r chi.Router) {
				r.Use(auth.RequireAPIKeyPermission("dashboard"))
				r.Get("/dashboards", dashboardHandler.HandleListDashboards)
				r.Post("/dashboards", dashboardHandler.HandleCreateDashboard)
				r.Get("/dashboards/{id}", dashboardHandler.HandleGetDashboard)
				r.Put("/dashboards/{id}", dashboardHandler.HandleUpdateDashboard)
				r.Delete("/dashboards/{id}", dashboardHandler.HandleDeleteDashboard)
				r.Post("/dashboards/{id}/widgets", dashboardHandler.HandleCreateWidget)
				r.Put("/dashboards/{id}/widgets/{widget_id}", dashboardHandler.HandleUpdateWidget)
				r.Put("/dashboards/{id}/widgets/{widget_id}/layout", dashboardHandler.HandleUpdateWidgetLayout)
				r.Delete("/dashboards/{id}/widgets/{widget_id}", dashboardHandler.HandleDeleteWidget)
				r.Put("/dashboards/{id}/variables", dashboardHandler.HandleUpdateVariables)
				r.Put("/dashboards/{id}/refresh-interval", dashboardHandler.HandleUpdateRefreshInterval)
				r.Post("/dashboards/{id}/execute", dashboardHandler.HandleExecuteDashboard)
				r.Post("/dashboards/{id}/widgets/{widget_id}/execute", dashboardHandler.HandleExecuteWidget)
				r.Post("/dashboards/{id}/presence", dashboardHandler.HandleUpdatePresence)
				r.Get("/dashboards/{id}/presence", dashboardHandler.HandleGetPresence)
				r.Get("/dashboards/{id}/export", dashboardHandler.HandleExportDashboard)
				r.Post("/dashboards/import", dashboardHandler.HandleImportDashboard)
				r.Get("/dashboards/{id}/events", dashboardHandler.HandleSSE)
				// Shared Links management (create/revoke require analyst+ on the
				// dashboard's scope; list is viewer+). The anonymous read route is
				// registered separately in the public block below.
				r.Get("/dashboards/{id}/shared-links", dashboardHandler.HandleListSharedLinks)
				r.Post("/dashboards/{id}/shared-links", dashboardHandler.HandleCreateSharedLink)
				r.Delete("/dashboards/{id}/shared-links/{link_id}", dashboardHandler.HandleRevokeSharedLink)
			})

			// Alert management (API keys require "alert_manage" permission)
			r.Group(func(r chi.Router) {
				r.Use(auth.RequireAPIKeyPermission("alert_manage"))
				r.Get("/alerts", alertHandler.HandleListAlerts)
				r.Post("/alerts", alertHandler.HandleCreateAlert)
				r.Get("/alerts/{id}", alertHandler.HandleGetAlert)
				r.Put("/alerts/{id}", alertHandler.HandleUpdateAlert)
				r.Delete("/alerts/{id}", alertHandler.HandleDeleteAlert)
				r.Post("/alerts/import", alertHandler.HandleImportYAML)
				r.Post("/alerts/batch-toggle", alertHandler.HandleBatchToggleAlerts)
				r.Get("/alerts/{id}/executions", alertHandler.HandleGetExecutions)
			})

			// Webhook management
			r.Get("/webhooks", alertHandler.HandleListWebhooks)
			r.Post("/webhooks", alertHandler.HandleCreateWebhook)
			r.Get("/webhooks/{id}", alertHandler.HandleGetWebhook)
			r.Put("/webhooks/{id}", alertHandler.HandleUpdateWebhook)
			r.Delete("/webhooks/{id}", alertHandler.HandleDeleteWebhook)
			r.Post("/webhooks/{id}/test", alertHandler.HandleTestWebhook)

			// Fractal action management
			r.Get("/fractal-actions", alertHandler.HandleListFractalActions)
			r.Post("/fractal-actions", alertHandler.HandleCreateFractalAction)
			r.Get("/fractal-actions/{id}", alertHandler.HandleGetFractalAction)
			r.Put("/fractal-actions/{id}", alertHandler.HandleUpdateFractalAction)
			r.Delete("/fractal-actions/{id}", alertHandler.HandleDeleteFractalAction)

			// Email action management
			r.Get("/email-actions", alertHandler.HandleListEmailActions)
			r.Post("/email-actions", alertHandler.HandleCreateEmailAction)
			r.Get("/email-actions/{id}", alertHandler.HandleGetEmailAction)
			r.Put("/email-actions/{id}", alertHandler.HandleUpdateEmailAction)
			r.Delete("/email-actions/{id}", alertHandler.HandleDeleteEmailAction)
			r.Post("/email-actions/{id}/test", alertHandler.HandleTestEmailAction)

			// SMTP settings
			r.Get("/smtp-settings", alertHandler.HandleGetSMTPSettings)
			r.Post("/smtp-settings", alertHandler.HandleUpdateSMTPSettings)

			// Prism management
			r.Get("/prisms", prismHandler.HandleListPrisms)
			r.Post("/prisms", prismHandler.HandleCreatePrism)
			r.Get("/prisms/{id}", prismHandler.HandleGetPrism)
			r.Put("/prisms/{id}", prismHandler.HandleUpdatePrism)
			r.Delete("/prisms/{id}", prismHandler.HandleDeletePrism)
			r.Post("/prisms/{id}/select", prismHandler.HandleSelectPrism)
			r.Post("/prisms/{id}/members", prismHandler.HandleAddMember)
			r.Delete("/prisms/{id}/members/{fractalID}", prismHandler.HandleRemoveMember)
			r.Get("/prisms/{id}/permissions", prismHandler.HandleListPrismPermissions)
			r.Post("/prisms/{id}/permissions", prismHandler.HandleGrantPrismPermission)
			r.Put("/prisms/{id}/permissions/{permId}", prismHandler.HandleUpdatePrismPermission)
			r.Delete("/prisms/{id}/permissions/{permId}", prismHandler.HandleRevokePrismPermission)

			// Fractal management
			r.Get("/fractals", fractalHandler.HandleListFractals)
			r.Post("/fractals", fractalHandler.HandleCreateFractal)
			r.Get("/fractals/{id}", fractalHandler.HandleGetFractal)
			r.Put("/fractals/{id}", fractalHandler.HandleUpdateFractal)
			r.Delete("/fractals/{id}", fractalHandler.HandleDeleteFractal)
			r.Post("/fractals/{id}/select", fractalHandler.HandleSelectFractal)
			r.Get("/fractals/{id}/stats", fractalHandler.HandleGetStats)
			r.Put("/fractals/{id}/retention", fractalHandler.HandleSetRetention)
			r.Put("/fractals/{id}/disk-quota", fractalHandler.HandleSetDiskQuota)
			r.Post("/fractals/stats/refresh", fractalHandler.HandleRefreshStats)

			// Fractal permissions (fractal admin or tenant admin, checked in handler)
			r.Get("/fractals/{id}/permissions", fractalHandler.HandleListPermissions)
			r.Post("/fractals/{id}/permissions", fractalHandler.HandleGrantPermission)
			r.Put("/fractals/{id}/permissions/{permId}", fractalHandler.HandleUpdatePermission)
			r.Delete("/fractals/{id}/permissions/{permId}", fractalHandler.HandleRevokePermission)

			// Groups (tenant admin only, checked in handler)
			r.Get("/groups", groupHandler.HandleListGroups)
			r.Post("/groups", groupHandler.HandleCreateGroup)
			r.Get("/groups/{id}", groupHandler.HandleGetGroup)
			r.Put("/groups/{id}", groupHandler.HandleUpdateGroup)
			r.Delete("/groups/{id}", groupHandler.HandleDeleteGroup)
			r.Get("/groups/{id}/members", groupHandler.HandleListMembers)
			r.Post("/groups/{id}/members", groupHandler.HandleAddMember)
			r.Delete("/groups/{id}/members/{username}", groupHandler.HandleRemoveMember)

			// API Key management (fractal-scoped)
			r.Get("/fractals/{id}/api-keys", apiKeyHandler.HandleListAPIKeys)
			r.Post("/fractals/{id}/api-keys", apiKeyHandler.HandleCreateAPIKey)
			r.Get("/fractals/{id}/api-keys/{keyId}", apiKeyHandler.HandleGetAPIKey)
			r.Put("/fractals/{id}/api-keys/{keyId}", apiKeyHandler.HandleUpdateAPIKey)
			r.Delete("/fractals/{id}/api-keys/{keyId}", apiKeyHandler.HandleDeleteAPIKey)
			r.Post("/fractals/{id}/api-keys/{keyId}/toggle", apiKeyHandler.HandleToggleAPIKey)

			// API Key management (prism-scoped)
			r.Get("/prisms/{id}/api-keys", apiKeyHandler.HandleListPrismAPIKeys)
			r.Post("/prisms/{id}/api-keys", apiKeyHandler.HandleCreatePrismAPIKey)
			r.Get("/prisms/{id}/api-keys/{keyId}", apiKeyHandler.HandleGetPrismAPIKey)
			r.Put("/prisms/{id}/api-keys/{keyId}", apiKeyHandler.HandleUpdatePrismAPIKey)
			r.Delete("/prisms/{id}/api-keys/{keyId}", apiKeyHandler.HandleDeletePrismAPIKey)
			r.Post("/prisms/{id}/api-keys/{keyId}/toggle", apiKeyHandler.HandleTogglePrismAPIKey)

			// Ingest Token management
			r.Get("/fractals/{id}/ingest-tokens", ingestTokenHandler.HandleListTokens)
			r.Post("/fractals/{id}/ingest-tokens", ingestTokenHandler.HandleCreateToken)
			r.Get("/fractals/{id}/ingest-tokens/{tokenId}", ingestTokenHandler.HandleGetToken)
			r.Put("/fractals/{id}/ingest-tokens/{tokenId}", ingestTokenHandler.HandleUpdateToken)
			r.Delete("/fractals/{id}/ingest-tokens/{tokenId}", ingestTokenHandler.HandleDeleteToken)
			r.Post("/fractals/{id}/ingest-tokens/{tokenId}/toggle", ingestTokenHandler.HandleToggleToken)

			// Dictionaries
			r.Get("/dictionaries", dictionaryHandler.HandleListDictionaries)
			r.Post("/dictionaries", dictionaryHandler.HandleCreateDictionary)
			r.Get("/dictionaries/{id}", dictionaryHandler.HandleGetDictionary)
			r.Put("/dictionaries/{id}", dictionaryHandler.HandleUpdateDictionary)
			r.Delete("/dictionaries/{id}", dictionaryHandler.HandleDeleteDictionary)
			r.Get("/dictionaries/{id}/data", dictionaryHandler.HandleGetRows)
			r.Post("/dictionaries/{id}/data", dictionaryHandler.HandleUpsertRows)
			r.Delete("/dictionaries/{id}/data/{key}", dictionaryHandler.HandleDeleteRow)
			r.Post("/dictionaries/{id}/import", dictionaryHandler.HandleImportCSV)
			r.Get("/dictionaries/{id}/export", dictionaryHandler.HandleExportCSV)
			r.Post("/dictionaries/{id}/columns", dictionaryHandler.HandleAddColumn)
			r.Delete("/dictionaries/{id}/columns/{name}", dictionaryHandler.HandleRemoveColumn)
			r.Post("/dictionaries/{id}/columns/{name}/key", dictionaryHandler.HandleSetColumnKey)
			r.Delete("/dictionaries/{id}/columns/{name}/key", dictionaryHandler.HandleUnsetColumnKey)
			r.Post("/dictionaries/{id}/reload", dictionaryHandler.HandleReloadDictionary)

			// Analytics models
			r.Get("/models", modelHandler.HandleList)
			r.Post("/models", modelHandler.HandleCreate)
			r.Post("/models/test-extraction", modelHandler.HandleTestExtraction)
			r.Post("/models/generate-query", modelHandler.HandleGenerateQuery)
			r.Post("/models/parse-query", modelHandler.HandleParseQuery)
			r.Post("/models/preview", modelHandler.HandlePreview)
			r.Post("/models/import", modelHandler.HandleImport)
			r.Get("/models/{id}", modelHandler.HandleGet)
			r.Put("/models/{id}", modelHandler.HandleUpdate)
			r.Delete("/models/{id}", modelHandler.HandleDelete)
			r.Get("/models/{id}/data", modelHandler.HandleGetData)
			r.Get("/models/{id}/stats", modelHandler.HandleGetStats)
			r.Get("/models/{id}/histogram", modelHandler.HandleGetHistogram)
			r.Get("/models/{id}/export", modelHandler.HandleExport)
			r.Post("/models/{id}/enable-alert", modelHandler.HandleEnableAlert)
			r.Post("/models/{id}/disable-alert", modelHandler.HandleDisableAlert)
			r.Post("/models/{id}/backfill", modelHandler.HandleStartBackfill)
			r.Post("/models/{id}/backfill/cancel", modelHandler.HandleCancelBackfill)

			// Dictionary actions (for alerts)
			r.Get("/dictionary-actions", dictionaryHandler.HandleListDictionaryActions)
			r.Post("/dictionary-actions", dictionaryHandler.HandleCreateDictionaryAction)
			r.Get("/dictionary-actions/{id}", dictionaryHandler.HandleGetDictionaryAction)
			r.Put("/dictionary-actions/{id}", dictionaryHandler.HandleUpdateDictionaryAction)
			r.Delete("/dictionary-actions/{id}", dictionaryHandler.HandleDeleteDictionaryAction)

			// Saved Queries
			r.Get("/saved-queries", savedQueryHandler.HandleList)
			r.Post("/saved-queries", savedQueryHandler.HandleCreate)
			r.Put("/saved-queries/{id}", savedQueryHandler.HandleUpdate)
			r.Delete("/saved-queries/{id}", savedQueryHandler.HandleDelete)
			r.Post("/saved-queries/{id}/use", savedQueryHandler.HandleMarkUsed)
			r.Post("/saved-queries/{id}/favorite", savedQueryHandler.HandleFavorite)
			r.Delete("/saved-queries/{id}/favorite", savedQueryHandler.HandleUnfavorite)

			r.Get("/query-history", queryHistoryHandler.HandleList)
			r.Post("/query-history", queryHistoryHandler.HandleRecord)
			r.Delete("/query-history", queryHistoryHandler.HandleClear)
			r.Delete("/query-history/{id}", queryHistoryHandler.HandleDelete)

			// Chat
			r.Get("/chat/conversations", chatHandler.HandleListConversations)
			r.Post("/chat/conversations", chatHandler.HandleCreateConversation)
			r.Patch("/chat/conversations/{id}", chatHandler.HandleRenameConversation)
			r.Delete("/chat/conversations/{id}", chatHandler.HandleDeleteConversation)
			r.Get("/chat/conversations/{id}/messages", chatHandler.HandleGetMessages)
			r.Delete("/chat/conversations/{id}/messages", chatHandler.HandleClearMessages)
			r.Post("/chat/conversations/{id}/stream", chatHandler.HandleStream)
			r.Patch("/chat/conversations/{id}/libraries", chatHandler.HandleSetConversationLibraries)
			r.Get("/chat/conversations/{id}/libraries", chatHandler.HandleGetConversationLibraries)
			r.Delete("/chat/conversations", chatHandler.HandleDeleteAllConversations)
			r.Get("/chat/instructions", chatHandler.HandleListInstructions)
			r.Post("/chat/instructions", chatHandler.HandleCreateInstruction)
			r.Put("/chat/instructions/{instructionId}", chatHandler.HandleUpdateInstruction)
			r.Delete("/chat/instructions/{instructionId}", chatHandler.HandleDeleteInstruction)

			// Context Links (enabled endpoint for all users, CRUD admin-checked in handler)
			r.Get("/context-links/enabled", contextLinkHandler.HandleListEnabled)
			r.Get("/context-links", contextLinkHandler.HandleList)
			r.Post("/context-links", contextLinkHandler.HandleCreate)
			r.Get("/context-links/{id}", contextLinkHandler.HandleGet)
			r.Put("/context-links/{id}", contextLinkHandler.HandleUpdate)
			r.Delete("/context-links/{id}", contextLinkHandler.HandleDelete)

			// Alert Feeds
			// Instruction Libraries
			r.Get("/instruction-libraries", instructionHandler.HandleListLibraries)
			r.Get("/instruction-libraries/ensure-default", instructionHandler.HandleEnsureDefaultLibrary)
			r.Post("/instruction-libraries", instructionHandler.HandleCreateLibrary)
			r.Get("/instruction-libraries/{id}", instructionHandler.HandleGetLibrary)
			r.Put("/instruction-libraries/{id}", instructionHandler.HandleUpdateLibrary)
			r.Delete("/instruction-libraries/{id}", instructionHandler.HandleDeleteLibrary)
			r.Get("/instruction-libraries/{id}/pages", instructionHandler.HandleListPages)
			r.Post("/instruction-libraries/{id}/pages", instructionHandler.HandleCreatePage)
			r.Get("/instruction-libraries/{id}/pages/{pageId}", instructionHandler.HandleGetPage)
			r.Get("/instruction-libraries/{id}/pages/{pageId}/backlinks", instructionHandler.HandleGetBacklinks)
			r.Put("/instruction-libraries/{id}/pages/{pageId}", instructionHandler.HandleUpdatePage)
			r.Patch("/instruction-libraries/{id}/pages/{pageId}/move", instructionHandler.HandleMovePage)
			r.Get("/instruction-libraries/{id}/folders", instructionHandler.HandleListFolders)
			r.Post("/instruction-libraries/{id}/folders", instructionHandler.HandleCreateFolder)
			r.Put("/instruction-libraries/{id}/folders/{folderId}", instructionHandler.HandleUpdateFolder)
			r.Delete("/instruction-libraries/{id}/folders/{folderId}", instructionHandler.HandleDeleteFolder)
			r.Delete("/instruction-libraries/{id}/pages/{pageId}", instructionHandler.HandleDeletePage)
			r.Post("/instruction-libraries/{id}/sync", instructionHandler.HandleSyncLibrary)

			r.Get("/feeds", feedHandler.HandleListFeeds)
			r.Post("/feeds", feedHandler.HandleCreateFeed)
			r.Get("/feeds/{id}", feedHandler.HandleGetFeed)
			r.Put("/feeds/{id}", feedHandler.HandleUpdateFeed)
			r.Delete("/feeds/{id}", feedHandler.HandleDeleteFeed)
			r.Post("/feeds/{id}/sync", feedHandler.HandleSyncFeed)
			r.Get("/feeds/{id}/alerts", feedHandler.HandleGetFeedAlerts)
			r.Post("/feeds/{id}/alerts/enable-all", feedHandler.HandleEnableAllAlerts)
			r.Post("/feeds/{id}/alerts/disable-all", feedHandler.HandleDisableAllAlerts)
			r.Get("/alerts/feed", feedHandler.HandleListAllFeedAlerts)
			r.Post("/alerts/feed/batch-toggle", alertHandler.HandleBatchToggleFeedAlerts)
			r.Post("/alerts/{id}/duplicate", alertHandler.HandleDuplicateAlert)
			r.Post("/alerts/{id}/toggle-feed", alertHandler.HandleToggleFeedAlert)

			// Normalizers (list for all users, CRUD admin-checked in handler)
			r.Get("/normalizers", normalizerHandler.HandleList)
			r.Post("/normalizers", normalizerHandler.HandleCreate)
			r.Post("/normalizers/preview", normalizerHandler.HandlePreview)
			r.Get("/normalizers/samples", normalizerHandler.HandleSamples)
			r.Post("/normalizers/import", normalizerHandler.HandleImportYAML)
			r.Get("/normalizers/{id}", normalizerHandler.HandleGet)
			r.Put("/normalizers/{id}", normalizerHandler.HandleUpdate)
			r.Delete("/normalizers/{id}", normalizerHandler.HandleDelete)
			r.Post("/normalizers/{id}/set-default", normalizerHandler.HandleSetDefault)
			r.Post("/normalizers/{id}/duplicate", normalizerHandler.HandleDuplicate)
			r.Get("/normalizers/{id}/export", normalizerHandler.HandleExportYAML)
			r.Get("/normalizers/{id}/tokens", normalizerHandler.HandleTokenUsage)

			// Schema fields (admin-only, checked in handler)
			r.Get("/admin/schema-fields", schemaFieldsHandler.HandleList)
			r.Post("/admin/schema-fields", schemaFieldsHandler.HandleCreate)
			r.Delete("/admin/schema-fields/{name}", schemaFieldsHandler.HandleDelete)
			r.Post("/admin/schema-fields/reset", schemaFieldsHandler.HandleReset)
			r.Get("/admin/schema-fields/export", schemaFieldsHandler.HandleExportYAML)
			r.Post("/admin/schema-fields/import", schemaFieldsHandler.HandleImportYAML)
			// Sampled field distribution, capacity, and ranked suggestions. One
			// request renders the whole schema tab.
			r.Get("/admin/schema-fields/insights", schemaFieldsHandler.HandleInsights)
			r.Post("/admin/schema-fields/ignore/{name}", schemaFieldsHandler.HandleIgnore)
			r.Delete("/admin/schema-fields/ignore/{name}", schemaFieldsHandler.HandleUnignore)

			// Admin-only routes (checked in handler)
			r.Post("/auth/register", authHandler.HandleRegister)
			r.Post("/auth/invite/reset", authHandler.HandleResetInvite)
			r.Post("/auth/admin-reset-password", authHandler.HandleAdminResetPassword)
			r.Get("/users", authHandler.HandleListUsers)
			r.Put("/users/{username}", authHandler.HandleUpdateUser)
			r.Put("/users/{username}/enabled", authHandler.HandleSetUserEnabled)
			r.Delete("/users", authHandler.HandleDeleteUser)
			r.Get("/users/mtls-status", authHandler.HandleMTLSStatus)
			r.Post("/users/{username}/client-cert", authHandler.HandleGenerateClientCert)
			r.Delete("/logs", statusHandler.HandleClearLogs)

			// Performance monitoring (admin-only, checked in handler)
			r.Get("/admin/processes", performanceHandler.HandleProcesses)
			r.Post("/admin/kill-query", performanceHandler.HandleKillQuery)
			r.Get("/admin/metrics", performanceHandler.HandleMetrics)
			r.Get("/admin/ingest-daily", performanceHandler.HandleIngestDaily)
			r.Get("/admin/alert-stats", performanceHandler.HandleAlertStats)
		})
	})

	// Elasticsearch-compatible bulk API (token-authenticated, no session required)
	r.Group(func(r chi.Router) {
		r.Use(ingest.RateLimitMiddleware(rateLimiter))
		r.Post("/_bulk", elasticHandler.HandleBulk)
		r.Put("/_bulk", elasticHandler.HandleBulk)
	})

	// OpenTelemetry (OTLP/HTTP) log ingestion (token-authenticated, no session required)
	r.Group(func(r chi.Router) {
		r.Use(ingest.RateLimitMiddleware(rateLimiter))
		r.Post("/v1/logs", otlpHandler.HandleLogs)
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

	// Serve static files and web UI. noDirFS suppresses directory index
	// listings, so requests for a directory 404 instead of enumerating it.
	fileServer := http.FileServer(noDirFS{http.Dir("./web")})
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.ServeFile(w, r, "./web/index.html")
			return
		}
		// Pretty path for shared wallboards: /shared/<token> serves the standalone
		// public render page (the token is read client-side and sent to the
		// anonymous API). The page is a static file, so no auth is involved here.
		if strings.HasPrefix(r.URL.Path, "/shared/") {
			http.ServeFile(w, r, "./web/shared.html")
			return
		}
		fileServer.ServeHTTP(w, r)
	})

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
	sseHub.Close()

	// Stop accepting new HTTP connections
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	// Stop the background dashboard executor (waits for in-flight refreshes)
	dashboardExecutor.Stop()

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
	PostgresHost       string
	PostgresPort       int
	PostgresDB         string
	PostgresUser       string
	PostgresPassword   string
	ClickHouseHost     string
	ClickHousePort     int
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string
	Port               int
	MaxQueryRows       int
	LiteLLMURL         string
	LiteLLMMasterKey   string
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

	// ClickHouse cluster mode (empty = single-node)
	ClickHouseHosts   string // Comma-separated list of hosts (overrides ClickHouseHost when set)
	ClickHouseCluster string // Cluster name for ON CLUSTER DDL and ReplicatedMergeTree
	// Optional single LB endpoint for ingest writes; keeps CLICKHOUSE_HOSTS for schema sync
	ClickHouseWriteHost string

	// Base URL for external links (e.g. webhook alert_link)
	BaseURL string

	// CORS
	CORSOrigins string

	// Prometheus metrics (disabled by default)
	MetricsEnabled bool
	MetricsAddr    string
}

func loadConfig() Config {
	config := Config{
		PostgresHost:       getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:       getEnvInt("POSTGRES_PORT", 5432),
		PostgresDB:         getEnv("POSTGRES_DB", "bifract"),
		PostgresUser:       getEnv("POSTGRES_USER", "bifract"),
		PostgresPassword:   getEnv("POSTGRES_PASSWORD", "bifract"),
		ClickHouseHost:     getEnv("CLICKHOUSE_HOST", "localhost"),
		ClickHousePort:     getEnvInt("CLICKHOUSE_PORT", 9000),
		ClickHouseDB:       getEnv("CLICKHOUSE_DB", "logs"),
		ClickHouseUser:     getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		Port:               getEnvInt("BIFRACT_PORT", 8080),
		MaxQueryRows:       getEnvInt("BIFRACT_MAX_QUERY_ROWS", 10000),
		LiteLLMURL:         getEnv("LITELLM_URL", "http://litellm:8000"),
		LiteLLMMasterKey:   getEnv("LITELLM_MASTER_KEY", ""),
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

		// ClickHouse cluster mode
		ClickHouseHosts:     getEnv("CLICKHOUSE_HOSTS", ""),
		ClickHouseCluster:   getEnv("CLICKHOUSE_CLUSTER", ""),
		ClickHouseWriteHost: getEnv("CLICKHOUSE_WRITE_HOST", ""),

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
	log.Printf("  ClickHouse: %s:%d", config.ClickHouseHost, config.ClickHousePort)
	log.Printf("  Database: %s", config.ClickHouseDB)
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
	if config.ClickHouseCluster != "" {
		log.Printf("  ClickHouse Cluster: %s (replicated mode)", config.ClickHouseCluster)
		if config.ClickHouseHosts != "" {
			log.Printf("  ClickHouse Hosts: %s", config.ClickHouseHosts)
		}
		if config.ClickHouseWriteHost != "" {
			log.Printf("  ClickHouse Write Host (ingest LB): %s", config.ClickHouseWriteHost)
		}
	}

	if config.MetricsEnabled {
		log.Printf("  Prometheus Metrics: %s/metrics", config.MetricsAddr)
	}

	return config
}

// addTimeIfValid sets m[key] to t's UTC time if valid, omitting the key
// entirely when NULL -- shared by the archive/maintain status blocks in the
// system/archive handler so the omit-on-NULL convention lives in one place.
func addTimeIfValid(m map[string]interface{}, key string, t sql.NullTime) {
	if t.Valid {
		m[key] = t.Time.UTC()
	}
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

// maintainStaleAfter bounds how old the maintain CronJob's last attempt can be
// before the admin UI flags it as overdue (system/archive's "on_schedule").
// The job runs hourly; ~1.7x that schedule gives slack for a run that's
// simply taking a while without flagging every normal pass as stale.
const maintainStaleAfter = 100 * time.Minute

// parseArchiveTime accepts the restore window bounds from the admin UI. It
// prefers RFC3339 (what the client sends after converting to UTC) but tolerates
// a few tz-less layouts, interpreted as UTC.
func parseArchiveTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	for _, layout := range []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized time %q", s)
}

// retentionConflict describes a fractal whose retention window ends after the
// requested restore window begins.
type retentionConflict struct {
	FractalID     string `json:"fractal_id"`
	FractalName   string `json:"fractal_name"`
	RetentionDays int    `json:"retention_days"`
	// HorizonTS is the oldest event the fractal keeps. Anything restored before
	// it is deleted by the next retention pass.
	HorizonTS string `json:"horizon"`
}

// retentionConflicts returns the fractals for which restoring from `from` would
// replay data the retention pass then deletes.
//
// Retention deletes on EVENT time while a restore window is INGEST time. Logs
// are ingested at or after the event they describe, so an ingest window starting
// before the horizon necessarily contains events before it too - the check is
// exact in that direction. It can still miss backfilled data (old events ingested
// recently), which is why it gates rather than silently rewrites the request.
//
// Fractals with unlimited retention (retention_days NULL) never conflict.
func retentionConflicts(ctx context.Context, pg *storage.PostgresClient, fractalIDs []string, from time.Time) ([]retentionConflict, error) {
	rows, err := pg.Query(ctx,
		`SELECT id::text, name, retention_days FROM fractals WHERE id::text = ANY($1) AND retention_days IS NOT NULL`,
		fractalIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []retentionConflict
	for rows.Next() {
		var c retentionConflict
		if err := rows.Scan(&c.FractalID, &c.FractalName, &c.RetentionDays); err != nil {
			return nil, err
		}
		horizon := time.Now().UTC().AddDate(0, 0, -c.RetentionDays)
		if from.Before(horizon) {
			c.HorizonTS = horizon.Format(time.RFC3339)
			out = append(out, c)
		}
	}
	return out, rows.Err()
}

// validateRecallQuery parses a BQL query and translates it in iceberg source
// mode against a placeholder table so parse errors and commands unsupported in
// archive search are rejected at submit time rather than surfacing as a failed
// job. Only translation validity is checked; no query runs.
func validateRecallQuery(query string) error {
	pipeline, err := parser.ParseQuery(query)
	if err != nil {
		return fmt.Errorf("invalid query: %w", err)
	}
	if _, err := parser.TranslateToSQLWithOrder(pipeline, parser.QueryOptions{
		StartTime:          time.Now().Add(-time.Hour),
		EndTime:            time.Now(),
		MaxRows:            1,
		SourceMode:         parser.SourceIceberg,
		UseIngestTimestamp: true,
		TableName:          "icebergValidate",
	}); err != nil {
		return err
	}
	return nil
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
		Permissions: keyData.Permissions,
	}, nil
}

func (a *APIKeyValidatorAdapter) UpdateLastUsed(ctx context.Context, keyID string) error {
	return a.storage.UpdateLastUsed(ctx, keyID)
}
