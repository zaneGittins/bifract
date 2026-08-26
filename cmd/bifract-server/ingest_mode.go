package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bifract/pkg/api"
	"bifract/pkg/fractals"
	"bifract/pkg/ingest"
	"bifract/pkg/ingesttokens"
	"bifract/pkg/metrics"
	"bifract/pkg/normalizers"
	"bifract/pkg/notifications"
	"bifract/pkg/settings"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// connectPostgres opens and health-checks the Postgres client (no schema init).
// Shared by the full server (Fatalf on error via mustConnectPostgres) and the ingest
// server (retry). Returns an error so the ingest tier can tolerate the startup window
// before its least-privilege role exists.
func connectPostgres(config Config) (*storage.PostgresClient, error) {
	pg, err := storage.NewPostgresClient(
		config.PostgresHost,
		config.PostgresPort,
		config.PostgresDB,
		config.PostgresUser,
		config.PostgresPassword,
	)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pg.HealthCheck(ctx); err != nil {
		pg.Close()
		return nil, fmt.Errorf("health check: %w", err)
	}
	return pg, nil
}

// mustConnectPostgres is the full-server wrapper (Fatalf on failure).
func mustConnectPostgres(config Config) *storage.PostgresClient {
	log.Println("Connecting to PostgreSQL...")
	pg, err := connectPostgres(config)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	log.Println("Successfully connected to PostgreSQL")
	return pg
}

// connectClickHouse builds the query (all-shards) and ingest (write-routed) ClickHouse
// clients and health-checks both (no schema init). It returns an error so the ingest tier
// can retry through the startup window before its least-privilege user exists; the full
// server wraps it in mustConnectClickHouse (Fatalf).
//
// controlPlane selects whether the query client bootstraps the database: true for the app
// tier, which owns schema provisioning and connects with an admin identity; false for the
// ingest tier, whose least-privilege user cannot create databases. The ingest pool never
// bootstraps, because the query client above it already has.
func connectClickHouse(config Config, controlPlane bool) (db, dbIngest *storage.ClickHouseClient, err error) {
	queryRole := storage.RoleIngest
	if controlPlane {
		queryRole = storage.RoleControlPlane
	}
	queryPool := storage.DefaultQueryPoolConfig()
	if config.CHQueryMaxConns > 0 {
		queryPool.MaxOpenConns = config.CHQueryMaxConns
		queryPool.MaxIdleConns = config.CHQueryMaxConns / 4
		if queryPool.MaxIdleConns < 2 {
			queryPool.MaxIdleConns = 2
		}
	}

	ingestPool := storage.DefaultIngestPoolConfig()
	if config.CHIngestMaxConns > 0 {
		ingestPool.MaxOpenConns = config.CHIngestMaxConns
		ingestPool.MaxIdleConns = config.CHIngestMaxConns / 2
		if ingestPool.MaxIdleConns < 2 {
			ingestPool.MaxIdleConns = 2
		}
	}

	// The query client covers every address (schema sync and all-node backpressure
	// need that); the ingest client routes through the write LB when one is set.
	queryOpts, err := config.CH.ClientOptions(queryPool, queryRole)
	if err != nil {
		return nil, nil, err
	}
	ingestOpts, err := config.CH.IngestOptions(ingestPool, storage.RoleIngest)
	if err != nil {
		return nil, nil, err
	}

	db, err = storage.NewClickHouseClient(queryOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("query pool: %w", err)
	}
	dbIngest, err = storage.NewClickHouseClient(ingestOpts)
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("ingest pool: %w", err)
	}

	for _, hc := range []struct {
		name string
		c    *storage.ClickHouseClient
	}{{"query pool", db}, {"ingest pool", dbIngest}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		herr := hc.c.HealthCheck(ctx)
		cancel()
		if herr != nil {
			db.Close()
			dbIngest.Close()
			return nil, nil, fmt.Errorf("%s health check: %w", hc.name, herr)
		}
	}
	return db, dbIngest, nil
}

// mustConnectClickHouse is the full-server wrapper (Fatalf on failure).
func mustConnectClickHouse(config Config) (db, dbIngest *storage.ClickHouseClient) {
	log.Println("Connecting to ClickHouse...")
	// App/control-plane tier: owns schema provisioning, so it bootstraps the database.
	db, dbIngest, err := connectClickHouse(config, true)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	log.Println("Successfully connected to ClickHouse")
	return db, dbIngest
}

// connectIngestDBsWithRetry dials Postgres + ClickHouse with the ingest tier's
// least-privilege credentials, retrying through the startup window before the app tier
// provisions those identities (on k8s the deployments start independently). Fatalf after
// the ceiling so a genuine misconfiguration still surfaces.
func connectIngestDBsWithRetry(config Config) (*storage.PostgresClient, *storage.ClickHouseClient, *storage.ClickHouseClient) {
	deadline := time.Now().Add(5 * time.Minute)
	logged := false
	for {
		pg, perr := connectPostgres(config)
		if perr == nil {
			// Ingest tier: connect-only. Its least-privilege user cannot create the
			// database, and the app tier has already provisioned it.
			db, dbIngest, cherr := connectClickHouse(config, false)
			if cherr == nil {
				log.Println("Connected to PostgreSQL and ClickHouse (ingest identity)")
				return pg, db, dbIngest
			}
			pg.Close()
			perr = cherr
		}
		if time.Now().After(deadline) {
			log.Fatalf("Ingest server could not connect to databases within the startup window: %v", perr)
		}
		if !logged {
			log.Printf("Waiting for ingest DB identities to be provisioned (retrying): %v", perr)
			logged = true
		}
		time.Sleep(3 * time.Second)
	}
}

// waitForWriteTable polls (bounded) until the ingest write table exists, so an ingest
// server that boots before the full-server tier finishes migrations does not drop a
// startup burst. The queue's insert retry/backoff is the backstop past the ceiling.
// Checks via system.tables (not SELECT FROM logs) so it works for the least-privilege
// ingest user, which has no read access to log data.
func waitForWriteTable(dbIngest *storage.ClickHouseClient) {
	table := dbIngest.WriteTable()
	deadline := time.Now().Add(2 * time.Minute)
	query := "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = '" + table + "'"
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rows, err := dbIngest.Query(ctx, query)
		cancel()
		if err == nil && len(rows) > 0 {
			return
		}
		if time.Now().After(deadline) {
			log.Printf("Warning: ingest starting before write table %s is ready", table)
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// runIngestServer runs the data-plane-only ingest tier (invoked by `bifract-server
// ingest`). It constructs only the ingest queue, spool tee, backpressure monitors, and
// the ingest HTTP surface -- no migrations, alerts, models, dashboards, query, or UI.
// The full-server tier owns schema migrations and the control plane.
func runIngestServer() {
	config := loadConfig()
	log.Printf("Starting Bifract in INGEST mode (port %d, workers %d, queue %d)",
		config.Port, config.IngestWorkers, config.IngestQueueSize)

	// Retry connecting so the ingest tier does not crash-loop during the window where
	// the app tier is still provisioning the least-privilege ingest DB identities
	// (on k8s the app and ingest deployments start independently). Bounded so a genuine
	// misconfiguration still surfaces.
	pg, db, dbIngest := connectIngestDBsWithRetry(config)
	defer pg.Close()
	defer db.Close()
	defer dbIngest.Close()

	notifWriter := notifications.New(pg)

	// Ingest mode never runs migrations. Wait (bounded) for the schema so cold starts
	// after a fresh deploy don't drop the first burst.
	waitForWriteTable(dbIngest)

	// The control plane refuses to start on an incompatible schema, but this tier is
	// the one that writes, and it starts independently. Without its own check it
	// would keep accepting logs into a table that is about to be dropped.
	if err := dbIngest.CheckLogsPartitionKey(context.Background()); errors.Is(err, storage.ErrIncompatibleSchema) {
		log.Fatalf("Refusing to ingest: %v", err)
	}

	if err := settings.Init(pg); err != nil {
		log.Printf("Warning: failed to initialize settings: %v", err)
	}

	fractalManager := fractals.NewManager(pg, db)
	normalizerManager := normalizers.NewManager(pg)

	ingestTokenStorage := ingesttokens.NewStorage(pg)
	tokenCache := ingesttokens.NewTokenCache(60 * time.Second)
	quotaManager := ingest.NewQuotaManager(pg, dbIngest)

	ingestQueue := ingest.NewIngestQueue(dbIngest, config.IngestQueueSize, config.IngestWorkers)
	ingestQueue.SetQuotaManager(quotaManager)
	ingestQueue.SetNotificationWriter(notifWriter)
	// Backpressure should reflect every shard, not just the write LB dbIngest targets.
	ingestQueue.SetMetricsClient(db)
	startArchiveSpool(ingestQueue, pg)
	if sysFractal, err := fractalManager.GetFractalByName(context.Background(), "system"); err == nil {
		ingestQueue.SetSystemFractalID(sysFractal.ID)
	} else {
		log.Printf("Warning: could not resolve system fractal for ingest monitoring: %v", err)
	}

	ingestHandler := ingest.NewIngestHandler(ingestQueue, config.MaxBodySize, tokenCache, ingestTokenStorage)
	ingestHandler.SetQuotaManager(quotaManager)
	elasticHandler := ingest.NewElasticBulkHandler(ingestHandler)
	otlpHandler := ingest.NewOTLPHandler(ingestHandler)
	internalIngestHandler := ingest.NewInternalIngestHandler(ingestQueue, config.MaxBodySize, fractalManager, normalizerManager)

	// The distribution-queue and DDL-queue monitors are NOT run here. They observe
	// cluster-global state (not an ingest input -- distinct from backpressure, which
	// the ingest tier does consume via local per-shard system metrics), require the
	// REMOTE privilege (clusterAllReplicas) that the least-privilege ingest user does
	// not have, and are already run by the app/control-plane tier -- which keeps
	// monitoring even when the ingest tier is scaled to 0. See cmd/bifract-server/main.go.

	rateLimiter := ingest.NewRateLimiter(float64(config.IngestRateLimit), config.IngestRateBurst)
	log.Printf("Ingest ready (workers: %d, queue: %d, rate limit: %d req/s, body limit: %d bytes)",
		config.IngestWorkers, config.IngestQueueSize, config.IngestRateLimit, config.MaxBodySize)

	r, _ := buildIngestRouter(ingestRouterDeps{
		rateLimiter:           rateLimiter,
		ingestHandler:         ingestHandler,
		internalIngestHandler: internalIngestHandler,
		elasticHandler:        elasticHandler,
		otlpHandler:           otlpHandler,
	})

	// Prometheus metrics (own surface; no alert engine to attach).
	var metricsServer *metrics.Server
	if config.MetricsEnabled {
		collector := metrics.New(Version)
		collector.AttachIngest(ingestQueue)
		metricsServer = metrics.NewServer(config.MetricsAddr, collector)
		metricsServer.Start()
	}

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", config.Port),
		Handler:      r,
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Printf("Starting Bifract ingest server on port %d...", config.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ingest server failed to start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down ingest server...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Ingest server forced to shutdown: %v", err)
	}
	ingestQueue.Shutdown() // drains pending inserts + the spool tee
	quotaManager.Stop()
	if metricsServer != nil {
		metricsServer.Shutdown()
	}
	log.Println("Ingest server stopped gracefully")
}

// ingestRouterDeps carries what the ingest tier's router mounts. It is
// deliberately a fraction of routerDeps: this tier serves the data plane only.
type ingestRouterDeps struct {
	rateLimiter           *ingest.RateLimiter
	ingestHandler         *ingest.IngestHandler
	internalIngestHandler *ingest.InternalIngestHandler
	elasticHandler        *ingest.ElasticBulkHandler
	otlpHandler           *ingest.OTLPHandler
}

// buildIngestRouter mounts the ingest data plane and the health probe. No
// session, CORS, CSP, or UI middleware: this tier serves no browser.
func buildIngestRouter(d ingestRouterDeps) (*chi.Mux, *api.Registry) {
	reg := api.NewRegistry()
	mux := chi.NewRouter()
	mux.Use(middleware.Recoverer)
	mux.Use(middleware.RequestID)
	mux.Use(middleware.RealIP)

	r := api.NewRouter(mux, reg)
	r.Route("/api/v1", func(r api.Router) {
		r.Register(api.Route{
			Method:  http.MethodGet,
			Path:    "/health",
			Summary: "Liveness probe.",
			Handler: handleHealth,
		})
		r.Group(func(r api.Router) {
			r.Use(ingest.RateLimitMiddleware(d.rateLimiter))
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/ingest",
				Summary: "Ingest a batch of logs, routed to the fractal the ingest token is scoped to.",
				Handler: d.ingestHandler.HandleIngest,
			})
		})
		r.Group(func(r api.Router) {
			r.Use(ingest.InternalOnlyMiddleware)
			r.Use(ingest.RateLimitMiddleware(d.rateLimiter))
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/internal/ingest/{fractal}",
				Summary: "Ingest logs for a named fractal from inside the private network, without a token.",
				Handler: d.internalIngestHandler.HandleInternalIngest,
			})
		})
	})

	r.Group(func(r api.Router) {
		r.Use(ingest.RateLimitMiddleware(d.rateLimiter))
		r.Register(api.Route{
			Method:  http.MethodPost,
			Path:    "/_bulk",
			Summary: "Ingest logs through the Elasticsearch-compatible bulk API.",
			Handler: d.elasticHandler.HandleBulk,
		})
		r.Register(api.Route{
			Method:  http.MethodPut,
			Path:    "/_bulk",
			Summary: "Ingest logs through the Elasticsearch-compatible bulk API.",
			Handler: d.elasticHandler.HandleBulk,
		})
	})
	r.Group(func(r api.Router) {
		r.Use(ingest.RateLimitMiddleware(d.rateLimiter))
		r.Register(api.Route{
			Method:  http.MethodPost,
			Path:    "/v1/logs",
			Summary: "Ingest logs as an OTLP/HTTP ExportLogsServiceRequest.",
			Handler: d.otlpHandler.HandleLogs,
		})
	})

	return mux, reg
}
