package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
// clients and health-checks both (no schema init). Returns an error so the ingest tier
// can retry through the startup window before its least-privilege user exists; the full
// server uses mustConnectClickHouse (Fatalf).
// connectClickHouse dials ClickHouse and returns the query pool (all shards) and the
// ingest pool. createDB selects whether the cluster path bootstraps the database
// (CREATE DATABASE IF NOT EXISTS): true for the app/control-plane tier (which owns
// schema provisioning and connects with an admin identity), false for the ingest tier
// (whose least-privilege user cannot create databases and connects to the already
// provisioned one). Ignored in single-node mode, which never creates the database.
func connectClickHouse(config Config, createDB bool) (db, dbIngest *storage.ClickHouseClient, err error) {
	newCluster := storage.NewClickHouseClusterClient
	if !createDB {
		newCluster = storage.NewClickHouseClusterClientConnectOnly
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

	if config.ClickHouseCluster != "" && config.ClickHouseHosts != "" {
		// Cluster mode: connect to multiple hosts.
		hosts := strings.Split(config.ClickHouseHosts, ",")
		db, err = newCluster(
			hosts, config.ClickHousePort,
			config.ClickHouseDB, config.ClickHouseUser, config.ClickHousePassword,
			config.ClickHouseCluster, queryPool,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("cluster query pool: %w", err)
		}
		// When a dedicated write LB host is set, route ingest writes through it so k8s
		// spreads connections across all shards; the query pool (db) keeps all shard
		// addresses (used for schema sync and all-shards backpressure).
		ingestHosts := hosts
		if config.ClickHouseWriteHost != "" {
			ingestHosts = []string{config.ClickHouseWriteHost}
		}
		dbIngest, err = newCluster(
			ingestHosts, config.ClickHousePort,
			config.ClickHouseDB, config.ClickHouseUser, config.ClickHousePassword,
			config.ClickHouseCluster, ingestPool,
		)
		if err != nil {
			db.Close()
			return nil, nil, fmt.Errorf("cluster ingest pool: %w", err)
		}
	} else {
		// Single-node mode (default).
		db, err = storage.NewClickHouseClientWithPool(
			config.ClickHouseHost, config.ClickHousePort,
			config.ClickHouseDB, config.ClickHouseUser, config.ClickHousePassword,
			queryPool,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("query pool: %w", err)
		}
		dbIngest, err = storage.NewClickHouseClientWithPool(
			config.ClickHouseHost, config.ClickHousePort,
			config.ClickHouseDB, config.ClickHouseUser, config.ClickHousePassword,
			ingestPool,
		)
		if err != nil {
			db.Close()
			return nil, nil, fmt.Errorf("ingest pool: %w", err)
		}
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

	// Cluster-health monitors (active in cluster mode); they write ch.* system events
	// via the queue and fire notifications, same as the full server.
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

	rateLimiter := ingest.NewRateLimiter(float64(config.IngestRateLimit), config.IngestRateBurst)
	log.Printf("Ingest ready (workers: %d, queue: %d, rate limit: %d req/s, body limit: %d bytes)",
		config.IngestWorkers, config.IngestQueueSize, config.IngestRateLimit, config.MaxBodySize)

	// Minimal router: ingest data-plane + health. No session/CORS/CSP/UI middleware.
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)

	r.Route("/api/v1", func(r chi.Router) {
		// Health (the image HEALTHCHECK GETs /api/v1/health for both tiers).
		r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"healthy"}`))
		})
		// Token-authenticated ingest.
		r.Group(func(r chi.Router) {
			r.Use(ingest.RateLimitMiddleware(rateLimiter))
			r.Post("/ingest", ingestHandler.HandleIngest)
		})
		// Internal ingest (private-network only, no token).
		r.Group(func(r chi.Router) {
			r.Use(ingest.InternalOnlyMiddleware)
			r.Use(ingest.RateLimitMiddleware(rateLimiter))
			r.Post("/internal/ingest/{fractal}", internalIngestHandler.HandleInternalIngest)
		})
	})

	// Elasticsearch-compatible bulk API.
	r.Group(func(r chi.Router) {
		r.Use(ingest.RateLimitMiddleware(rateLimiter))
		r.Post("/_bulk", elasticHandler.HandleBulk)
		r.Put("/_bulk", elasticHandler.HandleBulk)
	})
	// OpenTelemetry (OTLP/HTTP) logs.
	r.Group(func(r chi.Router) {
		r.Use(ingest.RateLimitMiddleware(rateLimiter))
		r.Post("/v1/logs", otlpHandler.HandleLogs)
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
	distMonitor.Stop()
	ddlMonitor.Stop()
	ingestQueue.Shutdown() // drains pending inserts + the spool tee
	quotaManager.Stop()
	if metricsServer != nil {
		metricsServer.Shutdown()
	}
	log.Println("Ingest server stopped gracefully")
}
