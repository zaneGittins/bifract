package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	"bifract/pkg/archives"
	"bifract/pkg/auth"
	"bifract/pkg/backup"
	"bifract/pkg/chat"
	"bifract/pkg/comments"
	"bifract/pkg/dashboards"
	"bifract/pkg/dictionaries"
	"bifract/pkg/fractals"
	"bifract/pkg/ingest"
	"bifract/pkg/ingesttokens"
	"bifract/pkg/metrics"
	"bifract/pkg/notebooks"
	"bifract/pkg/prisms"
	"bifract/pkg/query"
	"bifract/pkg/contextlinks"
	"bifract/pkg/feeds"
	"bifract/pkg/groups"
	"bifract/pkg/normalizers"
	"bifract/pkg/maxmind"
	"bifract/pkg/oidc"
	"bifract/pkg/rbac"
	"bifract/pkg/savedqueries"
	"bifract/pkg/settings"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
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

func main() {
	// Quick health probe for Docker HEALTHCHECK.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		resp, err := http.Get("http://localhost:8080/api/v1/health")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Load configuration from environment
	config := loadConfig()

	// Initialize PostgreSQL client
	log.Println("Connecting to PostgreSQL...")
	pg, err := storage.NewPostgresClient(
		config.PostgresHost,
		config.PostgresPort,
		config.PostgresDB,
		config.PostgresUser,
		config.PostgresPassword,
	)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pg.Close()

	// Health check
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pg.HealthCheck(ctx); err != nil {
		log.Fatalf("PostgreSQL health check failed: %v", err)
	}
	log.Println("Successfully connected to PostgreSQL")

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

	// Initialize ClickHouse clients (separate pools for ingest vs queries)
	log.Println("Connecting to ClickHouse...")

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

	var db, dbIngest *storage.ClickHouseClient

	if config.ClickHouseCluster != "" && config.ClickHouseHosts != "" {
		// Cluster mode: connect to multiple hosts
		hosts := strings.Split(config.ClickHouseHosts, ",")
		db, err = storage.NewClickHouseClusterClient(
			hosts, config.ClickHousePort,
			config.ClickHouseDB, config.ClickHouseUser, config.ClickHousePassword,
			config.ClickHouseCluster, queryPool,
		)
		if err != nil {
			log.Fatalf("Failed to connect to ClickHouse cluster (query pool): %v", err)
		}
		dbIngest, err = storage.NewClickHouseClusterClient(
			hosts, config.ClickHousePort,
			config.ClickHouseDB, config.ClickHouseUser, config.ClickHousePassword,
			config.ClickHouseCluster, ingestPool,
		)
		if err != nil {
			log.Fatalf("Failed to connect to ClickHouse cluster (ingest pool): %v", err)
		}
	} else {
		// Single-node mode (default)
		db, err = storage.NewClickHouseClientWithPool(
			config.ClickHouseHost, config.ClickHousePort,
			config.ClickHouseDB, config.ClickHouseUser, config.ClickHousePassword,
			queryPool,
		)
		if err != nil {
			log.Fatalf("Failed to connect to ClickHouse (query pool): %v", err)
		}
		dbIngest, err = storage.NewClickHouseClientWithPool(
			config.ClickHouseHost, config.ClickHousePort,
			config.ClickHouseDB, config.ClickHouseUser, config.ClickHousePassword,
			ingestPool,
		)
		if err != nil {
			log.Fatalf("Failed to connect to ClickHouse (ingest pool): %v", err)
		}
	}
	defer db.Close()
	defer dbIngest.Close()

	// Health check both pools
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.HealthCheck(ctx); err != nil {
		log.Fatalf("ClickHouse health check failed (query pool): %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dbIngest.HealthCheck(ctx); err != nil {
		log.Fatalf("ClickHouse health check failed (ingest pool): %v", err)
	}
	log.Printf("Successfully connected to ClickHouse (query pool: %d conns, ingest pool: %d conns)",
		queryPool.MaxOpenConns, ingestPool.MaxOpenConns)

	log.Println("Initializing ClickHouse schema...")
	if err := db.Initialize(context.Background(), dbsql.ClickHouseSQL); err != nil {
		log.Fatalf("Failed to initialize ClickHouse schema: %v", err)
	}
	log.Println("ClickHouse schema ready")

	// Initialize settings from database
	if err := settings.Init(pg); err != nil {
		log.Printf("Warning: Failed to initialize settings: %v", err)
	}
	log.Println("Settings initialized")

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
			if err := fractalManager.EnforceRetention(ctx); err != nil {
				log.Printf("Warning: Retention enforcement failed: %v", err)
			}
			cancel()
		}
	}()

	// Initialize dictionary manager (before alert engine so dict actions can be wired in)
	log.Println("Initializing dictionary manager...")
	dictionaryManager := dictionaries.NewManager(pg, db)
	dictionaryHandler := dictionaries.NewHandler(dictionaryManager, fractalManager)
	log.Println("Dictionary manager initialized")

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
	normalizerHandler := normalizers.NewHandler(normalizerManager)

	alertEngine := alerts.NewEngineWithDicts(pg, db, dictionaryManager, alertBaseURL)
	alertManager := alerts.NewManager(pg, alertEngine, normalizerManager)

	if err := alertEngine.RefreshAlerts(context.Background()); err != nil {
		log.Printf("Warning: Failed to load initial alerts cache: %v", err)
	}

	alertEngine.Start(time.Duration(config.AlertEvalInterval) * time.Second)
	log.Printf("Alert system initialized (evaluation interval: %ds)", config.AlertEvalInterval)

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

	// Initialize ingestion queue and handlers (uses dedicated ingest pool)
	log.Println("Initializing ingestion queue...")
	ingestQueue := ingest.NewIngestQueue(dbIngest, config.IngestQueueSize, config.IngestWorkers)
	ingestQueue.SetQuotaManager(quotaManager)

	ingestHandler := ingest.NewIngestHandler(ingestQueue, config.MaxBodySize, tokenCache, ingestTokenStorage)
	ingestHandler.SetQuotaManager(quotaManager)
	elasticHandler := ingest.NewElasticBulkHandler(ingestHandler)
	internalIngestHandler := ingest.NewInternalIngestHandler(ingestQueue, config.MaxBodySize, fractalManager, normalizerManager)

	// Wire ingest pressure signal to the alert engine so it defers evaluation
	// during heavy ingestion. Cursor-based tracking ensures no logs are missed.
	alertEngine.SetIngestPressureFunc(func() bool {
		return ingestQueue.Depth() > config.IngestQueueSize/10
	})
	alertEngine.SetLastIngestedFunc(func(fractalID string) time.Time {
		return ingestQueue.LastIngested(fractalID)
	})

	// Rate limiter for ingestion endpoints
	rateLimiter := ingest.NewRateLimiter(float64(config.IngestRateLimit), config.IngestRateBurst)
	log.Printf("Ingestion ready (workers: %d, queue: %d, rate limit: %d req/s, body limit: %d bytes)",
		config.IngestWorkers, config.IngestQueueSize, config.IngestRateLimit, config.MaxBodySize)

	queryHandler := query.NewQueryHandlerFull(db, config.MaxQueryRows, fractalManager, dictionaryManager, prismManager)
	queryHandler.SetPostgresClient(pg)

	// Launch MaxMind background load after queryHandler exists
	if maxmindManager != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()
			if err := maxmindManager.LoadAll(ctx); err != nil {
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
	performanceHandler := query.NewPerformanceHandler(db)
	settingsHandler := settings.NewHandler(pg)

	// Create API key handler and storage
	apiKeyHandler := apikeys.NewHandler(pg, fractalManager)
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
	prismHandler := prisms.NewHandler(prismManager, authHandler)
	fractalHandler := fractals.NewHandler(fractalManager, authHandler, prismManager)
	fractalHandler.SetRBAC(pg, authHandler.RBACResolver())

	// Wire RBAC into handlers that need per-fractal permission checks
	apiKeyHandler.SetRBAC(authHandler.RBACResolver())
	ingestTokenHandler.SetRBAC(authHandler.RBACResolver())
	queryHandler.SetRBACResolver(authHandler.RBACResolver())

	// Groups handler (tenant admin only)
	groupHandler := groups.NewHandler(pg)

	commentHandler := comments.NewCommentHandlerWithFractals(pg, db, fractalManager)
	notebookHandler := notebooks.NewNotebookHandler(pg, db, fractalManager, config.LiteLLMURL, config.LiteLLMMasterKey)
	notebookHandler.SetRBACResolver(authHandler.RBACResolver())
	dashboardHandler := dashboards.NewDashboardHandler(pg, fractalManager)
	dashboardHandler.SetRBACResolver(authHandler.RBACResolver())
	alertHandler := alerts.NewHandlerWithFractals(alertManager, fractalManager)
	alertHandler.SetRBACResolver(authHandler.RBACResolver())

	chatManager := chat.NewManager(pg, db, fractalManager, normalizerManager, config.LiteLLMURL, config.LiteLLMMasterKey)
	rbacAdapter := &fractalAccessAdapter{resolver: authHandler.RBACResolver()}
	chatHandler := chat.NewHandler(chatManager, fractalManager, rbacAdapter)
	savedQueryHandler := savedqueries.NewHandler(pg, fractalManager)
	savedQueryHandler.SetRBACResolver(rbacAdapter)
	contextLinkManager := contextlinks.NewManager(pg)
	contextLinkHandler := contextlinks.NewHandler(contextLinkManager)

	// Initialize feed system
	feedManager := feeds.NewManager(pg)
	feedSyncer := feeds.NewSyncer(feedManager, alertManager, normalizerManager)
	feedHandler := feeds.NewHandler(feedManager, alertManager, fractalManager, feedSyncer)
	feedSyncer.Start()
	log.Println("Feed sync system initialized")

	// Initialize archive system
	var archiveManager *archives.Manager
	archiveStorageCfg := backup.StorageConfigFromEnv("/archives")
	archiveBackend, err := backup.NewStorageBackend(archiveStorageCfg)
	if err != nil {
		log.Printf("Warning: archive storage backend not available: %v", err)
	} else {
		archiveStore := archives.NewStorage(pg)
		archiveManager = archives.NewManager(pg, db, archiveBackend, archiveStore)
		archiveManager.RecoverInterrupted(context.Background())
		log.Println("Archive system initialized")
	}
	archiveHandler := archives.NewHandler(archiveManager, fractalManager, authHandler.RBACResolver(), ingestTokenStorage)

	// Start archive scheduler
	var archiveScheduler *archives.Scheduler
	if archiveManager != nil {
		archiveScheduler = archives.NewScheduler(archiveManager, fractalManager)
		archiveScheduler.Start()
	}

	// Setup router
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Timeout(60 * time.Second))

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
					"script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com; "+
					"style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://unpkg.com; "+
					"font-src 'self' https://fonts.gstatic.com; "+
					"img-src 'self' data: https://*.basemaps.cartocdn.com https://*.tile.openstreetmap.org; "+
					"connect-src 'self'")
			if secureCookies {
				w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
			}
			next.ServeHTTP(w, r)
		})
	})

	// CORS middleware
	corsOrigins := strings.Split(config.CORSOrigins, ",")
	for i := range corsOrigins {
		corsOrigins[i] = strings.TrimSpace(corsOrigins[i])
	}
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token", "X-API-Key"},
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
			r.Get("/query/reference", queryHandler.HandleReference)
			r.Get("/logs/recent", queryHandler.HandleGetRecentLogs)
			r.Post("/logs/by-timestamp", queryHandler.HandleGetLogByTimestamp)
			r.Get("/status", statusHandler.HandleStatus)
			r.Get("/health/clickhouse", statusHandler.HandleHealthCheck)
			r.Get("/system/pressure", func(w http.ResponseWriter, r *http.Request) {
				alertsDeferred := ingestQueue.Depth() > config.IngestQueueSize/10
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{
					"alerts_deferred": alertsDeferred,
				})
			})

			// Settings
			r.Get("/settings", settingsHandler.HandleGet)
			r.Post("/settings", settingsHandler.HandleUpdate)

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
				r.Get("/notebooks/{id}/export", notebookHandler.HandleExportNotebook)
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
				r.Put("/dashboards/{id}/widgets/{widget_id}/results", dashboardHandler.HandleUpdateWidgetResults)
				r.Put("/dashboards/{id}/widgets/{widget_id}/layout", dashboardHandler.HandleUpdateWidgetLayout)
				r.Delete("/dashboards/{id}/widgets/{widget_id}", dashboardHandler.HandleDeleteWidget)
				r.Put("/dashboards/{id}/variables", dashboardHandler.HandleUpdateVariables)
				r.Post("/dashboards/{id}/presence", dashboardHandler.HandleUpdatePresence)
				r.Get("/dashboards/{id}/presence", dashboardHandler.HandleGetPresence)
				r.Get("/dashboards/{id}/export", dashboardHandler.HandleExportDashboard)
				r.Post("/dashboards/import", dashboardHandler.HandleImportDashboard)
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

			// Prism management
			r.Get("/prisms", prismHandler.HandleListPrisms)
			r.Post("/prisms", prismHandler.HandleCreatePrism)
			r.Get("/prisms/{id}", prismHandler.HandleGetPrism)
			r.Put("/prisms/{id}", prismHandler.HandleUpdatePrism)
			r.Delete("/prisms/{id}", prismHandler.HandleDeletePrism)
			r.Post("/prisms/{id}/select", prismHandler.HandleSelectPrism)
			r.Post("/prisms/{id}/members", prismHandler.HandleAddMember)
			r.Delete("/prisms/{id}/members/{fractalID}", prismHandler.HandleRemoveMember)

			// Fractal management
			r.Get("/fractals", fractalHandler.HandleListFractals)
			r.Post("/fractals", fractalHandler.HandleCreateFractal)
			r.Get("/fractals/{id}", fractalHandler.HandleGetFractal)
			r.Put("/fractals/{id}", fractalHandler.HandleUpdateFractal)
			r.Delete("/fractals/{id}", fractalHandler.HandleDeleteFractal)
			r.Post("/fractals/{id}/select", fractalHandler.HandleSelectFractal)
			r.Get("/fractals/{id}/stats", fractalHandler.HandleGetStats)
			r.Put("/fractals/{id}/retention", fractalHandler.HandleSetRetention)
			r.Put("/fractals/{id}/archive-schedule", fractalHandler.HandleSetArchiveSchedule)
			r.Put("/fractals/{id}/disk-quota", fractalHandler.HandleSetDiskQuota)
			r.Post("/fractals/stats/refresh", fractalHandler.HandleRefreshStats)

			// Fractal archives (fractal admin or tenant admin, checked in handler)
			r.Post("/fractals/{id}/archives", archiveHandler.HandleCreateArchive)
			r.Get("/fractals/{id}/archives", archiveHandler.HandleListArchives)
			r.Get("/fractals/{id}/archives/{archiveId}", archiveHandler.HandleGetArchive)
			r.Post("/fractals/{id}/archives/{archiveId}/restore", archiveHandler.HandleRestoreArchive)
			r.Post("/fractals/{id}/archives/{archiveId}/cancel", archiveHandler.HandleCancelOperation)
			r.Delete("/fractals/{id}/archives/{archiveId}", archiveHandler.HandleDeleteArchive)

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

			// API Key management
			r.Get("/fractals/{id}/api-keys", apiKeyHandler.HandleListAPIKeys)
			r.Post("/fractals/{id}/api-keys", apiKeyHandler.HandleCreateAPIKey)
			r.Get("/fractals/{id}/api-keys/{keyId}", apiKeyHandler.HandleGetAPIKey)
			r.Put("/fractals/{id}/api-keys/{keyId}", apiKeyHandler.HandleUpdateAPIKey)
			r.Delete("/fractals/{id}/api-keys/{keyId}", apiKeyHandler.HandleDeleteAPIKey)
			r.Post("/fractals/{id}/api-keys/{keyId}/toggle", apiKeyHandler.HandleToggleAPIKey)

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
			r.Post("/dictionaries/{id}/columns", dictionaryHandler.HandleAddColumn)
			r.Delete("/dictionaries/{id}/columns/{name}", dictionaryHandler.HandleRemoveColumn)
			r.Post("/dictionaries/{id}/columns/{name}/key", dictionaryHandler.HandleSetColumnKey)
			r.Delete("/dictionaries/{id}/columns/{name}/key", dictionaryHandler.HandleUnsetColumnKey)
			r.Post("/dictionaries/{id}/reload", dictionaryHandler.HandleReloadDictionary)

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

			// Chat
			r.Get("/chat/conversations", chatHandler.HandleListConversations)
			r.Post("/chat/conversations", chatHandler.HandleCreateConversation)
			r.Patch("/chat/conversations/{id}", chatHandler.HandleRenameConversation)
			r.Delete("/chat/conversations/{id}", chatHandler.HandleDeleteConversation)
			r.Get("/chat/conversations/{id}/messages", chatHandler.HandleGetMessages)
			r.Delete("/chat/conversations/{id}/messages", chatHandler.HandleClearMessages)
			r.Post("/chat/conversations/{id}/stream", chatHandler.HandleStream)
			r.Patch("/chat/conversations/{id}/instruction", chatHandler.HandleSetConversationInstruction)
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
			r.Post("/normalizers/import", normalizerHandler.HandleImportYAML)
			r.Get("/normalizers/{id}", normalizerHandler.HandleGet)
			r.Put("/normalizers/{id}", normalizerHandler.HandleUpdate)
			r.Delete("/normalizers/{id}", normalizerHandler.HandleDelete)
			r.Post("/normalizers/{id}/set-default", normalizerHandler.HandleSetDefault)
			r.Post("/normalizers/{id}/duplicate", normalizerHandler.HandleDuplicate)
			r.Get("/normalizers/{id}/export", normalizerHandler.HandleExportYAML)
			r.Get("/normalizers/{id}/tokens", normalizerHandler.HandleTokenUsage)

			// Admin-only routes (checked in handler)
			r.Post("/auth/register", authHandler.HandleRegister)
			r.Post("/auth/invite/reset", authHandler.HandleResetInvite)
			r.Post("/auth/admin-reset-password", authHandler.HandleAdminResetPassword)
			r.Get("/users", authHandler.HandleListUsers)
			r.Put("/users/{username}", authHandler.HandleUpdateUser)
			r.Delete("/users", authHandler.HandleDeleteUser)
			r.Delete("/logs", statusHandler.HandleClearLogs)

			// Performance monitoring (admin-only, checked in handler)
			r.Get("/admin/processes", performanceHandler.HandleProcesses)
			r.Post("/admin/kill-query", performanceHandler.HandleKillQuery)
			r.Get("/admin/metrics", performanceHandler.HandleMetrics)
		})
	})

	// Elasticsearch-compatible bulk API (token-authenticated, no session required)
	r.Group(func(r chi.Router) {
		r.Use(ingest.RateLimitMiddleware(rateLimiter))
		r.Post("/_bulk", elasticHandler.HandleBulk)
		r.Put("/_bulk", elasticHandler.HandleBulk)
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

	// Serve static files and web UI
	fs := http.FileServer(http.Dir("./web"))
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.ServeFile(w, r, "./web/index.html")
			return
		}
		fs.ServeHTTP(w, r)
	})

	// HTTP server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", config.Port),
		Handler:      r,
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 120 * time.Second,
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

	// Stop accepting new HTTP connections
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	// Drain the ingestion queue (finish pending inserts)
	ingestQueue.Shutdown()

	// Stop the quota manager refresh loop
	quotaManager.Stop()

	// Stop the feed syncer
	feedSyncer.Stop()

	// Stop MaxMind refresh
	if maxmindManager != nil {
		maxmindManager.Stop()
	}

	// Stop the alert engine
	alertEngine.Stop()

	// Stop archive scheduler and running operations
	if archiveScheduler != nil {
		archiveScheduler.Stop()
	}
	if archiveManager != nil {
		archiveManager.Shutdown()
	}

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

	// Alert evaluation
	AlertEvalInterval int // seconds

	// ClickHouse pool sizing (0 = use defaults)
	CHQueryMaxConns  int
	CHIngestMaxConns int

	// ClickHouse cluster mode (empty = single-node)
	ClickHouseHosts   string // Comma-separated list of hosts (overrides ClickHouseHost when set)
	ClickHouseCluster string // Cluster name for ON CLUSTER DDL and ReplicatedMergeTree

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

		// Alert evaluation default
		AlertEvalInterval: getEnvInt("BIFRACT_ALERT_EVAL_INTERVAL", 30),

		// ClickHouse pool sizing (0 = use package defaults)
		CHQueryMaxConns:  getEnvInt("BIFRACT_CH_QUERY_MAX_CONNS", 0),
		CHIngestMaxConns: getEnvInt("BIFRACT_CH_INGEST_MAX_CONNS", 0),

		// ClickHouse cluster mode
		ClickHouseHosts:   getEnv("CLICKHOUSE_HOSTS", ""),
		ClickHouseCluster: getEnv("CLICKHOUSE_CLUSTER", ""),

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
	log.Printf("  Alert Eval Interval: %ds", config.AlertEvalInterval)
	if config.ClickHouseCluster != "" {
		log.Printf("  ClickHouse Cluster: %s (replicated mode)", config.ClickHouseCluster)
		if config.ClickHouseHosts != "" {
			log.Printf("  ClickHouse Hosts: %s", config.ClickHouseHosts)
		}
	}

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
		Permissions: keyData.Permissions,
	}, nil
}

func (a *APIKeyValidatorAdapter) UpdateLastUsed(ctx context.Context, keyID string) error {
	return a.storage.UpdateLastUsed(ctx, keyID)
}
