package main

import (
	"net/http"
	"os"
	"strings"
	"time"

	"bifract/pkg/alerts"
	"bifract/pkg/api"
	"bifract/pkg/apikeys"
	"bifract/pkg/archive"
	"bifract/pkg/attack"
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
	"bifract/pkg/models"
	"bifract/pkg/normalizers"
	"bifract/pkg/notebooks"
	"bifract/pkg/notifications"
	"bifract/pkg/oidc"
	"bifract/pkg/prisms"
	"bifract/pkg/query"
	"bifract/pkg/queryhistory"
	"bifract/pkg/savedqueries"
	"bifract/pkg/schemafields"
	"bifract/pkg/settings"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

// routerDeps carries everything buildRouter mounts. Construction and wiring
// stay in main; this type is the seam between the two.
type routerDeps struct {
	config Config

	pg       *storage.PostgresClient
	db       *storage.ClickHouseClient
	dbIngest *storage.ClickHouseClient

	fractalManager  *fractals.Manager
	ingestQueue     *ingest.IngestQueue
	rateLimiter     *ingest.RateLimiter
	distMonitor     *storage.DistributionMonitor
	ddlMonitor      *storage.DDLMonitor
	recallEstimator *archive.Estimator

	// alertDeferThreshold is the ingest queue depth past which alert
	// evaluation is deferred, reported by /system/pressure.
	alertDeferThreshold int

	alertHandler          *alerts.Handler
	apiKeyHandler         *apikeys.Handler
	authHandler           *auth.AuthHandler
	chatHandler           *chat.Handler
	commentHandler        *comments.CommentHandler
	contextLinkHandler    *contextlinks.Handler
	dashboardHandler      *dashboards.DashboardHandler
	deepLinkHandler       *deeplink.Handler
	dictionaryHandler     *dictionaries.Handler
	elasticHandler        *ingest.ElasticBulkHandler
	feedHandler           *feeds.Handler
	fractalHandler        *fractals.Handler
	groupHandler          *groups.Handler
	ingestHandler         *ingest.IngestHandler
	ingestTokenHandler    *ingesttokens.Handler
	instructionHandler    *instructions.Handler
	internalIngestHandler *ingest.InternalIngestHandler
	modelHandler          *models.Handler
	normalizerHandler     *normalizers.Handler
	notebookHandler       *notebooks.NotebookHandler
	notificationHandler   *notifications.Handler
	oidcHandler           *oidc.Handler
	otlpHandler           *ingest.OTLPHandler
	performanceHandler    *query.PerformanceHandler
	prismHandler          *prisms.Handler
	queryHandler          *query.QueryHandler
	queryHistoryHandler   *queryhistory.Handler
	savedQueryHandler     *savedqueries.Handler
	schemaFieldsHandler   *schemafields.Handler
	settingsHandler       *settings.Handler
	statusHandler         *query.StatusHandler
}

// buildRouter mounts every HTTP route on a new chi router, returning it with
// the registry describing the routes mounted through api.Router.Register.
func buildRouter(d routerDeps) (*chi.Mux, *api.Registry) {
	reg := api.NewRegistry()
	mux := chi.NewRouter()

	// Middleware
	mux.Use(middleware.Recoverer)
	mux.Use(middleware.RequestID)
	mux.Use(middleware.RealIP)
	// Timeout middleware, bypassed for SSE and chat streaming endpoints.
	mux.Use(func(next http.Handler) http.Handler {
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
	mux.Use(func(next http.Handler) http.Handler {
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
	mux.Use(compressor.Handler)

	// CORS middleware
	corsOrigins := strings.Split(d.config.CORSOrigins, ",")
	for i := range corsOrigins {
		corsOrigins[i] = strings.TrimSpace(corsOrigins[i])
	}
	mux.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token", "X-API-Key", "X-SSE-Client-ID"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// API routes
	r := api.NewRouter(mux, reg)
	r.Route("/api/v1", func(r api.Router) {
		// Ingestion route (token-authenticated, no session required)
		r.Group(func(r api.Router) {
			r.Use(ingest.RateLimitMiddleware(d.rateLimiter))
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/ingest",
				Response: ingest.IngestResponse{},
				Summary:  "Ingest a batch of logs, routed to the fractal the ingest token is scoped to.",
				Handler:  d.ingestHandler.HandleIngest,
			})
		})

		// Internal ingestion route (private-network only, no token required)
		r.Group(func(r api.Router) {
			r.Use(ingest.InternalOnlyMiddleware)
			r.Use(ingest.RateLimitMiddleware(d.rateLimiter))
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/internal/ingest/{fractal}",
				Response: ingest.IngestResponse{},
				Summary:  "Ingest logs for a named fractal from inside the private network, without a token.",
				Handler:  d.internalIngestHandler.HandleInternalIngest,
			})
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
		r.With(ingest.RateLimitMiddleware(sharedLinkLimiter)).Register(api.Route{
			Method:  http.MethodGet,
			Path:    "/shared/{token}",
			Summary: "Render a shared dashboard from cached results, without authentication.",
			Handler: d.dashboardHandler.HandleSharedDashboard,
		})
		r.Register(api.Route{
			Method:  http.MethodPost,
			Path:    "/auth/login",
			Request: auth.LoginRequest{},
			Summary: "Exchange a username and password for a session.",
			Handler: d.authHandler.HandleLogin,
		})
		r.Register(api.Route{
			Method:   http.MethodGet,
			Path:     "/auth/invite/validate",
			Response: auth.Response{},
			Summary:  "Check whether an invite token is still valid.",
			Handler:  d.authHandler.HandleValidateInvite,
		})
		r.Register(api.Route{
			Method:   http.MethodPost,
			Path:     "/auth/invite/accept",
			Request:  auth.AcceptInviteRequest{},
			Response: auth.Response{},
			Summary:  "Set a password and activate an account from an invite token.",
			Handler:  d.authHandler.HandleAcceptInvite,
		})
		r.Register(api.Route{
			Method:  http.MethodGet,
			Path:    "/health",
			Summary: "Liveness probe.",
			Handler: handleHealth,
		})
		// OIDC routes (public, no auth required)
		if d.oidcHandler != nil {
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/auth/oidc/config",
				Summary: "Report whether OIDC login is available.",
				Handler: d.oidcHandler.HandleConfig,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/auth/oidc/login",
				Summary: "Begin the OIDC authorization flow.",
				Handler: d.oidcHandler.HandleLogin,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/auth/oidc/callback",
				Summary: "Complete the OIDC flow and establish a session.",
				Handler: d.oidcHandler.HandleCallback,
			})
		} else {
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/auth/oidc/config",
				Summary: "Report whether OIDC login is available.",
				Handler: d.handleOIDCDisabled,
			})
		}

		// Protected routes (auth required)
		r.Group(func(r api.Router) {
			r.Use(d.authHandler.AuthMiddleware)

			// Body size limit for non-ingest API endpoints (1MB)
			r.Use(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB
					next.ServeHTTP(w, r)
				})
			})

			// Version
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/version",
				Summary: "Report the server version.",
				Handler: d.handleVersion,
			})

			// Query and status
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/query",
				Response: query.QueryResponse{},
				Summary:  "Run a BQL query and return the full result set.",
				Handler:  d.queryHandler.HandleQuery,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/query/stream",
				Response: query.QueryResponse{},
				Summary:  "Run a BQL query and stream results as they arrive.",
				Handler:  d.queryHandler.HandleQueryStream,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/query/fieldstats",
				Summary: "Compute per-field value distributions across a query's matches.",
				Handler: d.queryHandler.HandleFieldStats,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/query/validate",
				Request:  query.QueryRequest{},
				Response: query.ValidateResponse{},
				Summary:  "Parse and translate a BQL query without running it.",
				Handler:  d.queryHandler.HandleValidate,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/query/reference",
				Summary: "Return the BQL command and function reference.",
				Handler: d.queryHandler.HandleReference,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/query/fields",
				Response: api.ListResponse[string]{},
				Summary:  "List the field names available to queries.",
				Handler:  d.schemaFieldsHandler.HandleCatalog,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/logs/recent",
				Summary: "Return the most recent logs in the fractal.",
				Handler: d.queryHandler.HandleGetRecentLogs,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/logs/histogram",
				Summary: "Return quarter-hour event counts for the recent window.",
				Handler: d.queryHandler.HandleGetRecentHistogram,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/logs/by-timestamp",
				Request:  query.LogByTimestampRequest{},
				Response: map[string]interface{}{},
				Summary:  "Fetch one log's full detail by timestamp and log id.",
				Handler:  d.queryHandler.HandleGetLogByTimestamp,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/logs/fields",
				Response: map[string]interface{}{},
				Summary:  "List the field names present in the fractal's logs.",
				Handler:  d.queryHandler.HandleGetLogFields,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/status",
				Response: query.StatusResponse{},
				Summary:  "Report backend connectivity and store status.",
				Handler:  d.statusHandler.HandleStatus,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/health/clickhouse",
				Summary: "Ping ClickHouse for the connection indicator.",
				Handler: d.statusHandler.HandleHealthCheck,
			})
			// The single source of truth for what this deployment is and what it can
			// do. It replaces inferring the shape from incidental payload keys on
			// four unrelated endpoints, which could only ever answer "cluster or
			// not" and said nothing about a feature the server had refused.
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/system/topology",
				Summary: "Report the ClickHouse topology and capability probes.",
				Handler: d.handleTopology,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/system/pressure",
				Summary: "Report ingest queue depth, backpressure, and spool usage.",
				Handler: d.handlePressure,
			})

			// Health notifications
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/notifications",
				Response: api.Response[notifications.NotificationFeed]{},
				Summary:  "List health notifications.",
				Handler:  d.notificationHandler.HandleList,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/notifications/count",
				Response: api.Response[notifications.UnreadCount]{},
				Summary:  "Return the caller's unread notification count.",
				Handler:  d.notificationHandler.HandleCount,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/notifications/read",
				Summary: "Mark the caller's notifications as read.",
				Handler: d.notificationHandler.HandleMarkRead,
			})

			// Settings
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/settings",
				Response: settings.SettingsResponse{},
				Summary:  "Read the instance settings.",
				Handler:  d.settingsHandler.HandleGet,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/settings",
				Request:  settings.Settings{},
				Response: settings.SettingsResponse{},
				Summary:  "Update the instance settings.",
				Handler:  d.settingsHandler.HandleUpdate,
			})

			// Iceberg archive status + enable toggle (admin only).
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/system/archive",
				Summary: "Report archive status, lifecycle, and per-fractal footprint.",
				Handler: d.handleArchiveStatus,
			})
			r.Register(api.Route{
				Method:  http.MethodPut,
				Path:    "/system/archive/enabled",
				Request: enabledRequest{},
				Summary: "Enable or disable archiving to Iceberg.",
				Handler: d.handleSetArchiveEnabled,
			})
			// "Run now": request an out-of-schedule maintenance pass. The server does
			// not link the archiver's Iceberg stack, so it merely records the request
			// on the shared archive_maintain_status row (raw SQL, like /clear above);
			// the always-on maintain-loop claims it on its next poll and runs the pass.
			// Platform-agnostic: the maintainer is a container (Docker) or a replicas:1
			// Deployment (k8s), both polling this same Postgres row -- no k8s API access
			// or RBAC for the app tier.
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/system/archive/maintain/run",
				Summary: "Run archive maintenance now.",
				Handler: d.handleRunArchiveMaintain,
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
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/system/archive/clear",
				Summary: "Drop the archive catalog, discarding all archived data.",
				Handler: d.handleClearArchiveCatalog,
			})

			// Clear the archive spool (admin only): discard un-archived buffered data
			// on every ingest pod. The spool is durable and per-pod, so this is a
			// broadcast, not a single wipe: incrementing a global generation asks each
			// pod to reset its own spool and stamp a marker its archiver waits on
			// (see startArchiveSpool + Archiver.syncSpoolClear). Pods apply it within
			// their poll interval (~10s). Use before re-pointing to a fresh archive so
			// the old spool's tail does not drain into the new one; NOT needed when
			// migrating backends (there the tail correctly bridges the gap).
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/system/archive/spool/clear",
				Summary: "Discard the un-archived spool.",
				Handler: d.handleClearArchiveSpool,
			})

			// Distribution queue per-shard diagnostics + reset (admin only, cluster
			// mode only). See ClickHouseClient.DistributionQueueByShard/
			// ResetDistributedQueue: reset drops and recreates logs_distributed on one
			// shard, discarding that shard's queued (not-yet-forwarded) batches. Pure
			// local DDL -- no filesystem/pod access, no ON CLUSTER/Keeper coordination.
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/system/distribution-queue/shards",
				Summary: "Report the distributed insert queue per shard.",
				Handler: d.handleDistributionQueueShards,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/system/distribution-queue/reset",
				Request: resetDistributionQueueRequest{},
				Summary: "Reset a shard's distributed insert queue.",
				Handler: d.handleResetDistributionQueue,
			})

			// Advanced endpoint analysis toggle (admin only): gates the process
			// lineage/frequency materialized views (heavy per-insert triggers).
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/system/endpoint-analysis",
				Summary: "Report whether advanced endpoint analysis is enabled.",
				Handler: d.handleGetEndpointAnalysis,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/system/endpoint-analysis",
				Request: enabledRequest{},
				Summary: "Enable or disable advanced endpoint analysis.",
				Handler: d.handleSetEndpointAnalysis,
			})

			// Shared Links global toggle (admin only): master switch for public,
			// no-auth, read-only dashboard access. Default off (opt-in). When off,
			// every anonymous /shared/{token} request 404s and new links cannot be
			// created; existing links can still be listed and revoked for cleanup.
			// Readable by any authenticated user: the dashboard UI needs it to decide
			// whether to show the "Share" button. It is a non-sensitive feature flag.
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/system/shared-links",
				Summary: "Report whether shared dashboard links are enabled.",
				Handler: d.handleGetSharedLinksEnabled,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/system/shared-links",
				Request: enabledRequest{},
				Summary: "Enable or disable shared dashboard links instance-wide.",
				Handler: d.handleSetSharedLinksEnabled,
			})

			// Enqueue restore/reconcile jobs (async). The bifract-archiver run
			// process claims and executes them; this handler only writes the queue.
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/system/archive/restore",
				Request: createRestoreRequest{},
				Summary: "Start restoring an archived window into a fractal.",
				Handler: d.handleCreateRestore,
			})

			// List recent restore jobs for the admin UI (newest first).
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/system/archive/restore",
				Response: api.ListResponse[map[string]interface{}]{},
				Summary:  "List restore jobs.",
				Handler:  d.handleListRestores,
			})

			// Cancel a pending or running restore job. Moving the row out of
			// 'running' is the cancel signal: the owning worker notices on its next
			// heartbeat, issues KILL QUERY against the insert's query_id, and stops.
			// Rows already inserted stay put; re-running is idempotent (always deduped).
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/system/archive/restore/{id}/cancel",
				Summary: "Cancel a restore job.",
				Handler: d.handleCancelRestore,
			})

			// Resume a failed or cancelled restore. cursor_ts survives on the row, so
			// requeueing picks up at the first unfinished ingest-day chunk instead of
			// replaying the whole window. Safe to use even if the cursor is stale:
			// restores are always deduped and therefore idempotent.
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/system/archive/restore/{id}/resume",
				Summary: "Resume a failed or cancelled restore from its cursor.",
				Handler: d.handleResumeRestore,
			})

			// Recall: per-fractal async BQL search over the Iceberg archive.
			// Analyst+ (unlike the admin-only Restore under /system/archive):
			// searching cold storage is a normal-user read, scoped by fractal RBAC.
			// Whether Recall is available (archive enabled + spool provisioned).
			// Any authenticated user may check this so the tab can be gated for
			// analysts, who cannot read the admin-only /system/archive status.
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/recall/available",
				Summary: "Report whether Recall can run against the archive.",
				Handler: d.handleRecallAvailable,
			})

			// Pre-flight scan estimate: what a Recall over this window would open,
			// from Iceberg manifests only (no object data read). Lets the UI warn
			// before a user waits minutes on a window with tens of thousands of
			// files behind it. Analyst+, same as the search itself.
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/recall/{fractalID}/estimate",
				Summary: "Estimate what a Recall over a window would scan.",
				Handler: d.handleRecallEstimate,
			})

			// Submit a Recall search (returns the job id to poll).
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/recall/{fractalID}",
				Request: createRecallRequest{},
				Summary: "Submit a Recall search over the archive.",
				Handler: d.handleCreateRecall,
			})

			// List recent Recall jobs for a fractal (newest first, no results payload).
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/recall/{fractalID}",
				Summary: "List recent Recall jobs for a fractal.",
				Handler: d.handleListRecalls,
			})

			// Fetch one Recall job with its results (inline render / reattach).
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/recall/{fractalID}/{id}",
				Summary: "Read one Recall job and its results.",
				Handler: d.handleGetRecall,
			})

			// Cancel a Recall job. Works while pending (never claimed) or running:
			// flipping the row out of 'running' is the signal the archiver worker's
			// watcher polls for -- it then kills the ClickHouse query. Terminal jobs
			// (succeeded/failed/canceled) return 409.
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/recall/{fractalID}/{id}/cancel",
				Summary: "Cancel a running Recall job.",
				Handler: d.handleCancelRecall,
			})

			// Auth
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/auth/logout",
				Response: auth.Response{},
				Summary:  "End the current session.",
				Handler:  d.authHandler.HandleLogout,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/auth/user",
				Response: auth.Response{},
				Summary:  "Describe the authenticated caller and its current scope.",
				Handler:  d.authHandler.HandleCurrentUser,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/auth/change-password",
				Request:  auth.ChangePasswordRequest{},
				Response: auth.Response{},
				Summary:  "Change the caller's own password.",
				Handler:  d.authHandler.HandleChangePassword,
			})
			r.Register(api.Route{
				Method:   http.MethodPatch,
				Path:     "/auth/preferences",
				Request:  auth.UpdatePreferencesRequest{},
				Response: auth.Response{},
				Summary:  "Update the caller's display preferences.",
				Handler:  d.authHandler.HandleUpdatePreferences,
			})

			// Comments
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/comments",
				Request:  comments.CreateCommentRequest{},
				Response: comments.Response{},
				Summary:  "Create a comment on a log.",
				Handler:  d.commentHandler.HandleCreateComment,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/comments/flat",
				Response: api.ListResponse[storage.Comment]{},
				Summary:  "List comments individually rather than grouped by log.",
				Handler:  d.commentHandler.HandleGetFlatComments,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/comments/bulk-add-tag",
				Request: comments.BulkTagRequest{},
				Summary: "Add a tag to several comments at once.",
				Handler: d.commentHandler.HandleBulkAddTag,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/comments/bulk-remove-tag",
				Request: comments.BulkTagRequest{},
				Summary: "Remove a tag from several comments at once.",
				Handler: d.commentHandler.HandleBulkRemoveTag,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/comments/bulk-delete",
				Request: comments.BulkDeleteRequest{},
				Summary: "Delete several comments at once.",
				Handler: d.commentHandler.HandleBulkDeleteComments,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/comments/tags",
				Response: comments.Response{},
				Summary:  "List the tags in use across the scope's comments.",
				Handler:  d.commentHandler.HandleGetTags,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/comments/graph/log-fields",
				Request:  comments.LogFieldsRequest{},
				Response: comments.Response{},
				Summary:  "Batch-fetch parsed field data for a set of logs.",
				Handler:  d.commentHandler.HandleGetLogFields,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/comments/{id}",
				Response: comments.Response{},
				Summary:  "Read one comment.",
				Handler:  d.commentHandler.HandleGetComment,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/comments/{id}",
				Request:  comments.UpdateCommentRequest{},
				Response: comments.Response{},
				Summary:  "Update one comment.",
				Handler:  d.commentHandler.HandleUpdateComment,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/comments/{id}",
				Response: comments.Response{},
				Summary:  "Delete one comment.",
				Handler:  d.commentHandler.HandleDeleteComment,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/logs/{log_id}/comments",
				Response: comments.Response{},
				Summary:  "List the comments on one log.",
				Handler:  d.commentHandler.HandleGetLogComments,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/logs/{log_id}/comments",
				Response: comments.Response{},
				Summary:  "Delete every comment on one log.",
				Handler:  d.commentHandler.HandleDeleteCommentsByLogID,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/logs/commented",
				Response: api.ListResponse[map[string]interface{}]{},
				Summary:  "List the logs that carry comments.",
				Handler:  d.commentHandler.HandleGetCommentedLogs,
			})

			// Notebooks (API keys require "notebook" permission)
			r.Group(func(r api.Router) {
				r.Use(auth.RequireAPIKeyPermission("notebook"))
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/notebooks",
					Response: api.ListResponse[storage.Notebook]{},
					Summary:  "List notebooks in scope.",
					Handler:  d.notebookHandler.HandleListNotebooks,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/notebooks",
					Request:  notebooks.CreateNotebookRequest{},
					Response: notebooks.Response{},
					Summary:  "Create a notebook.",
					Handler:  d.notebookHandler.HandleCreateNotebook,
				})
				r.Register(api.Route{
					Method:  http.MethodGet,
					Path:    "/notebooks/ai-status",
					Summary: "Report whether AI summary generation is available.",
					Handler: d.notebookHandler.HandleAIStatus,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/notebooks/import",
					Response: notebooks.Response{},
					Summary:  "Import a notebook from YAML.",
					Handler:  d.notebookHandler.HandleImportNotebook,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/notebooks/generate-from-comments",
					Request:  notebooks.GenerateFromCommentsRequest{},
					Response: notebooks.Response{},
					Summary:  "Build a notebook from every comment carrying a tag.",
					Handler:  d.notebookHandler.HandleGenerateFromComments,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/notebooks/{id}",
					Response: notebooks.Response{},
					Summary:  "Read one notebook with its sections.",
					Handler:  d.notebookHandler.HandleGetNotebook,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/notebooks/{id}",
					Request:  notebooks.UpdateNotebookRequest{},
					Response: notebooks.Response{},
					Summary:  "Update a notebook's metadata.",
					Handler:  d.notebookHandler.HandleUpdateNotebook,
				})
				r.Register(api.Route{
					Method:   http.MethodDelete,
					Path:     "/notebooks/{id}",
					Response: notebooks.Response{},
					Summary:  "Delete a notebook.",
					Handler:  d.notebookHandler.HandleDeleteNotebook,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/notebooks/{id}/sections",
					Request:  notebooks.CreateSectionRequest{},
					Response: notebooks.Response{},
					Summary:  "Add a section to a notebook.",
					Handler:  d.notebookHandler.HandleCreateSection,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/notebooks/{id}/sections/{section_id}",
					Request:  notebooks.UpdateSectionRequest{},
					Response: notebooks.Response{},
					Summary:  "Update a notebook section.",
					Handler:  d.notebookHandler.HandleUpdateSection,
				})
				r.Register(api.Route{
					Method:   http.MethodDelete,
					Path:     "/notebooks/{id}/sections/{section_id}",
					Response: notebooks.Response{},
					Summary:  "Delete a notebook section.",
					Handler:  d.notebookHandler.HandleDeleteSection,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/notebooks/{id}/sections/{section_id}/execute",
					Request:  notebooks.ExecuteQueryRequest{},
					Response: notebooks.Response{},
					Summary:  "Run a query section and cache its results.",
					Handler:  d.notebookHandler.HandleExecuteQuerySection,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/notebooks/{id}/sections/{section_id}/summarize",
					Response: notebooks.Response{},
					Summary:  "Generate an AI summary of a notebook's other sections.",
					Handler:  d.notebookHandler.HandleGenerateAISummary,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/notebooks/{id}/sections/{section_id}/results",
					Request:  notebooks.UpdateSectionResultsRequest{},
					Response: notebooks.Response{},
					Summary:  "Replace a query section's cached results.",
					Handler:  d.notebookHandler.HandleUpdateSectionResults,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/notebooks/{id}/sections/reorder",
					Request:  notebooks.ReorderSectionsRequest{},
					Response: notebooks.Response{},
					Summary:  "Reorder a notebook's sections.",
					Handler:  d.notebookHandler.HandleReorderSections,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/notebooks/{id}/variables",
					Request:  notebooks.UpdateVariablesRequest{},
					Response: notebooks.Response{},
					Summary:  "Update a notebook's query variables.",
					Handler:  d.notebookHandler.HandleUpdateVariables,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/notebooks/{id}/presence",
					Response: notebooks.Response{},
					Summary:  "Report the caller as viewing a notebook.",
					Handler:  d.notebookHandler.HandleUpdatePresence,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/notebooks/{id}/presence",
					Response: notebooks.Response{},
					Summary:  "List who is currently viewing a notebook.",
					Handler:  d.notebookHandler.HandleGetPresence,
				})
				r.Register(api.Route{
					Method:  http.MethodGet,
					Path:    "/notebooks/{id}/tags",
					Summary: "List the tags used across a notebook's sections.",
					Handler: d.notebookHandler.HandleGetNotebookTags,
				})
				r.Register(api.Route{
					Method:  http.MethodGet,
					Path:    "/notebooks/{id}/export",
					Summary: "Export a notebook as YAML.",
					Handler: d.notebookHandler.HandleExportNotebook,
				})
				r.Register(api.Route{
					Method:  http.MethodGet,
					Path:    "/notebooks/{id}/events",
					Summary: "Stream a notebook's live edits and presence.",
					Handler: d.notebookHandler.HandleSSE,
				})
			})

			// Dashboards (API keys require "dashboard" permission)
			r.Group(func(r api.Router) {
				r.Use(auth.RequireAPIKeyPermission("dashboard"))
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/dashboards",
					Response: api.ListResponse[storage.Dashboard]{},
					Summary:  "List dashboards in scope.",
					Handler:  d.dashboardHandler.HandleListDashboards,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/dashboards",
					Request:  dashboards.CreateDashboardRequest{},
					Response: dashboards.Response{},
					Summary:  "Create a dashboard.",
					Handler:  d.dashboardHandler.HandleCreateDashboard,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/dashboards/{id}",
					Response: dashboards.Response{},
					Summary:  "Read one dashboard with its widgets.",
					Handler:  d.dashboardHandler.HandleGetDashboard,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/dashboards/{id}",
					Request:  dashboards.UpdateDashboardRequest{},
					Response: dashboards.Response{},
					Summary:  "Update a dashboard's metadata.",
					Handler:  d.dashboardHandler.HandleUpdateDashboard,
				})
				r.Register(api.Route{
					Method:   http.MethodDelete,
					Path:     "/dashboards/{id}",
					Response: dashboards.Response{},
					Summary:  "Delete a dashboard.",
					Handler:  d.dashboardHandler.HandleDeleteDashboard,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/dashboards/{id}/widgets",
					Request:  dashboards.CreateWidgetRequest{},
					Response: dashboards.Response{},
					Summary:  "Add a widget to a dashboard.",
					Handler:  d.dashboardHandler.HandleCreateWidget,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/dashboards/{id}/widgets/{widget_id}",
					Request:  dashboards.UpdateWidgetRequest{},
					Response: dashboards.Response{},
					Summary:  "Update a widget.",
					Handler:  d.dashboardHandler.HandleUpdateWidget,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/dashboards/{id}/widgets/{widget_id}/layout",
					Request:  dashboards.UpdateWidgetLayoutRequest{},
					Response: dashboards.Response{},
					Summary:  "Move or resize a widget.",
					Handler:  d.dashboardHandler.HandleUpdateWidgetLayout,
				})
				r.Register(api.Route{
					Method:   http.MethodDelete,
					Path:     "/dashboards/{id}/widgets/{widget_id}",
					Response: dashboards.Response{},
					Summary:  "Delete a widget.",
					Handler:  d.dashboardHandler.HandleDeleteWidget,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/dashboards/{id}/variables",
					Request:  dashboards.UpdateVariablesRequest{},
					Response: dashboards.Response{},
					Summary:  "Update a dashboard's query variables.",
					Handler:  d.dashboardHandler.HandleUpdateVariables,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/dashboards/{id}/refresh-interval",
					Request:  dashboards.UpdateRefreshIntervalRequest{},
					Response: dashboards.Response{},
					Summary:  "Set a dashboard's server-side refresh cadence.",
					Handler:  d.dashboardHandler.HandleUpdateRefreshInterval,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/dashboards/{id}/execute",
					Response: dashboards.Response{},
					Summary:  "Run every widget on a dashboard and push the results to viewers.",
					Handler:  d.dashboardHandler.HandleExecuteDashboard,
				})
				r.Register(api.Route{
					Method:  http.MethodPost,
					Path:    "/dashboards/{id}/widgets/{widget_id}/execute",
					Request: dashboards.ExecuteWidgetRequest{},
					Summary: "Run one widget and persist its results.",
					Handler: d.dashboardHandler.HandleExecuteWidget,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/dashboards/{id}/presence",
					Response: dashboards.Response{},
					Summary:  "Report the caller as viewing a dashboard.",
					Handler:  d.dashboardHandler.HandleUpdatePresence,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/dashboards/{id}/presence",
					Response: dashboards.Response{},
					Summary:  "List who is currently viewing a dashboard.",
					Handler:  d.dashboardHandler.HandleGetPresence,
				})
				r.Register(api.Route{
					Method:  http.MethodGet,
					Path:    "/dashboards/{id}/export",
					Summary: "Export a dashboard as YAML.",
					Handler: d.dashboardHandler.HandleExportDashboard,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/dashboards/import",
					Response: dashboards.Response{},
					Summary:  "Import a dashboard from YAML.",
					Handler:  d.dashboardHandler.HandleImportDashboard,
				})
				r.Register(api.Route{
					Method:  http.MethodGet,
					Path:    "/dashboards/{id}/events",
					Summary: "Stream a dashboard's live edits, results, and presence.",
					Handler: d.dashboardHandler.HandleSSE,
				})
				// Shared Links management (create/revoke require analyst+ on the
				// dashboard's scope; list is viewer+). The anonymous read route is
				// registered separately in the public block below.
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/dashboards/{id}/shared-links",
					Response: dashboards.Response{},
					Summary:  "List a dashboard's shared links, without their tokens.",
					Handler:  d.dashboardHandler.HandleListSharedLinks,
				})
				r.Register(api.Route{
					Method:  http.MethodPost,
					Path:    "/dashboards/{id}/shared-links",
					Request: dashboards.CreateSharedLinkRequest{},
					Summary: "Mint a shared link for a dashboard.",
					Handler: d.dashboardHandler.HandleCreateSharedLink,
				})
				r.Register(api.Route{
					Method:   http.MethodDelete,
					Path:     "/dashboards/{id}/shared-links/{link_id}",
					Response: dashboards.Response{},
					Summary:  "Revoke a shared link.",
					Handler:  d.dashboardHandler.HandleRevokeSharedLink,
				})
			})

			// Alert management (API keys require "alert_manage" permission)
			r.Group(func(r api.Router) {
				r.Use(auth.RequireAPIKeyPermission("alert_manage"))
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/alerts",
					Response: api.ListResponse[*alerts.Alert]{},
					Summary:  "List alerts in scope.",
					Handler:  d.alertHandler.HandleListAlerts,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/alerts",
					Request:  alerts.AlertCreateRequest{},
					Response: api.Response[*alerts.Alert]{},
					Summary:  "Create an alert.",
					Handler:  d.alertHandler.HandleCreateAlert,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/alerts/{id}",
					Response: api.Response[*alerts.Alert]{},
					Summary:  "Read one alert.",
					Handler:  d.alertHandler.HandleGetAlert,
				})
				r.Register(api.Route{
					Method:   http.MethodPut,
					Path:     "/alerts/{id}",
					Request:  alerts.AlertUpdateRequest{},
					Response: api.Response[*alerts.Alert]{},
					Summary:  "Update an alert.",
					Handler:  d.alertHandler.HandleUpdateAlert,
				})
				r.Register(api.Route{
					Method:  http.MethodDelete,
					Path:    "/alerts/{id}",
					Summary: "Delete an alert.",
					Handler: d.alertHandler.HandleDeleteAlert,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/alerts/import",
					Request:  alerts.ImportYAMLRequest{},
					Response: api.Response[*alerts.Alert]{},
					Summary:  "Import an alert from YAML or Sigma.",
					Handler:  d.alertHandler.HandleImportYAML,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/alerts/batch-toggle",
					Request:  alerts.BatchToggleAlertsRequest{},
					Response: api.Response[map[string]int]{},
					Summary:  "Enable or disable a set of alerts.",
					Handler:  d.alertHandler.HandleBatchToggleAlerts,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/alerts/{id}/executions",
					Response: api.ListResponse[map[string]interface{}]{},
					Summary:  "List an alert's evaluation history.",
					Handler:  d.alertHandler.HandleGetExecutions,
				})

				// MITRE ATT&CK coverage, derived from the attack.* labels rules
				// already carry. Read-only, scoped to the session's fractal/prism.
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/attack/matrix",
					Response: api.Response[*attack.Matrix]{},
					Summary:  "Return the embedded ATT&CK matrix.",
					Handler:  d.alertHandler.HandleAttackMatrix,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/attack/coverage",
					Response: api.Response[*attack.Coverage]{},
					Summary:  "Return per-technique rule counts and the coverage summary.",
					Handler:  d.alertHandler.HandleAttackCoverage,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/attack/techniques/{id}/rules",
					Response: api.Response[alerts.TechniqueRules]{},
					Summary:  "List the rules covering one technique.",
					Handler:  d.alertHandler.HandleAttackTechniqueRules,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/attack/techniques/{id}/gap",
					Response: api.Response[alerts.Gap]{},
					Summary:  "List candidate rules for one uncovered technique.",
					Handler:  d.alertHandler.HandleAttackTechniqueGap,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/attack/gaps",
					Response: api.Response[alerts.AttackGaps]{},
					Summary:  "Rank uncovered techniques by what can be covered today.",
					Handler:  d.alertHandler.HandleAttackGaps,
				})
				r.Register(api.Route{
					Method:  http.MethodGet,
					Path:    "/attack/layer",
					Summary: "Export coverage as an ATT&CK Navigator layer.",
					Handler: d.alertHandler.HandleAttackLayer,
				})
			})

			// Webhook management
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/webhooks",
				Response: api.ListResponse[*alerts.WebhookAction]{},
				Summary:  "List webhook actions in scope.",
				Handler:  d.alertHandler.HandleListWebhooks,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/webhooks",
				Request:  alerts.WebhookCreateRequest{},
				Response: api.Response[*alerts.WebhookAction]{},
				Summary:  "Create a webhook action.",
				Handler:  d.alertHandler.HandleCreateWebhook,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/webhooks/{id}",
				Response: api.Response[*alerts.WebhookAction]{},
				Summary:  "Read one webhook action.",
				Handler:  d.alertHandler.HandleGetWebhook,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/webhooks/{id}",
				Request:  alerts.WebhookUpdateRequest{},
				Response: api.Response[*alerts.WebhookAction]{},
				Summary:  "Update a webhook action.",
				Handler:  d.alertHandler.HandleUpdateWebhook,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/webhooks/{id}",
				Summary: "Delete a webhook action.",
				Handler: d.alertHandler.HandleDeleteWebhook,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/webhooks/{id}/test",
				Response: api.Response[*alerts.WebhookResult]{},
				Summary:  "Send a test payload to a webhook.",
				Handler:  d.alertHandler.HandleTestWebhook,
			})

			// Fractal action management
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractal-actions",
				Response: api.ListResponse[alerts.FractalAction]{},
				Summary:  "List fractal actions in scope.",
				Handler:  d.alertHandler.HandleListFractalActions,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/fractal-actions",
				Request:  alerts.FractalActionCreateRequest{},
				Response: api.Response[*alerts.FractalAction]{},
				Summary:  "Create a fractal action.",
				Handler:  d.alertHandler.HandleCreateFractalAction,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractal-actions/{id}",
				Response: api.Response[*alerts.FractalAction]{},
				Summary:  "Read one fractal action.",
				Handler:  d.alertHandler.HandleGetFractalAction,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/fractal-actions/{id}",
				Request:  alerts.FractalActionUpdateRequest{},
				Response: api.Response[*alerts.FractalAction]{},
				Summary:  "Update a fractal action.",
				Handler:  d.alertHandler.HandleUpdateFractalAction,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/fractal-actions/{id}",
				Summary: "Delete a fractal action.",
				Handler: d.alertHandler.HandleDeleteFractalAction,
			})

			// Email action management
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/email-actions",
				Response: api.ListResponse[alerts.EmailAction]{},
				Summary:  "List email actions in scope.",
				Handler:  d.alertHandler.HandleListEmailActions,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/email-actions",
				Request:  alerts.EmailActionCreateRequest{},
				Response: api.Response[*alerts.EmailAction]{},
				Summary:  "Create an email action.",
				Handler:  d.alertHandler.HandleCreateEmailAction,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/email-actions/{id}",
				Response: api.Response[*alerts.EmailAction]{},
				Summary:  "Read one email action.",
				Handler:  d.alertHandler.HandleGetEmailAction,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/email-actions/{id}",
				Request:  alerts.EmailActionUpdateRequest{},
				Response: api.Response[*alerts.EmailAction]{},
				Summary:  "Update an email action.",
				Handler:  d.alertHandler.HandleUpdateEmailAction,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/email-actions/{id}",
				Summary: "Delete an email action.",
				Handler: d.alertHandler.HandleDeleteEmailAction,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/email-actions/{id}/test",
				Response: api.Response[alerts.ActionTestResult]{},
				Summary:  "Send a test message through an email action.",
				Handler:  d.alertHandler.HandleTestEmailAction,
			})

			// SMTP settings
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/smtp-settings",
				Response: api.Response[alerts.SMTPConfig]{},
				Summary:  "Read the SMTP configuration, without the password.",
				Handler:  d.alertHandler.HandleGetSMTPSettings,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/smtp-settings",
				Request: alerts.SMTPConfig{},
				Summary: "Update the SMTP configuration.",
				Handler: d.alertHandler.HandleUpdateSMTPSettings,
			})

			// Prism management
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/prisms",
				Response: api.ListResponse[*prisms.Prism]{},
				Summary:  "List the prisms the caller can reach.",
				Handler:  d.prismHandler.HandleListPrisms,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/prisms",
				Request:  prisms.PrismRequest{},
				Response: api.Response[*prisms.Prism]{},
				Summary:  "Create a prism.",
				Handler:  d.prismHandler.HandleCreatePrism,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/prisms/{id}",
				Response: api.Response[*prisms.Prism]{},
				Summary:  "Read one prism.",
				Handler:  d.prismHandler.HandleGetPrism,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/prisms/{id}",
				Request:  prisms.PrismRequest{},
				Response: api.Response[*prisms.Prism]{},
				Summary:  "Update a prism.",
				Handler:  d.prismHandler.HandleUpdatePrism,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/prisms/{id}",
				Response: api.Response[map[string]bool]{},
				Summary:  "Delete a prism.",
				Handler:  d.prismHandler.HandleDeletePrism,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/prisms/{id}/select",
				Response: api.Response[prisms.SelectedPrism]{},
				Summary:  "Set the session's selected prism.",
				Handler:  d.prismHandler.HandleSelectPrism,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/prisms/{id}/members",
				Request:  prisms.AddMemberRequest{},
				Response: api.Response[*prisms.Prism]{},
				Summary:  "Add a fractal to a prism.",
				Handler:  d.prismHandler.HandleAddMember,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/prisms/{id}/members/{fractalID}",
				Response: api.Response[*prisms.Prism]{},
				Summary:  "Remove a fractal from a prism.",
				Handler:  d.prismHandler.HandleRemoveMember,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/prisms/{id}/permissions",
				Response: api.ListResponse[storage.PrismPermission]{},
				Summary:  "List who has access to a prism.",
				Handler:  d.prismHandler.HandleListPrismPermissions,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/prisms/{id}/permissions",
				Request:  prisms.GrantPermissionRequest{},
				Response: api.Response[*storage.PrismPermission]{},
				Summary:  "Grant a user or group access to a prism.",
				Handler:  d.prismHandler.HandleGrantPrismPermission,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/prisms/{id}/permissions/{permId}",
				Request:  prisms.UpdatePermissionRequest{},
				Response: api.Response[map[string]string]{},
				Summary:  "Change a prism permission's role.",
				Handler:  d.prismHandler.HandleUpdatePrismPermission,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/prisms/{id}/permissions/{permId}",
				Response: api.Response[map[string]string]{},
				Summary:  "Revoke a prism permission.",
				Handler:  d.prismHandler.HandleRevokePrismPermission,
			})

			// Fractal management
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractals",
				Response: api.Response[fractals.FractalListResponse]{},
				Summary:  "List the fractals and prisms the caller can reach.",
				Handler:  d.fractalHandler.HandleListFractals,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/fractals",
				Request:  fractals.CreateFractalRequest{},
				Response: api.Response[*fractals.Fractal]{},
				Summary:  "Create a fractal.",
				Handler:  d.fractalHandler.HandleCreateFractal,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractals/{id}",
				Response: api.Response[map[string]interface{}]{},
				Summary:  "Read one fractal.",
				Handler:  d.fractalHandler.HandleGetFractal,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/fractals/{id}",
				Request:  fractals.UpdateFractalRequest{},
				Response: api.Response[*fractals.Fractal]{},
				Summary:  "Update a fractal.",
				Handler:  d.fractalHandler.HandleUpdateFractal,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/fractals/{id}",
				Summary: "Delete a fractal and its data.",
				Handler: d.fractalHandler.HandleDeleteFractal,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/fractals/{id}/select",
				Response: api.Response[fractals.SelectedFractal]{},
				Summary:  "Set the session's selected fractal.",
				Handler:  d.fractalHandler.HandleSelectFractal,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractals/{id}/stats",
				Response: api.Response[*fractals.FractalStats]{},
				Summary:  "Return a fractal's row counts and storage usage.",
				Handler:  d.fractalHandler.HandleGetStats,
			})
			r.Register(api.Route{
				Method:  http.MethodPut,
				Path:    "/fractals/{id}/retention",
				Request: fractals.UpdateRetentionRequest{},
				Summary: "Set a fractal's hot-storage retention window.",
				Handler: d.fractalHandler.HandleSetRetention,
			})
			r.Register(api.Route{
				Method:  http.MethodPut,
				Path:    "/fractals/{id}/archive-retention",
				Request: fractals.UpdateArchiveRetentionRequest{},
				Summary: "Set a fractal's archive retention window.",
				Handler: d.fractalHandler.HandleSetArchiveRetention,
			})
			r.Register(api.Route{
				Method:  http.MethodPut,
				Path:    "/fractals/{id}/disk-quota",
				Request: fractals.UpdateDiskQuotaRequest{},
				Summary: "Set a fractal's disk quota and enforcement action.",
				Handler: d.fractalHandler.HandleSetDiskQuota,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/fractals/stats/refresh",
				Summary: "Recompute statistics for every fractal.",
				Handler: d.fractalHandler.HandleRefreshStats,
			})

			// Fractal permissions (fractal admin or tenant admin, checked in handler)
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractals/{id}/permissions",
				Response: api.ListResponse[storage.FractalPermission]{},
				Summary:  "List who has access to a fractal.",
				Handler:  d.fractalHandler.HandleListPermissions,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/fractals/{id}/permissions",
				Request:  fractals.GrantPermissionRequest{},
				Response: api.Response[*storage.FractalPermission]{},
				Summary:  "Grant a user or group access to a fractal.",
				Handler:  d.fractalHandler.HandleGrantPermission,
			})
			r.Register(api.Route{
				Method:  http.MethodPut,
				Path:    "/fractals/{id}/permissions/{permId}",
				Request: fractals.UpdatePermissionRequest{},
				Summary: "Change a fractal permission's role.",
				Handler: d.fractalHandler.HandleUpdatePermission,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/fractals/{id}/permissions/{permId}",
				Summary: "Revoke a fractal permission.",
				Handler: d.fractalHandler.HandleRevokePermission,
			})

			// Groups (tenant admin only, checked in handler)
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/groups",
				Summary: "List groups.",
				Handler: d.groupHandler.HandleListGroups,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/groups",
				Request: groups.GroupRequest{},
				Summary: "Create a group.",
				Handler: d.groupHandler.HandleCreateGroup,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/groups/{id}",
				Summary: "Read one group.",
				Handler: d.groupHandler.HandleGetGroup,
			})
			r.Register(api.Route{
				Method:  http.MethodPut,
				Path:    "/groups/{id}",
				Request: groups.GroupRequest{},
				Summary: "Update a group.",
				Handler: d.groupHandler.HandleUpdateGroup,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/groups/{id}",
				Summary: "Delete a group.",
				Handler: d.groupHandler.HandleDeleteGroup,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/groups/{id}/members",
				Summary: "List a group's members.",
				Handler: d.groupHandler.HandleListMembers,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/groups/{id}/members",
				Request: groups.AddMemberRequest{},
				Summary: "Add a user to a group.",
				Handler: d.groupHandler.HandleAddMember,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/groups/{id}/members/{username}",
				Summary: "Remove a user from a group.",
				Handler: d.groupHandler.HandleRemoveMember,
			})

			// API Key management (fractal-scoped)
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractals/{id}/api-keys",
				Response: api.ListResponse[apikeys.APIKey]{},
				Summary:  "List a fractal's API keys.",
				Handler:  d.apiKeyHandler.HandleListAPIKeys,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/fractals/{id}/api-keys",
				Request:  apikeys.CreateAPIKeyRequest{},
				Response: api.Response[apikeys.CreateAPIKeyResponse]{},
				Summary:  "Create an API key scoped to a fractal.",
				Handler:  d.apiKeyHandler.HandleCreateAPIKey,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractals/{id}/api-keys/{keyId}",
				Response: api.Response[*apikeys.APIKey]{},
				Summary:  "Read one fractal API key.",
				Handler:  d.apiKeyHandler.HandleGetAPIKey,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/fractals/{id}/api-keys/{keyId}",
				Request:  apikeys.UpdateAPIKeyRequest{},
				Response: api.Response[*apikeys.APIKey]{},
				Summary:  "Update a fractal API key.",
				Handler:  d.apiKeyHandler.HandleUpdateAPIKey,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/fractals/{id}/api-keys/{keyId}",
				Summary: "Delete a fractal API key.",
				Handler: d.apiKeyHandler.HandleDeleteAPIKey,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/fractals/{id}/api-keys/{keyId}/toggle",
				Response: api.Response[map[string]interface{}]{},
				Summary:  "Activate or deactivate a fractal API key.",
				Handler:  d.apiKeyHandler.HandleToggleAPIKey,
			})

			// API Key management (prism-scoped)
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/prisms/{id}/api-keys",
				Response: api.ListResponse[apikeys.APIKey]{},
				Summary:  "List a prism's API keys.",
				Handler:  d.apiKeyHandler.HandleListPrismAPIKeys,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/prisms/{id}/api-keys",
				Request:  apikeys.CreateAPIKeyRequest{},
				Response: api.Response[apikeys.CreateAPIKeyResponse]{},
				Summary:  "Create an API key scoped to a prism.",
				Handler:  d.apiKeyHandler.HandleCreatePrismAPIKey,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/prisms/{id}/api-keys/{keyId}",
				Response: api.Response[*apikeys.APIKey]{},
				Summary:  "Read one prism API key.",
				Handler:  d.apiKeyHandler.HandleGetPrismAPIKey,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/prisms/{id}/api-keys/{keyId}",
				Request:  apikeys.UpdateAPIKeyRequest{},
				Response: api.Response[*apikeys.APIKey]{},
				Summary:  "Update a prism API key.",
				Handler:  d.apiKeyHandler.HandleUpdatePrismAPIKey,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/prisms/{id}/api-keys/{keyId}",
				Summary: "Delete a prism API key.",
				Handler: d.apiKeyHandler.HandleDeletePrismAPIKey,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/prisms/{id}/api-keys/{keyId}/toggle",
				Response: api.Response[map[string]interface{}]{},
				Summary:  "Activate or deactivate a prism API key.",
				Handler:  d.apiKeyHandler.HandleTogglePrismAPIKey,
			})

			// Ingest Token management
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractals/{id}/ingest-tokens",
				Response: api.ListResponse[ingesttokens.IngestToken]{},
				Summary:  "List a fractal's ingest tokens.",
				Handler:  d.ingestTokenHandler.HandleListTokens,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/fractals/{id}/ingest-tokens",
				Request:  ingesttokens.CreateTokenRequest{},
				Response: api.Response[ingesttokens.CreateTokenResponse]{},
				Summary:  "Create an ingest token for a fractal.",
				Handler:  d.ingestTokenHandler.HandleCreateToken,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/fractals/{id}/ingest-tokens/{tokenId}",
				Response: api.Response[*ingesttokens.IngestToken]{},
				Summary:  "Read one ingest token.",
				Handler:  d.ingestTokenHandler.HandleGetToken,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/fractals/{id}/ingest-tokens/{tokenId}",
				Request:  ingesttokens.UpdateTokenRequest{},
				Response: api.Response[*ingesttokens.IngestToken]{},
				Summary:  "Update an ingest token.",
				Handler:  d.ingestTokenHandler.HandleUpdateToken,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/fractals/{id}/ingest-tokens/{tokenId}",
				Summary: "Delete an ingest token.",
				Handler: d.ingestTokenHandler.HandleDeleteToken,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/fractals/{id}/ingest-tokens/{tokenId}/toggle",
				Response: api.Response[map[string]interface{}]{},
				Summary:  "Activate or deactivate an ingest token.",
				Handler:  d.ingestTokenHandler.HandleToggleToken,
			})

			// Dictionaries
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/dictionaries",
				Response: api.ListResponse[*dictionaries.Dictionary]{},
				Summary:  "List dictionaries in scope.",
				Handler:  d.dictionaryHandler.HandleListDictionaries,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/dictionaries",
				Request:  dictionaries.CreateDictionaryRequest{},
				Response: api.Response[*dictionaries.Dictionary]{},
				Summary:  "Create a dictionary.",
				Handler:  d.dictionaryHandler.HandleCreateDictionary,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/dictionaries/{id}",
				Response: api.Response[*dictionaries.Dictionary]{},
				Summary:  "Read one dictionary's definition.",
				Handler:  d.dictionaryHandler.HandleGetDictionary,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/dictionaries/{id}",
				Request:  dictionaries.UpdateDictionaryRequest{},
				Response: api.Response[*dictionaries.Dictionary]{},
				Summary:  "Update a dictionary's definition.",
				Handler:  d.dictionaryHandler.HandleUpdateDictionary,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/dictionaries/{id}",
				Response: api.Response[map[string]bool]{},
				Summary:  "Delete a dictionary.",
				Handler:  d.dictionaryHandler.HandleDeleteDictionary,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/dictionaries/{id}/data",
				Response: api.ListResponse[dictionaries.DictionaryRow]{},
				Summary:  "Read a dictionary's rows.",
				Handler:  d.dictionaryHandler.HandleGetRows,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/dictionaries/{id}/data",
				Request:  dictionaries.UpsertRowsRequest{},
				Response: api.Response[map[string]int]{},
				Summary:  "Insert or update dictionary rows.",
				Handler:  d.dictionaryHandler.HandleUpsertRows,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/dictionaries/{id}/data/{key}",
				Response: api.Response[map[string]bool]{},
				Summary:  "Delete one dictionary row.",
				Handler:  d.dictionaryHandler.HandleDeleteRow,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/dictionaries/{id}/import",
				Response: api.Response[map[string]int]{},
				Summary:  "Load dictionary rows from an uploaded CSV.",
				Handler:  d.dictionaryHandler.HandleImportCSV,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/dictionaries/{id}/export",
				Summary: "Download a dictionary's rows as CSV.",
				Handler: d.dictionaryHandler.HandleExportCSV,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/dictionaries/{id}/columns",
				Request:  dictionaries.AddColumnRequest{},
				Response: api.Response[*dictionaries.Dictionary]{},
				Summary:  "Add a column to a dictionary.",
				Handler:  d.dictionaryHandler.HandleAddColumn,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/dictionaries/{id}/columns/{name}",
				Response: api.Response[*dictionaries.Dictionary]{},
				Summary:  "Remove a column from a dictionary.",
				Handler:  d.dictionaryHandler.HandleRemoveColumn,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/dictionaries/{id}/columns/{name}/key",
				Response: api.Response[*dictionaries.Dictionary]{},
				Summary:  "Make a column part of the dictionary's key.",
				Handler:  d.dictionaryHandler.HandleSetColumnKey,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/dictionaries/{id}/columns/{name}/key",
				Response: api.Response[*dictionaries.Dictionary]{},
				Summary:  "Remove a column from the dictionary's key.",
				Handler:  d.dictionaryHandler.HandleUnsetColumnKey,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/dictionaries/{id}/reload",
				Response: api.Response[map[string]bool]{},
				Summary:  "Force ClickHouse to reload a dictionary.",
				Handler:  d.dictionaryHandler.HandleReloadDictionary,
			})

			// Analytics models
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/models",
				Response: api.ListResponse[*models.Model]{},
				Summary:  "List models in scope.",
				Handler:  d.modelHandler.HandleList,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models",
				Request:  models.CreateRequest{},
				Response: api.Response[*models.Model]{},
				Summary:  "Create a model.",
				Handler:  d.modelHandler.HandleCreate,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models/test-extraction",
				Request:  models.TestExtractionRequest{},
				Response: api.Response[models.ExtractionTest]{},
				Summary:  "Run a model's extraction against recent logs.",
				Handler:  d.modelHandler.HandleTestExtraction,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models/generate-query",
				Request:  models.GenerateQueryRequest{},
				Response: api.Response[map[string]string]{},
				Summary:  "Generate the BQL a model definition implies.",
				Handler:  d.modelHandler.HandleGenerateQuery,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models/parse-query",
				Request:  models.ParseQueryRequest{},
				Response: api.Response[models.ParsedSource]{},
				Summary:  "Lower a BQL query into a model's filter and extraction.",
				Handler:  d.modelHandler.HandleParseQuery,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models/preview",
				Request:  models.PreviewRequest{},
				Response: api.Response[*models.PreviewResult]{},
				Summary:  "Estimate a model's output before creating it.",
				Handler:  d.modelHandler.HandlePreview,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models/import",
				Response: api.Response[*models.Model]{},
				Summary:  "Create a model from a YAML definition.",
				Handler:  d.modelHandler.HandleImport,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/models/{id}",
				Response: api.Response[models.ModelDetail]{},
				Summary:  "Read one model.",
				Handler:  d.modelHandler.HandleGet,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/models/{id}",
				Request:  models.UpdateRequest{},
				Response: api.Response[*models.Model]{},
				Summary:  "Update a model.",
				Handler:  d.modelHandler.HandleUpdate,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/models/{id}",
				Response: api.Response[map[string]bool]{},
				Summary:  "Delete a model and drop its ClickHouse objects.",
				Handler:  d.modelHandler.HandleDelete,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/models/{id}/data",
				Response: api.ListResponse[map[string]interface{}]{},
				Summary:  "Read a model's rows with their scores.",
				Handler:  d.modelHandler.HandleGetData,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/models/{id}/stats",
				Response: api.Response[map[string]interface{}]{},
				Summary:  "Return a model's aggregate statistics.",
				Handler:  d.modelHandler.HandleGetStats,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/models/{id}/histogram",
				Response: api.Response[map[string]interface{}]{},
				Summary:  "Return a model's score distribution.",
				Handler:  d.modelHandler.HandleGetHistogram,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/models/{id}/export",
				Summary: "Export a model definition as YAML.",
				Handler: d.modelHandler.HandleExport,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models/{id}/enable-alert",
				Response: api.Response[map[string]bool]{},
				Summary:  "Enable the alert backing a model.",
				Handler:  d.modelHandler.HandleEnableAlert,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models/{id}/disable-alert",
				Response: api.Response[map[string]bool]{},
				Summary:  "Pause the alert backing a model.",
				Handler:  d.modelHandler.HandleDisableAlert,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models/{id}/backfill",
				Request:  models.BackfillRequest{},
				Response: api.Response[*models.Model]{},
				Summary:  "Start a one-time historical backfill for a model.",
				Handler:  d.modelHandler.HandleStartBackfill,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/models/{id}/backfill/cancel",
				Response: api.Response[map[string]bool]{},
				Summary:  "Stop an in-flight model backfill, keeping partial data.",
				Handler:  d.modelHandler.HandleCancelBackfill,
			})

			// Dictionary actions (for alerts)
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/dictionary-actions",
				Response: api.ListResponse[*dictionaries.DictionaryAction]{},
				Summary:  "List dictionary actions in scope.",
				Handler:  d.dictionaryHandler.HandleListDictionaryActions,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/dictionary-actions",
				Request:  dictionaries.DictionaryActionRequest{},
				Response: api.Response[*dictionaries.DictionaryAction]{},
				Summary:  "Create a dictionary action.",
				Handler:  d.dictionaryHandler.HandleCreateDictionaryAction,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/dictionary-actions/{id}",
				Response: api.Response[*dictionaries.DictionaryAction]{},
				Summary:  "Read one dictionary action.",
				Handler:  d.dictionaryHandler.HandleGetDictionaryAction,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/dictionary-actions/{id}",
				Request:  dictionaries.DictionaryActionRequest{},
				Response: api.Response[*dictionaries.DictionaryAction]{},
				Summary:  "Update a dictionary action.",
				Handler:  d.dictionaryHandler.HandleUpdateDictionaryAction,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/dictionary-actions/{id}",
				Response: api.Response[map[string]bool]{},
				Summary:  "Delete a dictionary action.",
				Handler:  d.dictionaryHandler.HandleDeleteDictionaryAction,
			})

			// Saved Queries
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/saved-queries",
				Response: api.ListResponse[savedqueries.SavedQuery]{},
				Summary:  "List saved queries in scope.",
				Handler:  d.savedQueryHandler.HandleList,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/saved-queries",
				Request:  savedqueries.SavedQueryRequest{},
				Response: api.Response[savedqueries.SavedQuery]{},
				Summary:  "Save a query.",
				Handler:  d.savedQueryHandler.HandleCreate,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/saved-queries/{id}",
				Request:  savedqueries.SavedQueryRequest{},
				Response: api.Response[savedqueries.SavedQuery]{},
				Summary:  "Update a saved query.",
				Handler:  d.savedQueryHandler.HandleUpdate,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/saved-queries/{id}",
				Response: api.Response[map[string]bool]{},
				Summary:  "Delete a saved query.",
				Handler:  d.savedQueryHandler.HandleDelete,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/saved-queries/{id}/use",
				Response: api.Response[map[string]bool]{},
				Summary:  "Record that a saved query was run.",
				Handler:  d.savedQueryHandler.HandleMarkUsed,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/saved-queries/{id}/favorite",
				Response: api.Response[map[string]bool]{},
				Summary:  "Pin a saved query for the caller.",
				Handler:  d.savedQueryHandler.HandleFavorite,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/saved-queries/{id}/favorite",
				Response: api.Response[map[string]bool]{},
				Summary:  "Unpin a saved query for the caller.",
				Handler:  d.savedQueryHandler.HandleUnfavorite,
			})

			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/query-history",
				Response: api.ListResponse[queryhistory.QueryHistory]{},
				Summary:  "List the caller's recent queries.",
				Handler:  d.queryHistoryHandler.HandleList,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/query-history",
				Request:  queryhistory.RecordRequest{},
				Response: api.Response[map[string]bool]{},
				Summary:  "Record a query the caller ran.",
				Handler:  d.queryHistoryHandler.HandleRecord,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/query-history",
				Response: api.Response[map[string]bool]{},
				Summary:  "Clear the caller's query history.",
				Handler:  d.queryHistoryHandler.HandleClear,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/query-history/{id}",
				Response: api.Response[map[string]bool]{},
				Summary:  "Delete one query-history entry.",
				Handler:  d.queryHistoryHandler.HandleDelete,
			})

			// Chat. Conversations are private per-user state keyed on a real
			// users row, so they are session-only; instructions are fractal
			// config and stay open to keys.
			r.Group(func(r api.Router) {
				r.Use(auth.DenyAPIKey("chat"))
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/chat/conversations",
					Response: api.ListResponse[*chat.Conversation]{},
					Summary:  "List the caller's chat conversations.",
					Handler:  d.chatHandler.HandleListConversations,
				})
				r.Register(api.Route{
					Method:   http.MethodPost,
					Path:     "/chat/conversations",
					Request:  chat.CreateConversationRequest{},
					Response: api.Response[*chat.Conversation]{},
					Summary:  "Start a chat conversation.",
					Handler:  d.chatHandler.HandleCreateConversation,
				})
				r.Register(api.Route{
					Method:   http.MethodPatch,
					Path:     "/chat/conversations/{id}",
					Request:  chat.RenameConversationRequest{},
					Response: api.Response[*chat.Conversation]{},
					Summary:  "Rename a conversation.",
					Handler:  d.chatHandler.HandleRenameConversation,
				})
				r.Register(api.Route{
					Method:   http.MethodDelete,
					Path:     "/chat/conversations/{id}",
					Response: api.Response[map[string]bool]{},
					Summary:  "Delete a conversation.",
					Handler:  d.chatHandler.HandleDeleteConversation,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/chat/conversations/{id}/messages",
					Response: api.ListResponse[*chat.Message]{},
					Summary:  "Read a conversation's messages.",
					Handler:  d.chatHandler.HandleGetMessages,
				})
				r.Register(api.Route{
					Method:   http.MethodDelete,
					Path:     "/chat/conversations/{id}/messages",
					Response: api.Response[map[string]bool]{},
					Summary:  "Clear a conversation's messages.",
					Handler:  d.chatHandler.HandleClearMessages,
				})
				r.Register(api.Route{
					Method:  http.MethodPost,
					Path:    "/chat/conversations/{id}/stream",
					Request: chat.StreamMessageRequest{},
					Summary: "Send a message and stream the reply.",
					Handler: d.chatHandler.HandleStream,
				})
				r.Register(api.Route{
					Method:   http.MethodPatch,
					Path:     "/chat/conversations/{id}/libraries",
					Request:  chat.SetConversationLibrariesRequest{},
					Response: api.Response[map[string]bool]{},
					Summary:  "Set the instruction libraries attached to a conversation.",
					Handler:  d.chatHandler.HandleSetConversationLibraries,
				})
				r.Register(api.Route{
					Method:   http.MethodGet,
					Path:     "/chat/conversations/{id}/libraries",
					Response: api.ListResponse[*instructions.Library]{},
					Summary:  "List the instruction libraries attached to a conversation.",
					Handler:  d.chatHandler.HandleGetConversationLibraries,
				})
				r.Register(api.Route{
					Method:   http.MethodDelete,
					Path:     "/chat/conversations",
					Response: api.Response[map[string]bool]{},
					Summary:  "Delete all of the caller's conversations.",
					Handler:  d.chatHandler.HandleDeleteAllConversations,
				})
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/chat/instructions",
				Response: api.ListResponse[*chat.Instruction]{},
				Summary:  "List the caller's chat instructions.",
				Handler:  d.chatHandler.HandleListInstructions,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/chat/instructions",
				Request:  chat.InstructionRequest{},
				Response: api.Response[*chat.Instruction]{},
				Summary:  "Create a chat instruction.",
				Handler:  d.chatHandler.HandleCreateInstruction,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/chat/instructions/{instructionId}",
				Request:  chat.InstructionRequest{},
				Response: api.Response[*chat.Instruction]{},
				Summary:  "Update a chat instruction.",
				Handler:  d.chatHandler.HandleUpdateInstruction,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/chat/instructions/{instructionId}",
				Response: api.Response[map[string]bool]{},
				Summary:  "Delete a chat instruction.",
				Handler:  d.chatHandler.HandleDeleteInstruction,
			})

			// Context Links (enabled endpoint for all users, CRUD admin-checked in handler)
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/context-links/enabled",
				Response: api.ListResponse[contextlinks.ContextLink]{},
				Summary:  "List the context links enabled for the current scope.",
				Handler:  d.contextLinkHandler.HandleListEnabled,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/context-links",
				Response: api.ListResponse[contextlinks.ContextLink]{},
				Summary:  "List context links in scope.",
				Handler:  d.contextLinkHandler.HandleList,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/context-links",
				Request:  contextlinks.CreateRequest{},
				Response: api.Response[*contextlinks.ContextLink]{},
				Summary:  "Create a context link.",
				Handler:  d.contextLinkHandler.HandleCreate,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/context-links/{id}",
				Response: api.Response[*contextlinks.ContextLink]{},
				Summary:  "Read one context link.",
				Handler:  d.contextLinkHandler.HandleGet,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/context-links/{id}",
				Request:  contextlinks.UpdateRequest{},
				Response: api.Response[*contextlinks.ContextLink]{},
				Summary:  "Update a context link.",
				Handler:  d.contextLinkHandler.HandleUpdate,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/context-links/{id}",
				Response: api.Response[map[string]string]{},
				Summary:  "Delete a context link.",
				Handler:  d.contextLinkHandler.HandleDelete,
			})

			// Alert Feeds
			// Instruction Libraries
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/instruction-libraries",
				Response: api.Response[[]*instructions.Library]{},
				Summary:  "List instruction libraries in scope.",
				Handler:  d.instructionHandler.HandleListLibraries,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/instruction-libraries/ensure-default",
				Response: api.Response[*instructions.Library]{},
				Summary:  "Return the scope's library, creating it if there is none.",
				Handler:  d.instructionHandler.HandleEnsureDefaultLibrary,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/instruction-libraries",
				Request:  instructions.CreateLibraryRequest{},
				Response: api.Response[*instructions.Library]{},
				Summary:  "Create an instruction library.",
				Handler:  d.instructionHandler.HandleCreateLibrary,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/instruction-libraries/{id}",
				Response: api.Response[map[string]interface{}]{},
				Summary:  "Read one library with its pages.",
				Handler:  d.instructionHandler.HandleGetLibrary,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/instruction-libraries/{id}",
				Request:  instructions.UpdateLibraryRequest{},
				Response: api.Response[*instructions.Library]{},
				Summary:  "Update a library.",
				Handler:  d.instructionHandler.HandleUpdateLibrary,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/instruction-libraries/{id}",
				Summary: "Delete a library.",
				Handler: d.instructionHandler.HandleDeleteLibrary,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/instruction-libraries/{id}/pages",
				Response: api.Response[[]*instructions.Page]{},
				Summary:  "List a library's pages.",
				Handler:  d.instructionHandler.HandleListPages,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/instruction-libraries/{id}/pages",
				Request:  instructions.CreatePageRequest{},
				Response: api.Response[*instructions.Page]{},
				Summary:  "Create a page in a library.",
				Handler:  d.instructionHandler.HandleCreatePage,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/instruction-libraries/{id}/pages/{pageId}",
				Response: api.Response[*instructions.Page]{},
				Summary:  "Read one page.",
				Handler:  d.instructionHandler.HandleGetPage,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/instruction-libraries/{id}/pages/{pageId}/backlinks",
				Response: api.Response[[]instructions.PageRef]{},
				Summary:  "List the pages linking to a page.",
				Handler:  d.instructionHandler.HandleGetBacklinks,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/instruction-libraries/{id}/pages/{pageId}",
				Request:  instructions.UpdatePageRequest{},
				Response: api.Response[*instructions.Page]{},
				Summary:  "Update a page.",
				Handler:  d.instructionHandler.HandleUpdatePage,
			})
			r.Register(api.Route{
				Method:  http.MethodPatch,
				Path:    "/instruction-libraries/{id}/pages/{pageId}/move",
				Request: instructions.MovePageRequest{},
				Summary: "Move a page to another folder or position.",
				Handler: d.instructionHandler.HandleMovePage,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/instruction-libraries/{id}/folders",
				Response: api.Response[[]*instructions.Folder]{},
				Summary:  "List a library's folders.",
				Handler:  d.instructionHandler.HandleListFolders,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/instruction-libraries/{id}/folders",
				Request:  instructions.CreateFolderRequest{},
				Response: api.Response[*instructions.Folder]{},
				Summary:  "Create a folder in a library.",
				Handler:  d.instructionHandler.HandleCreateFolder,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/instruction-libraries/{id}/folders/{folderId}",
				Request:  instructions.UpdateFolderRequest{},
				Response: api.Response[*instructions.Folder]{},
				Summary:  "Rename or reorder a folder.",
				Handler:  d.instructionHandler.HandleUpdateFolder,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/instruction-libraries/{id}/folders/{folderId}",
				Summary: "Delete a folder, moving its pages to the root.",
				Handler: d.instructionHandler.HandleDeleteFolder,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/instruction-libraries/{id}/pages/{pageId}",
				Summary: "Delete a page.",
				Handler: d.instructionHandler.HandleDeletePage,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/instruction-libraries/{id}/sync",
				Response: api.Response[*instructions.SyncResult]{},
				Summary:  "Sync a repo-backed library now.",
				Handler:  d.instructionHandler.HandleSyncLibrary,
			})

			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/feeds",
				Response: api.Response[[]*feeds.Feed]{},
				Summary:  "List detection feeds in scope.",
				Handler:  d.feedHandler.HandleListFeeds,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/feeds",
				Request:  feeds.CreateRequest{},
				Response: api.Response[*feeds.Feed]{},
				Summary:  "Create a detection feed.",
				Handler:  d.feedHandler.HandleCreateFeed,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/feeds/{id}",
				Response: api.Response[*feeds.Feed]{},
				Summary:  "Read one feed.",
				Handler:  d.feedHandler.HandleGetFeed,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/feeds/{id}",
				Request:  feeds.UpdateRequest{},
				Response: api.Response[*feeds.Feed]{},
				Summary:  "Update a feed.",
				Handler:  d.feedHandler.HandleUpdateFeed,
			})
			r.Register(api.Route{
				Method:  http.MethodDelete,
				Path:    "/feeds/{id}",
				Summary: "Delete a feed and the alerts it created.",
				Handler: d.feedHandler.HandleDeleteFeed,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/feeds/{id}/sync",
				Summary: "Sync a feed now.",
				Handler: d.feedHandler.HandleSyncFeed,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/feeds/{id}/alerts",
				Response: api.Response[[]*alerts.Alert]{},
				Summary:  "List the alerts a feed created.",
				Handler:  d.feedHandler.HandleGetFeedAlerts,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/feeds/{id}/alerts/enable-all",
				Summary: "Enable every alert in a feed.",
				Handler: d.feedHandler.HandleEnableAllAlerts,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/feeds/{id}/alerts/disable-all",
				Summary: "Disable every alert in a feed.",
				Handler: d.feedHandler.HandleDisableAllAlerts,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/alerts/feed",
				Response: api.Response[*alerts.FeedAlertPage]{},
				Summary:  "List feed alerts in scope.",
				Handler:  d.feedHandler.HandleListAllFeedAlerts,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/alerts/feed/batch-toggle",
				Request:  alerts.BatchToggleFeedAlertsRequest{},
				Response: api.Response[map[string]int]{},
				Summary:  "Enable or disable a set of feed alerts.",
				Handler:  d.alertHandler.HandleBatchToggleFeedAlerts,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/alerts/{id}/duplicate",
				Response: api.Response[*alerts.Alert]{},
				Summary:  "Copy a feed alert into a standalone editable alert.",
				Handler:  d.alertHandler.HandleDuplicateAlert,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/alerts/{id}/toggle-feed",
				Request: alerts.ToggleFeedAlertRequest{},
				Summary: "Enable or disable one feed alert.",
				Handler: d.alertHandler.HandleToggleFeedAlert,
			})

			// Normalizers (list for all users, CRUD admin-checked in handler)
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/normalizers",
				Response: api.ListResponse[normalizers.Normalizer]{},
				Summary:  "List normalizers.",
				Handler:  d.normalizerHandler.HandleList,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/normalizers",
				Request:  normalizers.CreateRequest{},
				Response: api.Response[*normalizers.Normalizer]{},
				Summary:  "Create a normalizer.",
				Handler:  d.normalizerHandler.HandleCreate,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/normalizers/preview",
				Request:  normalizers.PreviewRequest{},
				Response: api.Response[normalizers.TraceResult]{},
				Summary:  "Run a normalizer against sample input without saving it.",
				Handler:  d.normalizerHandler.HandlePreview,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/normalizers/samples",
				Response: api.ListResponse[normalizers.LogSample]{},
				Summary:  "Return recent raw logs for the normalizer editor.",
				Handler:  d.normalizerHandler.HandleSamples,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/normalizers/import",
				Response: api.Response[*normalizers.Normalizer]{},
				Summary:  "Import a normalizer from YAML.",
				Handler:  d.normalizerHandler.HandleImportYAML,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/normalizers/{id}",
				Response: api.Response[*normalizers.Normalizer]{},
				Summary:  "Read one normalizer.",
				Handler:  d.normalizerHandler.HandleGet,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/normalizers/{id}",
				Request:  normalizers.UpdateRequest{},
				Response: api.Response[*normalizers.Normalizer]{},
				Summary:  "Update a normalizer.",
				Handler:  d.normalizerHandler.HandleUpdate,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/normalizers/{id}",
				Response: api.Response[map[string]string]{},
				Summary:  "Delete a normalizer.",
				Handler:  d.normalizerHandler.HandleDelete,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/normalizers/{id}/set-default",
				Response: api.Response[map[string]string]{},
				Summary:  "Make a normalizer the default for new tokens.",
				Handler:  d.normalizerHandler.HandleSetDefault,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/normalizers/{id}/duplicate",
				Response: api.Response[*normalizers.Normalizer]{},
				Summary:  "Copy a normalizer under a new name.",
				Handler:  d.normalizerHandler.HandleDuplicate,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/normalizers/{id}/export",
				Summary: "Export a normalizer as YAML.",
				Handler: d.normalizerHandler.HandleExportYAML,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/normalizers/{id}/tokens",
				Response: api.ListResponse[normalizers.TokenUsageInfo]{},
				Summary:  "List the ingest tokens using a normalizer.",
				Handler:  d.normalizerHandler.HandleTokenUsage,
			})

			// Schema fields (admin-only, checked in handler)
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/admin/schema-fields",
				Response: api.Response[schemafields.FieldCatalog]{},
				Summary:  "List the configured schema fields.",
				Handler:  d.schemaFieldsHandler.HandleList,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/admin/schema-fields",
				Request:  schemafields.CreateRequest{},
				Response: api.Response[*schemafields.SchemaField]{},
				Summary:  "Add a schema field.",
				Handler:  d.schemaFieldsHandler.HandleCreate,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/admin/schema-fields/{name}",
				Response: api.Response[map[string]string]{},
				Summary:  "Remove a schema field.",
				Handler:  d.schemaFieldsHandler.HandleDelete,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/admin/schema-fields/reset",
				Request:  schemafields.ResetRequest{},
				Response: api.Response[map[string]string]{},
				Summary:  "Rebuild the ClickHouse schema, dropping all log data.",
				Handler:  d.schemaFieldsHandler.HandleReset,
			})
			r.Register(api.Route{
				Method:  http.MethodGet,
				Path:    "/admin/schema-fields/export",
				Summary: "Export the custom schema fields as YAML.",
				Handler: d.schemaFieldsHandler.HandleExportYAML,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/admin/schema-fields/import",
				Response: api.Response[string]{},
				Summary:  "Import schema fields from YAML.",
				Handler:  d.schemaFieldsHandler.HandleImportYAML,
			})
			// Field distribution, storage, capacity, and ranked suggestions. One
			// request renders the whole schema tab, entirely from Postgres.
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/admin/schema-fields/insights",
				Response: api.Response[*schemafields.Insights]{},
				Summary:  "Return field distribution, storage, capacity, and suggestions.",
				Handler:  d.schemaFieldsHandler.HandleInsights,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/admin/schema-fields/refresh",
				Response: api.Response[map[string]string]{},
				Summary:  "Ask the background sweep to re-measure the schema now.",
				Handler:  d.schemaFieldsHandler.HandleRefresh,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/admin/schema-fields/ignore/{name}",
				Response: api.Response[map[string]string]{},
				Summary:  "Dismiss a suggested schema field.",
				Handler:  d.schemaFieldsHandler.HandleIgnore,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/admin/schema-fields/ignore/{name}",
				Response: api.Response[map[string]string]{},
				Summary:  "Restore a dismissed schema field to the suggestions.",
				Handler:  d.schemaFieldsHandler.HandleUnignore,
			})

			// Admin-only routes (checked in handler)
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/auth/register",
				Request:  auth.RegisterRequest{},
				Response: auth.Response{},
				Summary:  "Create a user and issue an invite token.",
				Handler:  d.authHandler.HandleRegister,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/auth/invite/reset",
				Request:  auth.ResetInviteRequest{},
				Response: auth.Response{},
				Summary:  "Reissue the invite token for a pending user.",
				Handler:  d.authHandler.HandleResetInvite,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/auth/admin-reset-password",
				Request:  auth.AdminResetPasswordRequest{},
				Response: auth.Response{},
				Summary:  "Reset another user's password.",
				Handler:  d.authHandler.HandleAdminResetPassword,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/users",
				Response: api.ListResponse[map[string]interface{}]{},
				Summary:  "List users.",
				Handler:  d.authHandler.HandleListUsers,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/users/{username}",
				Request:  auth.UpdateUserRequest{},
				Response: auth.Response{},
				Summary:  "Update a user's display name or role.",
				Handler:  d.authHandler.HandleUpdateUser,
			})
			r.Register(api.Route{
				Method:   http.MethodPut,
				Path:     "/users/{username}/enabled",
				Request:  auth.SetUserEnabledRequest{},
				Response: auth.Response{},
				Summary:  "Enable or disable a user account.",
				Handler:  d.authHandler.HandleSetUserEnabled,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/users",
				Response: auth.Response{},
				Summary:  "Delete a user.",
				Handler:  d.authHandler.HandleDeleteUser,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/users/mtls-status",
				Response: auth.Response{},
				Summary:  "Report whether mTLS client certificate generation is available.",
				Handler:  d.authHandler.HandleMTLSStatus,
			})
			r.Register(api.Route{
				Method:  http.MethodPost,
				Path:    "/users/{username}/client-cert",
				Request: auth.GenerateClientCertRequest{},
				Summary: "Generate a PKCS#12 client certificate for a user.",
				Handler: d.authHandler.HandleGenerateClientCert,
			})
			r.Register(api.Route{
				Method:   http.MethodDelete,
				Path:     "/logs",
				Response: map[string]interface{}{},
				Summary:  "Delete all log data in the fractal.",
				Handler:  d.statusHandler.HandleClearLogs,
			})

			// Performance monitoring (admin-only, checked in handler)
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/admin/processes",
				Response: map[string]interface{}{},
				Summary:  "List the queries ClickHouse is currently running.",
				Handler:  d.performanceHandler.HandleProcesses,
			})
			r.Register(api.Route{
				Method:   http.MethodPost,
				Path:     "/admin/kill-query",
				Response: map[string]interface{}{},
				Summary:  "Kill a running ClickHouse query.",
				Handler:  d.performanceHandler.HandleKillQuery,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/admin/metrics",
				Response: map[string]interface{}{},
				Summary:  "Return ClickHouse server metrics.",
				Handler:  d.performanceHandler.HandleMetrics,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/admin/ingest-daily",
				Response: map[string]interface{}{},
				Summary:  "Return per-day ingest volume.",
				Handler:  d.performanceHandler.HandleIngestDaily,
			})
			r.Register(api.Route{
				Method:   http.MethodGet,
				Path:     "/admin/alert-stats",
				Response: map[string]interface{}{},
				Summary:  "Return alert engine evaluation statistics.",
				Handler:  d.performanceHandler.HandleAlertStats,
			})
		})
	})

	// Elasticsearch-compatible bulk API (token-authenticated, no session required)
	r.Group(func(r api.Router) {
		r.Use(ingest.RateLimitMiddleware(d.rateLimiter))
		r.Register(api.Route{
			Method:   http.MethodPost,
			Path:     "/_bulk",
			Response: ingest.ElasticBulkResponse{},
			Summary:  "Ingest logs through the Elasticsearch-compatible bulk API.",
			Handler:  d.elasticHandler.HandleBulk,
		})
		r.Register(api.Route{
			Method:   http.MethodPut,
			Path:     "/_bulk",
			Response: ingest.ElasticBulkResponse{},
			Summary:  "Ingest logs through the Elasticsearch-compatible bulk API.",
			Handler:  d.elasticHandler.HandleBulk,
		})
	})

	// OpenTelemetry (OTLP/HTTP) log ingestion (token-authenticated, no session required)
	r.Group(func(r api.Router) {
		r.Use(ingest.RateLimitMiddleware(d.rateLimiter))
		r.Register(api.Route{
			Method:   http.MethodPost,
			Path:     "/v1/logs",
			Response: api.Response[*http.Request]{},
			Summary:  "Ingest logs as an OTLP/HTTP ExportLogsServiceRequest.",
			Handler:  d.otlpHandler.HandleLogs,
		})
	})
	// Deep links: the documented, hand-constructible entry point external tools
	// use to drop an analyst into a specific query. Session-authenticated and
	// resolved server-side, then redirected into the SPA.
	r.Register(api.Route{
		Method:  http.MethodGet,
		Path:    "/go/search",
		Summary: "Resolve a deep link and redirect into the app.",
		Handler: d.deepLinkHandler.HandleSearch,
	})

	r.Register(api.Route{
		Method:  http.MethodGet,
		Path:    "/*",
		Summary: "Serve the web UI.",
		Handler: staticFileHandler(),
	})

	return mux, reg
}
