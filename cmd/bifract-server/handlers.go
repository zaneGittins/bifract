package main

import (
	"bifract/pkg/api"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/archive"
	"bifract/pkg/parser"
	"bifract/pkg/query"
	"bifract/pkg/rbac"
	"bifract/pkg/settings"
	"bifract/pkg/spool"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// HTTP handlers that have no home package yet. They were closures inside
// buildRouter; extracting them keeps the route table readable and is a step
// toward moving each one into the package that owns its data.

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

// staticFileHandler serves the web UI. noDirFS suppresses directory index
// listings, so a request for a directory 404s instead of enumerating ./web.
func staticFileHandler() http.HandlerFunc {
	fileServer := http.FileServer(noDirFS{http.Dir("./web")})

	return func(w http.ResponseWriter, r *http.Request) {
		// Revalidate on every load. Without this the browser heuristically
		// reuses cached assets without asking, which after a deploy leaves a
		// stale mix (e.g. new dashboards.js calling into an old utils.js). The
		// file server still answers If-Modified-Since with 304, so an unchanged
		// asset costs only a small conditional round-trip, not a re-download.
		w.Header().Set("Cache-Control", "no-cache")

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
	}
}

// recallAnalystOK resolves the caller and requires analyst access to the
// fractal. Recall reads cold storage, which is a normal-user read scoped by
// fractal RBAC, unlike the admin-only restore endpoints.
func (d routerDeps) recallAnalystOK(w http.ResponseWriter, r *http.Request, fractalID string) (*storage.User, bool) {
	u, ok := r.Context().Value("user").(*storage.User)
	if !ok || u == nil {
		api.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return nil, false
	}
	role, err := d.authHandler.RBACResolver().ResolveFractalRole(r.Context(), u.Username, fractalID)
	if err != nil || !rbac.HasAccess(u, role, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Analyst access required")
		return nil, false
	}
	return u, true
}

// handleHealth is the liveness probe both server modes expose; the image
// HEALTHCHECK GETs it for either tier.
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy"}`))
}

func (d routerDeps) handleOIDCDisabled(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"enabled": false})
}

func (d routerDeps) handleVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"version": Version})
}

func (d routerDeps) handleTopology(w http.ResponseWriter, r *http.Request) {
	topo := d.db.Topology()
	resp := map[string]interface{}{
		"deployment":         string(topo.Kind),
		"distributed_tables": topo.DistributedTables,
		"shard_routing":      topo.ShardRouting,
		"managed_storage":    topo.ManagedStorage,
		"fanout_cluster":     topo.FanoutCluster,
	}
	if v, _, err := d.db.CheckVersionFloor(r.Context()); err == nil {
		resp["version"] = v.String()
		resp["min_version"] = storage.MinClickHouseVersion.String()
	}
	// Reasons carry the server's own messages, which name internal
	// identifiers, so non-admins get states without them.
	user, _ := r.Context().Value("user").(*storage.User)
	admin := user != nil && user.IsAdmin
	caps := map[string]interface{}{}
	for k, c := range d.db.Capabilities() {
		entry := map[string]interface{}{"state": c.State.String()}
		if admin && c.Reason != "" {
			entry["reason"] = c.Reason
		}
		caps[string(k)] = entry
	}
	resp["capabilities"] = caps
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (d routerDeps) handlePressure(w http.ResponseWriter, r *http.Request) {
	alertsDeferred := d.ingestQueue.Depth() > d.alertDeferThreshold
	resp := map[string]interface{}{
		"alerts_deferred": alertsDeferred,
	}
	if d.dbIngest.Topology().DistributedTables {
		s := d.distMonitor.Stats()
		resp["distribution_queue"] = map[string]interface{}{
			"healthy":           s.Healthy,
			"data_files":        s.DataFiles,
			"broken_data_files": s.BrokenDataFiles,
			"error_count":       s.ErrorCount,
		}
		since, bucket := query.MetricRange(r.URL.Query().Get("range"))
		resp["distribution_queue_history"] = d.distMonitor.History(r.Context(), since, bucket)
		resp["ddl_queue_history"] = d.ddlMonitor.History(r.Context(), since, bucket)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (d routerDeps) handleArchiveStatus(w http.ResponseWriter, r *http.Request) {
	enabled := false
	if v, _ := d.pg.GetSetting(r.Context(), "archive_enabled"); v == "true" {
		enabled = true
	}
	var updatedAt, lastCommit sql.NullTime
	var fractalCount int
	var totalBytes, totalRecords int64
	_ = d.pg.QueryRow(r.Context(),
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
	var maintainRetentionTables, maintainRetentionFiles, maintainOrphansDeleted int
	_ = d.pg.QueryRow(r.Context(),
		`SELECT last_run_at, last_attempt_at, last_outcome, last_error, duration_ms, tables_seen, compacted,
					        groups_failed, expired, candidate_bytes, compacted_bytes, run_requested_at, run_requested_by,
					        retention_tables, retention_files, orphans_deleted
					 FROM archive_maintain_status WHERE id = 1`).
		Scan(&maintainLastRun, &maintainLastAttempt, &maintainOutcome, &maintainError, &maintainDurationMs,
			&maintainTables, &maintainCompacted, &maintainGroupsFailed, &maintainExpired,
			&maintainCandidateBytes, &maintainCompactedBytes, &maintainRunRequestedAt, &maintainRunRequestedBy,
			&maintainRetentionTables, &maintainRetentionFiles, &maintainOrphansDeleted)
	maintainResp := map[string]interface{}{
		"outcome":         maintainOutcome,
		"duration_ms":     maintainDurationMs,
		"tables_seen":     maintainTables,
		"compacted":       maintainCompacted,
		"groups_failed":   maintainGroupsFailed,
		"expired":         maintainExpired,
		"candidate_bytes": maintainCandidateBytes,
		"compacted_bytes": maintainCompactedBytes,
		// Lifecycle: expired archive partitions dropped and unreferenced files
		// swept, so the panel shows storage being returned, not just compaction.
		"retention_tables": maintainRetentionTables,
		"retention_files":  maintainRetentionFiles,
		"orphans_deleted":  maintainOrphansDeleted,
		// on_schedule mirrors archiver_alive's freshness check above, sized to
		// the maintainer's scheduled cadence instead of the archiver's ~30s
		// heartbeat interval. false with no prior attempt at all (maintainOutcome
		// == "never") reads correctly as "not yet run" rather than "overdue".
		"on_schedule": maintainLastAttempt.Valid && time.Since(maintainLastAttempt.Time) < maintainStaleAfter,
		// A pending or in-progress "Run now": run_requested is the queued state
		// (claimed but not yet started, or waiting for the next poll); outcome
		// == "running" is the live pass. Lets the UI show/disable the button.
		"run_requested": maintainRunRequestedAt.Valid,
		// The maintainer normally converts 'running' to a terminal outcome when
		// it restarts, but it cannot if it is gone entirely (container removed,
		// deployment scaled to 0). Age it out here too so a marker nothing will
		// ever clear stops reading as a live pass and stops disabling "Run now".
		"stale_running": maintainOutcome == string(archive.MaintainOutcomeRunning) &&
			maintainLastAttempt.Valid && time.Since(maintainLastAttempt.Time) >= maintainStaleAfter,
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
	if rows, herr := d.pg.Query(r.Context(),
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
	provisioned := d.ingestQueue.SpoolProvisioned()
	usedBytes, maxBytes := d.ingestQueue.SpoolUsageBytes(), d.ingestQueue.SpoolMaxBytes()
	pressure := d.ingestQueue.SpoolPressure()
	if !provisioned {
		st := d.pg.ReadSpoolStatus(r.Context())
		provisioned, usedBytes, maxBytes, pressure = st.Provisioned, st.UsedBytes, st.MaxBytes, st.Pressure
	}
	resp := map[string]interface{}{
		"enabled":     enabled,
		"provisioned": provisioned,
		"backend":     getEnv("BIFRACT_ARCHIVE_BACKEND", "disk"),
		// Empty unless ClickHouse cannot read the archive, in which case
		// archiving still works but restore and recall do not. An admin
		// is the only one who can act on it, so it surfaces here.
		"read_blocked": archiveCHReadBlocked,
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
}

// enabledRequest toggles a boolean instance setting.
type enabledRequest struct {
	Enabled bool `json:"enabled"`
}

func (d routerDeps) handleSetArchiveEnabled(w http.ResponseWriter, r *http.Request) {
	// Guardrail: archiving can only be enabled when the spool machinery
	// is provisioned (dormant-but-present after --upgrade). In a split
	// deployment the spool lives in the ingest tier, so also accept its
	// published provisioned state.
	if !d.ingestQueue.SpoolProvisioned() && !d.pg.ReadSpoolStatus(r.Context()).Provisioned {
		api.WriteError(w, http.StatusBadRequest, "Archive not provisioned. Run bifract --upgrade to add the archiver, then retry.")
		return
	}
	var body enabledRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	val := "false"
	if body.Enabled {
		val = "true"
	}
	if err := d.pg.SetSetting(r.Context(), "archive_enabled", val); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to save")
		return
	}
	// Reflect immediately in the running tee (the poller also refreshes).
	d.ingestQueue.SetArchiveEnabled(body.Enabled)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "enabled": body.Enabled})
}

func (d routerDeps) handleRunArchiveMaintain(w http.ResponseWriter, r *http.Request) {
	u, _ := r.Context().Value("user").(*storage.User)
	// Same provisioning guard as enabling: until the archive machinery is
	// provisioned (--upgrade) there is no maintainer to service the request.
	if !d.ingestQueue.SpoolProvisioned() && !d.pg.ReadSpoolStatus(r.Context()).Provisioned {
		api.WriteError(w, http.StatusBadRequest, "Archive not provisioned. Run bifract --upgrade to add the archiver, then retry.")
		return
	}
	// A run-now while archiving is disabled would be claimed and then
	// skipped (skipped_disabled), a confusing no-op; reject up front so the
	// button's outcome is predictable.
	if v, _ := d.pg.GetSetting(r.Context(), archiveEnabledSetting); v != "true" {
		api.WriteError(w, http.StatusBadRequest, "Enable archiving before running maintenance.")
		return
	}
	if err := archive.RequestMaintainRun(r.Context(), d.pg.DB(), u.Username); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to request maintenance run")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func (d routerDeps) handleClearArchiveCatalog(w http.ResponseWriter, r *http.Request) {
	// Require archiving disabled so the archiver is not concurrently
	// recreating tables/namespaces mid-clear (races a clean reset to zero).
	if v, _ := d.pg.GetSetting(r.Context(), archiveEnabledSetting); v == "true" {
		api.WriteError(w, http.StatusConflict, "Disable archiving before clearing the catalog.")
		return
	}
	// archiveNamespace matches archive.Namespace (the single namespace every
	// fractal table lives under). All three writes run in one transaction so
	// a mid-clear failure never leaves orphaned namespace_properties rows or a
	// stale footprint; the whole reset either lands or rolls back.
	const archiveNamespace = "bifract"
	tx, err := d.pg.Begin(r.Context())
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to clear catalog")
		return
	}
	defer tx.Rollback(r.Context())
	if _, err := tx.Exec(r.Context(), "DELETE FROM iceberg_tables WHERE table_namespace = $1", archiveNamespace); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to clear catalog tables")
		return
	}
	if _, err := tx.Exec(r.Context(), "DELETE FROM iceberg_namespace_properties WHERE namespace = $1", archiveNamespace); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to clear catalog namespaces")
		return
	}
	// Zero the footprint the admin UI shows; the archiver heartbeat keeps it
	// at zero until new data is archived.
	if _, err := tx.Exec(r.Context(), "UPDATE archive_status SET fractal_count = 0, total_bytes = 0, total_records = 0, updated_at = NOW() WHERE id = 1"); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to reset archive status")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to commit catalog clear")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func (d routerDeps) handleClearArchiveSpool(w http.ResponseWriter, r *http.Request) {
	u, _ := r.Context().Value("user").(*storage.User)
	// Require archiving disabled: the reset runs on the ingest (Writer)
	// side only while the tee is not spooling, so a clear while enabled
	// would race live writes.
	if v, _ := d.pg.GetSetting(r.Context(), archiveEnabledSetting); v == "true" {
		api.WriteError(w, http.StatusConflict, "Disable archiving before clearing the spool.")
		return
	}
	gen := spoolClearGeneration(d.pg) + 1
	if err := d.pg.SetSetting(r.Context(), spool.ClearGenerationSettingKey, strconv.FormatInt(gen, 10)); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to request spool clear")
		return
	}
	log.Printf("[Archive] spool clear requested (generation %d) by %s", gen, u.Username)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true, "generation": gen,
		"message": "Each ingest pod will clear its spool within ~10s.",
	})
}

func (d routerDeps) handleDistributionQueueShards(w http.ResponseWriter, r *http.Request) {
	stats, err := d.dbIngest.DistributionQueueByShard(r.Context())
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to query distribution queue")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// resetDistributionQueueRequest names the shard whose queue to reset.
type resetDistributionQueueRequest struct {
	ShardNum uint64 `json:"shard_num"`
}

func (d routerDeps) handleResetDistributionQueue(w http.ResponseWriter, r *http.Request) {
	u, _ := r.Context().Value("user").(*storage.User)
	var body resetDistributionQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if err := d.dbIngest.ResetDistributedQueue(r.Context(), body.ShardNum); err != nil {
		log.Printf("[Admin] distribution queue reset failed (shard %d, requested by %s): %v", body.ShardNum, u.Username, err)
		// The error itself carries actionable guidance (e.g. "retry
		// immediately" when the table was dropped but not yet recreated),
		// which matters more here than a generic message.
		api.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("[Admin] distribution queue reset on shard %d by %s", body.ShardNum, u.Username)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "shard_num": body.ShardNum})
}

func (d routerDeps) handleGetEndpointAnalysis(w http.ResponseWriter, r *http.Request) {
	enabled := false
	if v, _ := d.pg.GetSetting(r.Context(), storage.AdvancedEndpointAnalysisSetting); v == "true" {
		enabled = true
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"enabled": enabled})
}

func (d routerDeps) handleSetEndpointAnalysis(w http.ResponseWriter, r *http.Request) {
	var body enabledRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	// Attach/detach the MVs first; only persist the setting if that succeeds,
	// so the stored flag always matches the actual ClickHouse state.
	if err := d.db.ReconcileEndpointAnalysisMVs(r.Context(), body.Enabled); err != nil {
		log.Printf("endpoint-analysis reconcile failed: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to apply change to ClickHouse")
		return
	}
	val := "false"
	if body.Enabled {
		val = "true"
	}
	if err := d.pg.SetSetting(r.Context(), storage.AdvancedEndpointAnalysisSetting, val); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to save")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "enabled": body.Enabled})
}

func (d routerDeps) handleGetSharedLinksEnabled(w http.ResponseWriter, r *http.Request) {
	enabled := false
	if v, _ := d.pg.GetSetting(r.Context(), storage.SharedLinksEnabledSetting); v == "true" {
		enabled = true
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"enabled": enabled})
}

func (d routerDeps) handleSetSharedLinksEnabled(w http.ResponseWriter, r *http.Request) {
	var body enabledRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	val := "false"
	if body.Enabled {
		val = "true"
	}
	if err := d.pg.SetSetting(r.Context(), storage.SharedLinksEnabledSetting, val); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to save")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "enabled": body.Enabled})
}

// createRestoreRequest describes an archive window to replay and where it lands.
type createRestoreRequest struct {
	FractalIDs []string `json:"fractal_ids"`
	From       string   `json:"from"`
	To         string   `json:"to"`
	Mode       string   `json:"mode"`
	// TargetMode selects where restored rows land: "existing" (the
	// default) restores each source fractal into itself; "new"
	// creates a dedicated no-retention fractal and restores a single
	// source into it.
	TargetMode string `json:"target_mode"`
	// NewFractalName names the fractal created when TargetMode="new".
	NewFractalName string `json:"new_fractal_name"`
	// AcknowledgeRetention confirms the operator has been shown that
	// the restored window falls outside the fractal's retention and
	// will be deleted again. See retentionConflicts.
	AcknowledgeRetention bool `json:"acknowledge_retention"`
}

func (d routerDeps) handleCreateRestore(w http.ResponseWriter, r *http.Request) {
	u, _ := r.Context().Value("user").(*storage.User)
	var body createRestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if len(body.FractalIDs) == 0 {
		api.WriteError(w, http.StatusBadRequest, "Select at least one fractal")
		return
	}
	if len(body.FractalIDs) > 200 {
		api.WriteError(w, http.StatusBadRequest, "Too many fractals in one request")
		return
	}
	from, err := parseArchiveTime(body.From)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid 'from' time")
		return
	}
	to, err := parseArchiveTime(body.To)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid 'to' time")
		return
	}
	if !to.After(from) {
		api.WriteError(w, http.StatusBadRequest, "'to' must be after 'from'")
		return
	}
	mode := body.Mode
	if mode == "" {
		mode = "restore"
	}
	if mode != "restore" && mode != "reconcile" {
		api.WriteError(w, http.StatusBadRequest, "mode must be 'restore' or 'reconcile'")
		return
	}

	// Restore into a new dedicated fractal: create a no-retention
	// workspace and route a single source fractal's archive into it. The
	// new fractal has no retention, so the retention-conflict check does
	// not apply. Restore-mode only (reconcile is inherently same-fractal),
	// single-source only (the provenance columns hold one source).
	if body.TargetMode == "new" {
		if mode != "restore" {
			api.WriteError(w, http.StatusBadRequest, "Restoring into a new fractal is only supported in restore mode")
			return
		}
		if len(body.FractalIDs) != 1 || body.FractalIDs[0] == "" {
			api.WriteError(w, http.StatusBadRequest, "Restoring into a new fractal requires exactly one source fractal")
			return
		}
		if strings.TrimSpace(body.NewFractalName) == "" {
			api.WriteError(w, http.StatusBadRequest, "A name for the new fractal is required")
			return
		}
		sourceID := body.FractalIDs[0]
		newFractal, err := d.fractalManager.CreateFractalForRestore(
			r.Context(), strings.TrimSpace(body.NewFractalName),
			fmt.Sprintf("Restored from fractal %s", sourceID), u.Username, sourceID, from, to)
		if err != nil {
			// Name collision and validation surface as a 400; the manager
			// wraps both, so a bad name does not read as a server error.
			api.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		batchID := uuid.NewString()
		var id int64
		if err := d.pg.QueryRow(r.Context(),
			`INSERT INTO archive_restore_jobs (batch_id, fractal_id, target_fractal_id, mode, from_ts, to_ts, requested_by)
						 VALUES ($1, $2, $3, 'restore', $4, $5, $6) RETURNING id`,
			batchID, sourceID, newFractal.ID, from, to, u.Username).Scan(&id); err != nil {
			api.WriteError(w, http.StatusInternalServerError, "Fractal created but failed to enqueue restore job")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true, "batch_id": batchID, "job_ids": []int64{id},
			"target_fractal_id": newFractal.ID, "target_fractal_name": newFractal.Name,
		})
		return
	}

	// Restoring past a fractal's retention horizon puts rows back that
	// the hourly retention pass deletes again, typically within the
	// hour. Block it unless the operator has been told and said yes;
	// silently doing the work and throwing it away is the worst outcome.
	if !body.AcknowledgeRetention {
		conflicts, err := retentionConflicts(r.Context(), d.pg, body.FractalIDs, from)
		if err != nil {
			api.WriteError(w, http.StatusInternalServerError, "Failed to check fractal retention")
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
		err := d.pg.QueryRow(r.Context(),
			`INSERT INTO archive_restore_jobs (batch_id, fractal_id, mode, from_ts, to_ts, requested_by)
						 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
			batchID, fid, mode, from, to, u.Username).Scan(&id)
		if err != nil {
			api.WriteError(w, http.StatusInternalServerError, "Failed to enqueue restore job")
			return
		}
		ids = append(ids, id)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true, "batch_id": batchID, "job_ids": ids,
	})
}

func (d routerDeps) handleListRestores(w http.ResponseWriter, r *http.Request) {
	// Pagination + optional status filter.
	q := r.URL.Query()
	limit, offset := api.PageParams(r, 20, 100)
	status := q.Get("status")
	validStatus := map[string]bool{"pending": true, "running": true, "succeeded": true, "failed": true, "canceled": true}
	where := ""
	var args []interface{}
	if validStatus[status] {
		where = "WHERE status = $1"
		args = append(args, status)
	}
	var total int
	if err := d.pg.QueryRow(r.Context(), "SELECT count(*) FROM archive_restore_jobs "+where, args...).Scan(&total); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to load jobs")
		return
	}
	// LEFT JOIN the destination fractal so the UI can show "-> name"
	// for restore-into-fractal jobs. Columns are qualified because the
	// join collides on id/created_at.
	rows, err := d.pg.Query(r.Context(),
		`SELECT j.id, j.batch_id, j.fractal_id, j.target_fractal_id, COALESCE(tf.name, ''),
					        j.mode, j.from_ts, j.to_ts, j.status,
					        j.target_rows, j.rows_restored, j.chunks_total, j.chunks_done, j.cursor_ts,
					        COALESCE(j.error, ''), COALESCE(j.requested_by, ''),
					        j.created_at, j.started_at, j.finished_at
					 FROM archive_restore_jobs j
					 LEFT JOIN fractals tf ON tf.id::text = j.target_fractal_id `+where+
			fmt.Sprintf(" ORDER BY j.created_at DESC LIMIT %d OFFSET %d", limit, offset), args...)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to load jobs")
		return
	}
	defer rows.Close()
	jobs := make([]map[string]interface{}, 0, 32)
	for rows.Next() {
		var (
			id                         int64
			batchID, fid, mode, status string
			errMsg, reqBy              string
			targetID, targetName       sql.NullString
			from, to, created          time.Time
			started, finished, cursor  sql.NullTime
			target, restored           int64
			chunksTotal, chunksDone    int
		)
		if err := rows.Scan(&id, &batchID, &fid, &targetID, &targetName, &mode, &from, &to, &status,
			&target, &restored, &chunksTotal, &chunksDone, &cursor,
			&errMsg, &reqBy, &created, &started, &finished); err != nil {
			continue
		}
		j := map[string]interface{}{
			"id": id, "batch_id": batchID, "fractal_id": fid, "mode": mode,
			"from": from.UTC(), "to": to.UTC(), "status": status,
			"target_rows": target, "rows_restored": restored,
			"chunks_total": chunksTotal, "chunks_done": chunksDone,
			"requested_by": reqBy, "created_at": created.UTC(),
		}
		if targetID.Valid && targetID.String != "" {
			j["target_fractal_id"] = targetID.String
			j["target_fractal_name"] = targetName.String
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
	api.WritePage(w, jobs, api.Page{Total: total, Limit: limit, Offset: offset})
}

func (d routerDeps) handleCancelRestore(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	res, err := d.pg.Exec(r.Context(),
		`UPDATE archive_restore_jobs SET status = 'canceled', finished_at = NOW(), updated_at = NOW()
					 WHERE id = $1 AND status IN ('pending', 'running')`, id)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to cancel")
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		api.WriteError(w, http.StatusConflict, "Job has already finished")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func (d routerDeps) handleResumeRestore(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	res, err := d.pg.Exec(r.Context(),
		`UPDATE archive_restore_jobs
					 SET status = 'pending', error = NULL, finished_at = NULL, started_at = NULL, updated_at = NOW()
					 WHERE id = $1 AND status IN ('failed', 'canceled')`, id)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to resume")
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		api.WriteError(w, http.StatusConflict, "Only a failed or canceled job can be resumed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func (d routerDeps) handleRecallAvailable(w http.ResponseWriter, r *http.Request) {
	if u, ok := r.Context().Value("user").(*storage.User); !ok || u == nil {
		api.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	enabled := false
	if v, _ := d.pg.GetSetting(r.Context(), "archive_enabled"); v == "true" {
		enabled = true
	}
	// Provisioned in-process (full server) or in the split ingest tier.
	provisioned := d.ingestQueue.SpoolProvisioned() || d.pg.ReadSpoolStatus(r.Context()).Provisioned
	resp := map[string]interface{}{
		"available": enabled && provisioned && archiveCHReadBlocked == "",
	}
	// Say why rather than presenting recall as simply absent.
	if archiveCHReadBlocked != "" {
		resp["reason"] = archiveCHReadBlocked
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (d routerDeps) handleRecallEstimate(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "fractalID")
	if _, ok := d.recallAnalystOK(w, r, fractalID); !ok {
		return
	}
	if d.recallEstimator == nil {
		api.WriteError(w, http.StatusServiceUnavailable, "Archive estimate unavailable")
		return
	}
	from, err := parseArchiveTime(r.URL.Query().Get("from"))
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid 'from' time")
		return
	}
	to, err := parseArchiveTime(r.URL.Query().Get("to"))
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid 'to' time")
		return
	}
	if !to.After(from) {
		api.WriteError(w, http.StatusBadRequest, "'to' must be after 'from'")
		return
	}
	ectx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	est, err := d.recallEstimator.Estimate(ectx, fractalID, from, to)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to estimate scan")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"files":      est.Files,
		"rows":       est.Rows,
		"bytes":      est.Bytes,
		"partitions": est.Partitions,
		"archived":   est.Archived,
	})
}

// createRecallRequest describes an archive search to submit.
type createRecallRequest struct {
	Query   string `json:"query"`
	From    string `json:"from"`
	To      string `json:"to"`
	MaxRows int    `json:"max_rows"`
	Fresh   bool   `json:"fresh"` // bypass result reuse, force a new scan
}

func (d routerDeps) handleCreateRecall(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "fractalID")
	u, ok := d.recallAnalystOK(w, r, fractalID)
	if !ok {
		return
	}
	var body createRecallRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if strings.TrimSpace(body.Query) == "" {
		api.WriteError(w, http.StatusBadRequest, "Query is required")
		return
	}
	from, err := parseArchiveTime(body.From)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid 'from' time")
		return
	}
	to, err := parseArchiveTime(body.To)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid 'to' time")
		return
	}
	if !to.After(from) {
		api.WriteError(w, http.StatusBadRequest, "'to' must be after 'from'")
		return
	}
	if err := validateRecallQuery(body.Query); err != nil {
		api.WriteError(w, http.StatusBadRequest, err.Error())
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
	// Result reuse: an identical search (same fractal, query, window, and
	// at least the requested row cap) that already succeeded and whose
	// results are still cached is returned as-is instead of re-scanning
	// object storage. Reuse is limited to answers that are still current:
	// either it finished seconds ago (the re-run / concurrent-user case,
	// where the archive is unchanged), or its window ends before the
	// stability horizon so every ingest-day partition it covers is sealed.
	// A live window (ending now) only reuses within the short TTL, so a
	// search over "up to this minute" never serves a stale answer.
	if !body.Fresh {
		var reuseID int64
		if err := d.pg.QueryRow(r.Context(),
			`SELECT id FROM archive_search_jobs
						 WHERE fractal_id = $1 AND query = $2 AND from_ts = $3 AND to_ts = $4
						   AND max_rows >= $5 AND status = 'succeeded' AND results IS NOT NULL
						   AND (finished_at > NOW() - INTERVAL '2 minutes'
						        OR (to_ts < NOW() - INTERVAL '2 hours' AND finished_at > NOW() - INTERVAL '1 hour'))
						 ORDER BY finished_at DESC LIMIT 1`,
			fractalID, body.Query, from, to, maxRows).Scan(&reuseID); err == nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "id": reuseID, "reused": true})
			return
		}
	}
	// Pre-flight scan ceiling: reject a window whose archived size exceeds
	// the admin's recall scan limit before it launches a NEW scan. Placed
	// after reuse so a still-cached identical result is served without
	// gating (a cache hit is not a scan) and without paying the estimate's
	// latency. ClickHouse does not enforce max_bytes_to_read on iceberg
	// table functions, so this admission gate is the real guard; it reuses
	// the same manifest-only estimate the UI shows. 0 = unlimited.
	if limit := settings.Get().RecallMaxBytesRead; limit > 0 && d.recallEstimator != nil {
		ectx, ecancel := context.WithTimeout(r.Context(), 20*time.Second)
		est, eerr := d.recallEstimator.Estimate(ectx, fractalID, from, to)
		ecancel()
		if eerr == nil && est.Archived && est.Bytes > limit {
			api.WriteError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("This window holds about %s of archived data, above the %s recall scan limit. Narrow the time range or raise the limit in Settings.",
				recallBytesHuman(est.Bytes), recallBytesHuman(limit)))
			return
		}
	}
	var inflight int
	if err := d.pg.QueryRow(r.Context(),
		`SELECT count(*) FROM archive_search_jobs WHERE requested_by = $1 AND status IN ('pending','running')`,
		u.Username).Scan(&inflight); err == nil && inflight >= 3 {
		api.WriteError(w, http.StatusTooManyRequests, "Too many searches in progress; wait for one to finish")
		return
	}
	var id int64
	if err := d.pg.QueryRow(r.Context(),
		`INSERT INTO archive_search_jobs (fractal_id, query, from_ts, to_ts, max_rows, requested_by)
					 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		fractalID, body.Query, from, to, maxRows, u.Username).Scan(&id); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to enqueue search")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "id": id})
}

func (d routerDeps) handleListRecalls(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "fractalID")
	if _, ok := d.recallAnalystOK(w, r, fractalID); !ok {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 50 {
		limit = 20
	}
	rows, err := d.pg.Query(r.Context(),
		`SELECT id, query, from_ts, to_ts, status, row_count, is_aggregated, limit_hit,
					        read_rows, read_bytes, COALESCE(error, ''), created_at, started_at, finished_at
					 FROM archive_search_jobs WHERE fractal_id = $1 ORDER BY created_at DESC LIMIT $2`,
		fractalID, limit)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to list searches")
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
}

func (d routerDeps) handleGetRecall(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "fractalID")
	if _, ok := d.recallAnalystOK(w, r, fractalID); !ok {
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
	err := d.pg.QueryRow(r.Context(),
		`SELECT query, from_ts, to_ts, status, row_count, is_aggregated, limit_hit,
					        read_rows, read_bytes,
					        field_order, results, COALESCE(error, ''), created_at, started_at, finished_at
					 FROM archive_search_jobs WHERE id = $1 AND fractal_id = $2`,
		id, fractalID).Scan(&query, &from, &to, &status, &rowCount, &isAgg, &limitHit,
		&readRows, &readBytes,
		&fieldOrder, &results, &errMsg, &created, &started, &finished)
	if err == sql.ErrNoRows {
		api.WriteError(w, http.StatusNotFound, "Search not found")
		return
	}
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to load search")
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
}

func (d routerDeps) handleCancelRecall(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "fractalID")
	if _, ok := d.recallAnalystOK(w, r, fractalID); !ok {
		return
	}
	id := chi.URLParam(r, "id")
	res, err := d.pg.Exec(r.Context(),
		`UPDATE archive_search_jobs SET status = 'canceled', finished_at = NOW(), updated_at = NOW()
					 WHERE id = $1 AND fractal_id = $2 AND status IN ('pending', 'running')`, id, fractalID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to cancel")
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		api.WriteError(w, http.StatusConflict, "Search already finished")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

// addTimeIfValid sets m[key] to t's UTC time if valid, omitting the key
// entirely when NULL -- shared by the archive/maintain status blocks in the
// system/archive handler so the omit-on-NULL convention lives in one place.
func addTimeIfValid(m map[string]interface{}, key string, t sql.NullTime) {
	if t.Valid {
		m[key] = t.Time.UTC()
	}
}

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

// recallBytesHuman renders a byte count for the recall scan-limit rejection
// message (binary units, one decimal above KB).
func recallBytesHuman(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	f := float64(n)
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	i := -1
	for f >= unit && i < len(units)-1 {
		f /= unit
		i++
	}
	return fmt.Sprintf("%.1f %s", f, units[i])
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
