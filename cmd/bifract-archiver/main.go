// Command bifract-archiver drains the durable ingest spool into per-fractal
// Apache Iceberg tables (Parquet + metadata) on object storage, independent of
// ClickHouse health. It ships in the same image as bifract-server and runs as a
// sidecar with `command: ["/bifract-archiver"]`.
//
// Subcommands:
//
//	run           (default) tail the spool -> Iceberg append/commit loop
//	maintain      one compaction + snapshot-expiry pass, then exit (manual/ad-hoc)
//	maintain-loop the always-on maintenance service: scheduled passes on a timer
//	              plus admin "Run now" requests. Runs as its own resource-capped
//	              workload on both platforms (Docker container, k8s replicas:1
//	              Deployment) so a compaction spike can't OOM the drain loop.
//	restore       replay an Iceberg window back into ClickHouse (Phase 8)
//	reconcile     heal ClickHouse gaps from Iceberg (Phase 8)
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bifract/pkg/archive"
	"bifract/pkg/spool"
	"bifract/pkg/storage"

	_ "github.com/KimMachineGun/automemlimit"
	_ "github.com/lib/pq"
	_ "go.uber.org/automaxprocs"
)

// archiveEnabledSetting mirrors the server's Postgres settings key so the tee
// and the archiver agree on the runtime on/off state.
const archiveEnabledSetting = "archive_enabled"

// Version is injected at build time via -ldflags.
var Version = "dev"

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.SetPrefix("[bifract-archiver] ")

	cmd := "run"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	switch cmd {
	case "run":
		runCmd()
	case "maintain":
		maintainCmd()
	case "maintain-loop":
		maintainLoopCmd()
	case "restore":
		restoreCmd(os.Args[2:])
	case "reconcile":
		reconcileCmd(os.Args[2:])
	case "version":
		log.Printf("bifract-archiver %s", Version)
	default:
		log.Fatalf("unknown subcommand %q (want run|maintain|maintain-loop|restore|reconcile|version)", cmd)
	}
}

func runCmd() {
	cfg, err := archive.ConfigFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The runtime on/off state lives in the Postgres settings table so the admin
	// UI can toggle it without a redeploy; BIFRACT_ARCHIVE_ENABLED seeds it.
	db, err := sql.Open("postgres", cfg.PGDSN)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	enabled := func() bool { return archiveEnabled(ctx, db, cfg.Enabled) }

	// Recall and restore workers do NOT run here. This process is the spool
	// drainer, co-located with an ingest pod because the spool is that pod's own
	// volume. The job queues only dispatch to ClickHouse and have no such affinity,
	// so bifract-server hosts them: scaling ingest down must not leave archive jobs
	// stuck at 'pending'.

	// Dormant-but-present: idle until archiving is enabled, so a provisioned but
	// disabled archive never needs the object store to be reachable.
	for !enabled() {
		log.Printf("archive disabled; idling (poll %s)", cfg.PollInterval)
		if !sleepCtx(ctx, maxDur(cfg.PollInterval, 5*time.Second)) {
			log.Println("shutting down")
			return
		}
	}

	log.Printf("archive enabled: backend=%s warehouse=%s spool=%s roll=%dMiB/%s",
		cfg.Obj.Backend, cfg.Obj.WarehouseURI(), cfg.SpoolPath,
		cfg.RollBytes>>20, cfg.RollInterval)

	// Export cloud-SDK env so both the catalog IO and iceberg-go's transaction
	// write path resolve the same endpoint/region/credentials.
	archive.ApplyBackendEnv(cfg.Obj)

	reader, err := spool.NewReader(cfg.SpoolPath)
	if err != nil {
		log.Fatalf("open spool: %v", err)
	}
	defer reader.Close()

	cat, err := archive.NewCatalog(ctx, "bifract", cfg.PGDSN, cfg.Obj)
	if err != nil {
		log.Fatalf("open catalog: %v", err)
	}

	arch := archive.NewArchiver(cfg, reader, cat, enabled, db)
	log.Println("archiver running")
	if err := arch.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("archiver stopped: %v", err)
	}
	log.Println("archiver stopped cleanly")
}

// maintainAdvisoryLock is a fixed Postgres advisory-lock key ensuring only one
// maintenance pass runs at a time. It is the authoritative singleton guard now
// that maintenance runs as a perpetual maintain-loop on both platforms (Docker
// container, k8s replicas:1 Deployment): a rolling update can briefly overlap
// two pods, and the lock serializes them so they never fight over table
// metadata.
const maintainAdvisoryLock = 0x62696672616d6169 // "bifrmai"

// defaultMaintainInterval is the maintain-loop scheduled cadence when
// BIFRACT_ARCHIVE_MAINTAIN_INTERVAL is unset or invalid. Hourly is plenty given
// roll-on-size already keeps most files large; compaction is a mop-up for
// small-file tails, not a constant grind, and admins can trigger an immediate
// pass from the UI ("Run now") without waiting for the next tick.
const defaultMaintainInterval = time.Hour

// maintainPollInterval is how often maintain-loop wakes to check for an
// admin-requested "Run now" pass or an elapsed scheduled interval. Small enough
// that Run now feels responsive in the admin UI, large enough that the idle
// poll is negligible load on Postgres.
const maintainPollInterval = 10 * time.Second

// defaultMaintainPassTimeout bounds a single pass so a wedged object-store call
// cannot hold the singleton lock and block every later pass indefinitely. The
// loop blocks in runMaintainOnce for the whole pass, so an unbounded hang stops
// maintenance permanently rather than just skipping a run. Sized just under the
// hourly cadence: a pass that has not finished within it is not going to.
// Override with BIFRACT_ARCHIVE_MAINTAIN_PASS_TIMEOUT, but keep it under the
// server's maintainStaleAfter (100m): past that the admin panel ages a
// still-running pass out as "Interrupted". Cosmetic only -- the advisory lock
// still serializes passes -- but misleading.
const defaultMaintainPassTimeout = 55 * time.Minute

// maintainPassTimeout reads the per-pass bound. Non-positive disables it, for
// an operator deliberately running an enormous catch-up pass by hand.
func maintainPassTimeout() time.Duration {
	raw := os.Getenv("BIFRACT_ARCHIVE_MAINTAIN_PASS_TIMEOUT")
	if raw == "" {
		return defaultMaintainPassTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("maintain: invalid BIFRACT_ARCHIVE_MAINTAIN_PASS_TIMEOUT %q: %v; using %s", raw, err, defaultMaintainPassTimeout)
		return defaultMaintainPassTimeout
	}
	return d
}

// maintainCmd runs a single compaction + snapshot-expiry pass, then exits. It
// remains for manual/ad-hoc invocation (`bifract-archiver maintain`) and is a
// singleton via the advisory lock in runMaintainOnce. Exits non-zero on a
// genuine failure; a disabled/locked skip exits zero after recording its
// outcome.
func maintainCmd() {
	cfg, err := archive.ConfigFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	db, err := sql.Open("postgres", cfg.PGDSN)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	if err := runMaintainOnce(context.Background(), cfg, db); err != nil {
		log.Fatalf("maintain failed: %v", err)
	}
}

// maintainLoopCmd is the always-on maintenance service that runs on both
// platforms: a dedicated bifract-archiver-maintain container (Docker) or a
// replicas:1 Deployment (k8s). Keeping compaction in its own resource-capped
// workload means a compaction memory spike OOMs only this (mostly idle) process
// and never the drain loop. It services two triggers:
//
//   - scheduled: a pass every BIFRACT_ARCHIVE_MAINTAIN_INTERVAL (default 1h)
//   - on demand: an admin "Run now" request, claimed from Postgres, serviced
//     within maintainPollInterval
//
// A short poll ticker drives both so Run now does not have to wait a full
// interval. The advisory lock in runMaintainOnce keeps it a singleton even if a
// rolling update briefly runs two pods.
func maintainLoopCmd() {
	interval := maintainLoopInterval()
	cfg, err := archive.ConfigFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	db, err := sql.Open("postgres", cfg.PGDSN)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("maintain-loop: scheduled every %s, checking for run-now every %s", interval, maintainPollInterval)

	// A killed pass leaves the status row claiming 'running'. Reconciling at
	// startup surfaces it within seconds of the restart instead of at the next
	// scheduled pass, which is the difference between noticing an OOMKill loop
	// and watching it run all night behind a green "Running now".
	reconcileInterruptedPass(ctx, db)

	// Scheduled cadence is tracked in-process, seeded to now: a fresh start (or a
	// restart / rolling update) waits a full interval before the first scheduled
	// pass rather than compacting on every boot. On-demand "Run now" is always
	// available immediately regardless of this clock.
	lastRun := time.Now()

	t := time.NewTicker(maintainPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("maintain-loop: shutting down")
			return
		case <-t.C:
			reason := ""
			// On-demand takes priority and is claimed atomically, so a request is
			// serviced exactly once even if two maintainer pods briefly overlap.
			if by, ok, err := archive.ClaimMaintainRunRequest(ctx, db); err != nil {
				log.Printf("maintain-loop: check run request: %v", err)
			} else if ok {
				reason = "run-now"
				if by != "" {
					reason = "run-now (requested by " + by + ")"
				}
			} else if time.Since(lastRun) >= interval {
				reason = "scheduled"
			}
			if reason == "" {
				continue
			}
			log.Printf("maintain-loop: starting pass: %s", reason)
			passStart := time.Now()
			// Log and continue: a transient failure (object-store blip, lost commit
			// race, disabled toggle) must not kill the loop. The outcome is already
			// persisted by runMaintainOnce for the admin panel.
			if err := runMaintainOnce(ctx, cfg, db); err != nil {
				log.Printf("maintain-loop: pass failed: %v", err)
			}
			// Anchored to when the pass STARTED, not when it finished, so the cadence
			// is the interval rather than interval plus pass duration. Finishing-time
			// anchoring turned an hourly schedule into one pass every 1h55m once
			// passes started running to their 55m timeout. A pass that overruns the
			// interval simply makes the next one due immediately, which is the same
			// behaviour as a CronJob with concurrencyPolicy Forbid.
			lastRun = passStart
		}
	}
}

// maintainLoopInterval reads the scheduled-cadence override. Unset, invalid, or
// non-positive all fall back to defaultMaintainInterval -- disabling scheduled
// maintenance is done by not running the maintainer (scale the container/Deployment
// to 0), never by a magic value here.
func maintainLoopInterval() time.Duration {
	raw := os.Getenv("BIFRACT_ARCHIVE_MAINTAIN_INTERVAL")
	if raw == "" {
		return defaultMaintainInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("maintain-loop: invalid BIFRACT_ARCHIVE_MAINTAIN_INTERVAL %q: %v; using %s", raw, err, defaultMaintainInterval)
		return defaultMaintainInterval
	}
	if d <= 0 {
		log.Printf("maintain-loop: non-positive BIFRACT_ARCHIVE_MAINTAIN_INTERVAL %q; using %s", raw, defaultMaintainInterval)
		return defaultMaintainInterval
	}
	return d
}

// runMaintainOnce executes the gated, singleton maintenance pass shared by the
// one-shot `maintain` command and the `maintain-loop` scheduler: skip if
// archiving is disabled, skip if another pass holds the advisory lock, else
// compact + expire snapshots across every fractal table and record the outcome.
//
// db is opened by the caller before any of these checks so every exit path --
// including a skip, not only a successful pass -- can record its outcome; a run
// that never touched Postgres would be indistinguishable from a healthy one.
// Returns an error only for a genuine pass failure (the caller decides whether
// to exit or continue); a disabled/locked skip returns nil after writing status.
func runMaintainOnce(parent context.Context, cfg archive.Config, db *sql.DB) error {
	if !maintainEnabled(cfg) {
		log.Println("maintain: archiving disabled; nothing to do")
		_ = archive.WriteMaintainOutcome(parent, db, archive.MaintainOutcomeSkippedDisabled, nil)
		return nil
	}
	archive.ApplyBackendEnv(cfg.Obj)

	// The advisory lock is session-scoped, so it must be taken and released on one
	// pinned connection. Taken from the pool it can be unlocked on a different
	// connection than it was locked on, which fails and leaks the lock for that
	// connection's lifetime -- every later pass then skips as "locked" forever.
	conn, err := db.Conn(parent)
	if err != nil {
		_ = archive.WriteMaintainOutcome(parent, db, archive.MaintainOutcomeError, err)
		return fmt.Errorf("advisory lock connection: %w", err)
	}
	defer conn.Close()

	var locked bool
	if err := conn.QueryRowContext(parent, "SELECT pg_try_advisory_lock($1)", int64(maintainAdvisoryLock)).Scan(&locked); err != nil {
		_ = archive.WriteMaintainOutcome(parent, db, archive.MaintainOutcomeError, err)
		return fmt.Errorf("advisory lock: %w", err)
	}
	if !locked {
		log.Println("maintain: another maintenance pass is running; skipping")
		_ = archive.WriteMaintainOutcome(parent, db, archive.MaintainOutcomeSkippedLocked, nil)
		return nil
	}
	// WithoutCancel so shutdown and pass timeout still release the lock. A failure
	// here returns the connection to the pool still holding a session-scoped lock,
	// which would make every later pass skip as "locked" with no other symptom, so
	// it is logged rather than discarded.
	defer func() {
		if _, err := conn.ExecContext(context.WithoutCancel(parent),
			"SELECT pg_advisory_unlock($1)", int64(maintainAdvisoryLock)); err != nil {
			log.Printf("maintain: failed to release the advisory lock: %v", err)
		}
	}()

	// Holding the lock proves no pass is in flight, so a row still marked running
	// belongs to a pass that was killed before it could record anything.
	if found, err := archive.ReconcileInterruptedMaintain(parent, db,
		"pass was killed before it could record an outcome (OOMKill, eviction, or restart)"); err != nil {
		log.Printf("maintain: reconcile interrupted pass: %v", err)
	} else if found {
		log.Println("maintain: previous pass did not finish; recorded as interrupted")
	}

	// Bound the pass. The loop blocks here for its whole duration, so an
	// unbounded hang inside object storage stops maintenance permanently.
	ctx := parent
	if timeout := maintainPassTimeout(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, timeout)
		defer cancel()
	}

	// Stamp an in-progress marker so the admin UI can show a live "running" state
	// for the duration of the pass (catalog open + compaction), between here and
	// the terminal WriteMaintainStatus/WriteMaintainOutcome below.
	_ = archive.MarkMaintainRunning(ctx, db)

	cat, err := archive.NewCatalog(ctx, "bifract", cfg.PGDSN, cfg.Obj)
	if err != nil {
		_ = archive.WriteMaintainOutcome(parent, db, maintainFailureOutcome(ctx, err), err)
		return fmt.Errorf("open catalog: %w", err)
	}
	opts := archive.MaintainOptionsFromEnv()
	// Per-fractal archive retention. A lookup failure must not skip the whole
	// pass: compaction and expiry are still worth running, and the next pass
	// retries retention. Absent policy means keep forever, so failing open here
	// keeps data rather than dropping it.
	if policy, err := archive.LoadRetentionPolicy(ctx, db); err != nil {
		log.Printf("maintain: archive retention policy unavailable, skipping retention this pass: %v", err)
	} else {
		opts.Retention = policy
	}

	// Orphan cleanup walks every table's full location, so it runs on its own
	// cadence rather than every pass. Skipping is always safe: orphans are already
	// unreachable bytes, so a deferred sweep costs storage, never correctness.
	orphanInterval := archive.OrphanSweepIntervalFromEnv()
	if sweep, err := archive.ClaimOrphanSweep(ctx, db, orphanInterval); err != nil {
		log.Printf("maintain: orphan sweep claim failed, skipping this pass: %v", err)
		opts.OrphanOlderThan = 0
	} else if !sweep {
		opts.OrphanOlderThan = 0
	} else {
		log.Printf("maintain: orphan sweep due (every %s)", orphanInterval)
	}

	log.Println("maintain: compaction + retention + snapshot expiry ...")
	stats, err := cat.Maintain(ctx, opts)
	if err != nil {
		// parent, not ctx: on a timeout ctx is already cancelled, and the whole
		// point of the outcome write is to record that fact. The stats go with it
		// because a pass abandoned at its deadline still did real work.
		_ = archive.WriteMaintainStatusOutcome(parent, db, stats, maintainFailureOutcome(ctx, err), err)
		// Deliberately no WriteFootprint here: a truncated pass only summed the
		// tables it reached, so publishing it would overwrite the real archive size
		// with a fraction of it. That is why a timed-out pass used to report a
		// footprint that had apparently collapsed overnight.
		return fmt.Errorf("maintain: %w", err)
	}
	if err := archive.WriteMaintainStatus(parent, db, stats); err != nil {
		log.Printf("maintain: failed to write status: %v", err)
	}
	// The pass just loaded every table, so this is the cheapest place in the
	// system to learn the archive's size. The drain loop no longer computes it.
	if err := archive.WriteFootprint(parent, db, stats.Tables, stats.FootprintBytes, stats.FootprintRecords); err != nil {
		log.Printf("maintain: failed to write footprint: %v", err)
	}
	return nil
}

// reconcileInterruptedPass records a previous pass that died without writing an
// outcome. It takes the maintain advisory lock first: acquiring it proves no
// pass is actually running, so a lingering 'running' marker is residue rather
// than a live pass. Entirely best-effort -- failing to claim the lock just means
// another maintainer is mid-pass, which is the normal case during a rolling
// update, and the next pass reconciles anyway.
func reconcileInterruptedPass(ctx context.Context, db *sql.DB) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	var locked bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", int64(maintainAdvisoryLock)).Scan(&locked); err != nil || !locked {
		return
	}
	defer func() {
		if _, err := conn.ExecContext(context.WithoutCancel(ctx),
			"SELECT pg_advisory_unlock($1)", int64(maintainAdvisoryLock)); err != nil {
			log.Printf("maintain-loop: failed to release the advisory lock: %v", err)
		}
	}()

	if found, err := archive.ReconcileInterruptedMaintain(ctx, db,
		"pass was killed before it could record an outcome (OOMKill, eviction, or restart)"); err != nil {
		log.Printf("maintain-loop: reconcile interrupted pass: %v", err)
	} else if found {
		log.Println("maintain-loop: previous pass did not finish; recorded as interrupted")
	}
}

// maintainFailureOutcome classifies a failed pass: a pass context that expired
// on its own deadline is a timeout, anything else is an ordinary error. A
// cancelled parent (shutdown) is not a deadline, so it stays an error.
func maintainFailureOutcome(ctx context.Context, err error) archive.MaintainOutcome {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return archive.MaintainOutcomeTimeout
	}
	return archive.MaintainOutcomeError
}

// maintainEnabled reports whether archiving is on: the env seed, or the runtime
// archive_enabled setting in Postgres, so an admin toggle is honored between
// loop passes without a redeploy.
func maintainEnabled(cfg archive.Config) bool {
	if cfg.Enabled {
		return true
	}
	return archiveEnabledFromDB(cfg.PGDSN)
}

// archiveEnabledFromDB reads the runtime archive_enabled setting.
func archiveEnabledFromDB(dsn string) bool {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return false
	}
	defer db.Close()
	var v string
	if err := db.QueryRow("SELECT value FROM settings WHERE key=$1", archiveEnabledSetting).Scan(&v); err != nil {
		return false
	}
	return v == "true"
}

// restoreCmd replays an Iceberg event-time window back into ClickHouse.
//
//	bifract-archiver restore --fractal <id> --from <ts> --to <ts> [--target <id>]
//
// --target routes the restored rows into a different fractal (restore into a
// dedicated workspace); it defaults to --fractal for a self-restore. The CLI does
// not create the target fractal; it must already exist. Restore is always deduped.
func restoreCmd(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	fractal := fs.String("fractal", "", "source fractal UUID whose archive to read (required)")
	target := fs.String("target", "", "destination fractal UUID (default: same as --fractal)")
	fromS := fs.String("from", "", "window start (RFC3339 or 'YYYY-MM-DD [HH:MM:SS]', required)")
	toS := fs.String("to", "", "window end (exclusive, required)")
	fs.Parse(args)

	from, to := mustParseWindow(*fractal, *fromS, *toS)
	tgt := *target
	if tgt == "" {
		tgt = *fractal
	}
	cfg, cat, ch := restoreDeps()
	defer ch.Close()

	log.Printf("restoring fractal %s -> %s ingested [%s, %s) ...", *fractal, tgt, chFmt(from), chFmt(to))
	n, err := cat.Restore(context.Background(), ch, cfg.Obj, *fractal, tgt, from, to, "", logRestoreChunk)
	if err != nil {
		log.Fatalf("restore failed: %v", err)
	}
	log.Printf("restore complete: %d rows inserted into logs", n)
}

// logRestoreChunk reports per-chunk progress for the one-shot CLI commands. The
// timestamp it prints is the resume point: re-running with --from set to it
// continues where an interrupted run stopped.
func logRestoreChunk(next time.Time, chunksDone, chunksTotal int, rowsSoFar int64) {
	log.Printf("  chunk %d/%d done, %d row(s) so far; resume point %s", chunksDone, chunksTotal, rowsSoFar, chFmt(next))
}

// reconcileCmd heals a ClickHouse gap from Iceberg (restores when Iceberg holds
// more than the hot store for the window).
//
//	bifract-archiver reconcile --fractal <id> --from <ts> --to <ts>
func reconcileCmd(args []string) {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	fractal := fs.String("fractal", "", "fractal UUID (required)")
	fromS := fs.String("from", "", "window start (required)")
	toS := fs.String("to", "", "window end (exclusive, required)")
	fs.Parse(args)

	from, to := mustParseWindow(*fractal, *fromS, *toS)
	cfg, cat, ch := restoreDeps()
	defer ch.Close()

	log.Printf("reconciling fractal %s [%s, %s) ...", *fractal, chFmt(from), chFmt(to))
	n, err := cat.Reconcile(context.Background(), ch, cfg.Obj, *fractal, from, to, "", logRestoreChunk)
	if err != nil {
		log.Fatalf("reconcile failed: %v", err)
	}
	if n == 0 {
		log.Printf("reconcile: no gap (ClickHouse already has the window)")
	} else {
		log.Printf("reconcile complete: %d missing rows restored", n)
	}
}

// restoreDeps builds the shared config, catalog, and ClickHouse client for the
// restore/reconcile commands.
func restoreDeps() (archive.Config, *archive.Catalog, *storage.ClickHouseClient) {
	cfg, err := archive.ConfigFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	archive.ApplyBackendEnv(cfg.Obj)
	cat, err := archive.NewCatalog(context.Background(), "bifract", cfg.PGDSN, cfg.Obj)
	if err != nil {
		log.Fatalf("open catalog: %v", err)
	}
	ch, err := archive.NewCHClient(cfg)
	if err != nil {
		log.Fatalf("connect clickhouse: %v", err)
	}
	return cfg, cat, ch
}

func mustParseWindow(fractal, fromS, toS string) (time.Time, time.Time) {
	if fractal == "" || fromS == "" || toS == "" {
		log.Fatalf("--fractal, --from and --to are all required")
	}
	from, err := parseTime(fromS)
	if err != nil {
		log.Fatalf("invalid --from: %v", err)
	}
	to, err := parseTime(toS)
	if err != nil {
		log.Fatalf("invalid --to: %v", err)
	}
	if !to.After(from) {
		log.Fatalf("--to must be after --from")
	}
	return from, to
}

func parseTime(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized time %q (want RFC3339 or 'YYYY-MM-DD [HH:MM:SS]')", s)
}

func chFmt(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05") }

// archiveEnabled reports the effective runtime state: the archive_enabled
// setting if present, otherwise the env seed.
func archiveEnabled(ctx context.Context, db *sql.DB, envSeed bool) bool {
	var v string
	err := db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key=$1", archiveEnabledSetting).Scan(&v)
	if err != nil {
		return envSeed
	}
	return v == "true"
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
