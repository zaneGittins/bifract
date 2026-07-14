// Command bifract-archiver drains the durable ingest spool into per-fractal
// Apache Iceberg tables (Parquet + metadata) on object storage, independent of
// ClickHouse health. It ships in the same image as bifract-server and runs as a
// sidecar with `command: ["/bifract-archiver"]`.
//
// Subcommands:
//
//	run        (default) tail the spool -> Iceberg append/commit loop
//	maintain   compaction, snapshot expiry, orphan cleanup (singleton)
//	restore    replay an Iceberg window back into ClickHouse (Phase 8)
//	reconcile  heal ClickHouse gaps from Iceberg (Phase 8)
package main

import (
	"context"
	"database/sql"
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
	case "restore":
		restoreCmd(os.Args[2:])
	case "reconcile":
		reconcileCmd(os.Args[2:])
	case "version":
		log.Printf("bifract-archiver %s", Version)
	default:
		log.Fatalf("unknown subcommand %q (want run|maintain|restore|reconcile|version)", cmd)
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

	// Restore worker: services admin-requested restore/reconcile jobs. It runs
	// independently of the drain enable gate (a DR restore may be needed while
	// ongoing archiving is paused) and builds the object-store catalog lazily on
	// the first job, so it does not break the dormant-but-present guarantee.
	go archive.NewRestoreWorker(cfg, db).Run(ctx)

	// Recall search worker: services per-fractal BQL searches over the Iceberg
	// archive. Same lazy-dep / independent-of-enable-gate design as restore.
	go archive.NewSearchWorker(cfg, db).Run(ctx)

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
// maintenance pass runs at a time (belt-and-suspenders alongside the k8s
// CronJob's concurrencyPolicy: Forbid).
const maintainAdvisoryLock = 0x62696672616d6169 // "bifrmai"

// maintainCmd runs compaction + snapshot expiry across all fractal tables. A
// singleton via a Postgres advisory lock: if another pass holds the lock, exit.
func maintainCmd() {
	cfg, err := archive.ConfigFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Opened before the disabled/lock checks (not just before cat.Maintain)
	// so every exit path below -- including a skip, not only a successful
	// pass -- can record its outcome; a run that never touches Postgres at
	// all would otherwise be indistinguishable from one that's healthy.
	db, err := sql.Open("postgres", cfg.PGDSN)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	if !cfg.Enabled {
		if v := archiveEnabledFromDB(cfg.PGDSN); !v {
			log.Println("maintain: archiving disabled; nothing to do")
			_ = archive.WriteMaintainOutcome(ctx, db, archive.MaintainOutcomeSkippedDisabled, nil)
			return
		}
	}
	archive.ApplyBackendEnv(cfg.Obj)

	var locked bool
	if err := db.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", int64(maintainAdvisoryLock)).Scan(&locked); err != nil {
		_ = archive.WriteMaintainOutcome(ctx, db, archive.MaintainOutcomeError, err)
		log.Fatalf("advisory lock: %v", err)
	}
	if !locked {
		log.Println("maintain: another maintenance pass is running; exiting")
		_ = archive.WriteMaintainOutcome(ctx, db, archive.MaintainOutcomeSkippedLocked, nil)
		return
	}
	defer db.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", int64(maintainAdvisoryLock))

	cat, err := archive.NewCatalog(ctx, "bifract", cfg.PGDSN, cfg.Obj)
	if err != nil {
		_ = archive.WriteMaintainOutcome(ctx, db, archive.MaintainOutcomeError, err)
		log.Fatalf("open catalog: %v", err)
	}
	log.Println("maintain: compaction + snapshot expiry ...")
	stats, err := cat.Maintain(ctx, archive.DefaultMaintainOptions())
	if err != nil {
		_ = archive.WriteMaintainOutcome(ctx, db, archive.MaintainOutcomeError, err)
		log.Fatalf("maintain failed: %v", err)
	}
	if err := archive.WriteMaintainStatus(ctx, db, stats); err != nil {
		log.Printf("maintain: failed to write status: %v", err)
	}
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
//	bifract-archiver restore --fractal <id> --from <ts> --to <ts> [--no-dedup]
func restoreCmd(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	fractal := fs.String("fractal", "", "fractal UUID (required)")
	fromS := fs.String("from", "", "window start (RFC3339 or 'YYYY-MM-DD [HH:MM:SS]', required)")
	toS := fs.String("to", "", "window end (exclusive, required)")
	noDedup := fs.Bool("no-dedup", false, "straight insert (skip windowed log_id anti-join)")
	fs.Parse(args)

	from, to := mustParseWindow(*fractal, *fromS, *toS)
	cfg, cat, ch := restoreDeps()
	defer ch.Close()

	log.Printf("restoring fractal %s [%s, %s) dedup=%v ...", *fractal, chFmt(from), chFmt(to), !*noDedup)
	n, err := cat.Restore(context.Background(), ch, cfg.Obj, *fractal, from, to, !*noDedup)
	if err != nil {
		log.Fatalf("restore failed: %v", err)
	}
	log.Printf("restore complete: %d rows inserted into logs", n)
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
	n, err := cat.Reconcile(context.Background(), ch, cfg.Obj, *fractal, from, to)
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
