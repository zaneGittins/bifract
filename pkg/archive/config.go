package archive

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

// Config holds the archiver's runtime configuration, assembled from the same
// environment the server uses (Postgres connection) plus BIFRACT_ARCHIVE_*.
type Config struct {
	// Enabled is the startup gate from BIFRACT_ARCHIVE_ENABLED (env/secret).
	// The runtime admin-UI toggle (Postgres settings) is layered on in Phase 5.
	Enabled bool

	Obj   objstore.Config
	PGDSN string

	// CH is the ClickHouse connection used by restore/reconcile to write back
	// into logs. It is the same contract the server reads; see
	// storage.ClickHouseEnvFromOS.
	CH storage.ClickHouseEnv

	SpoolPath string

	// RollBytes is the PER-FRACTAL roll threshold, in PAYLOAD bytes: a fractal is
	// committed on its own once its buffered data reaches this, so Parquet file
	// size tracks RollBytes regardless of how many fractals are ingesting
	// concurrently. (A shared threshold across all fractals divides into one small
	// file per fractal per roll, which is what buries compaction.)
	//
	// Payload, not the approxSize memory estimate: see fractalBuffer. Payload is
	// uncompressed, so the file it produces is smaller by whatever zstd achieves
	// on that fractal's data; the per-commit log reports both so the ratio is
	// observed rather than guessed.
	RollBytes int64
	// RollInterval bounds staleness: every interval the whole buffer is flushed
	// and the spool checkpoint advances, however little each fractal has.
	RollInterval time.Duration

	// MaxPendingBytes bounds the archiver's total in-memory buffer, in approxSize
	// MEMORY bytes. RollBytes is per-fractal, so without this cap a deployment
	// with many active fractals would hold RollBytes x fractals of un-flushed
	// data. Over the cap the largest fractal is committed early (a smaller file,
	// logged so the trade is visible), which keeps memory bounded while still
	// favouring big files.
	//
	// Its unit differs from RollBytes on purpose: this one has to predict the
	// cgroup, that one has to predict the file. A buffer's memory always exceeds
	// its payload, by a factor set by the fractal's field density, so an operator
	// sizing this reads the ratio off the per-commit log rather than assuming 1:1.
	MaxPendingBytes int64

	// PollInterval is how often the run loop checks the spool when it has caught
	// up (no data available).
	PollInterval time.Duration

	// RecallTimeout is the FALLBACK per-search timeout used only when the live
	// recall_timeout_seconds admin setting is unset. On expiry the ClickHouse
	// query is killed and the job is marked failed, freeing the user's in-flight
	// slot. Seeded from BIFRACT_RECALL_TIMEOUT; the settings page is the live knob.
	RecallTimeout time.Duration

	// JobConcurrency caps how many recall (and, separately, restore) jobs run at
	// once across the whole deployment. Both hand their scan to ClickHouse, so
	// the resource worth bounding is concurrent archive scans against the
	// cluster, not worker processes. Enforced globally at claim time, so the
	// number does not drift with replica count.
	JobConcurrency int
}

// ConfigFromEnv assembles archiver config from the environment.
func ConfigFromEnv() (Config, error) {
	obj, err := objstore.FromEnv()
	if err != nil {
		return Config{}, err
	}
	chEnv, err := storage.ClickHouseEnvFromOS()
	if err != nil {
		return Config{}, err
	}
	c := Config{
		Enabled:       getBool("BIFRACT_ARCHIVE_ENABLED", false),
		Obj:           obj,
		PGDSN:         pgDSN(),
		CH:            chEnv,
		SpoolPath:     getStr("BIFRACT_ARCHIVE_SPOOL_PATH", "/var/lib/bifract/spool"),
		RollBytes:     getInt64("BIFRACT_ARCHIVE_ROLL_BYTES", 256<<20),
		RollInterval:  getDuration("BIFRACT_ARCHIVE_ROLL_INTERVAL", 30*time.Minute),
		PollInterval:  getDuration("BIFRACT_ARCHIVE_POLL_INTERVAL", 2*time.Second),
		RecallTimeout: getDuration("BIFRACT_RECALL_TIMEOUT", 15*time.Minute),

		MaxPendingBytes: getInt64("BIFRACT_ARCHIVE_MAX_PENDING_BYTES", 1<<30),
		JobConcurrency:  getIntEnv("BIFRACT_ARCHIVE_JOB_CONCURRENCY", 2),
	}
	if c.JobConcurrency < 1 {
		c.JobConcurrency = 1
	}
	// A cap below the per-fractal threshold would make the memory backstop, not
	// RollBytes, decide every commit. A buffer's memory is always at least its
	// payload, so this is the floor below which that is guaranteed rather than
	// merely likely; the early-commit log covers the rest of the range.
	if c.MaxPendingBytes < c.RollBytes {
		c.MaxPendingBytes = c.RollBytes
	}
	return c, nil
}

func pgDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		getStr("POSTGRES_HOST", "localhost"),
		getIntEnv("POSTGRES_PORT", 5432),
		getStr("POSTGRES_USER", "bifract"),
		getStr("POSTGRES_PASSWORD", "bifract"),
		getStr("POSTGRES_DB", "bifract"),
	)
}

func getStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func getInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return def
}

func getIntEnv(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func getDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}
