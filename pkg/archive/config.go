package archive

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"bifract/pkg/objstore"
)

// Config holds the archiver's runtime configuration, assembled from the same
// environment the server uses (Postgres connection) plus BIFRACT_ARCHIVE_*.
type Config struct {
	// Enabled is the startup gate from BIFRACT_ARCHIVE_ENABLED (env/secret).
	// The runtime admin-UI toggle (Postgres settings) is layered on in Phase 5.
	Enabled bool

	Obj   objstore.Config
	PGDSN string

	// ClickHouse connection (used by restore/reconcile to write back into logs).
	CHHost     string
	CHPort     int
	CHDatabase string
	CHUser     string
	CHPassword string

	SpoolPath string

	// RollBytes / RollInterval flush accumulated per-fractal data to Iceberg on
	// whichever comes first, bounding file size and staleness.
	RollBytes    int64
	RollInterval time.Duration

	// PollInterval is how often the run loop checks the spool when it has caught
	// up (no data available).
	PollInterval time.Duration
}

// ConfigFromEnv assembles archiver config from the environment.
func ConfigFromEnv() (Config, error) {
	obj, err := objstore.FromEnv()
	if err != nil {
		return Config{}, err
	}
	c := Config{
		Enabled:      getBool("BIFRACT_ARCHIVE_ENABLED", false),
		Obj:          obj,
		PGDSN:        pgDSN(),
		CHHost:       getStr("CLICKHOUSE_HOST", "localhost"),
		CHPort:       getIntEnv("CLICKHOUSE_PORT", 9000),
		CHDatabase:   getStr("CLICKHOUSE_DB", "logs"),
		CHUser:       getStr("CLICKHOUSE_USER", "default"),
		CHPassword:   getStr("CLICKHOUSE_PASSWORD", "bifract"),
		SpoolPath:    getStr("BIFRACT_ARCHIVE_SPOOL_PATH", "/var/lib/bifract/spool"),
		RollBytes:    getInt64("BIFRACT_ARCHIVE_ROLL_BYTES", 256<<20),
		RollInterval: getDuration("BIFRACT_ARCHIVE_ROLL_INTERVAL", time.Hour),
		PollInterval: getDuration("BIFRACT_ARCHIVE_POLL_INTERVAL", 2*time.Second),
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
