package archive

import (
	"context"
	"fmt"
	"strings"
	"time"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

// chIcebergTableFunc builds the ClickHouse iceberg*() table-function expression
// that reads a fractal's Iceberg table directly from object storage. ClickHouse
// reads the metadata from the storage location (it does NOT use our Postgres
// catalog), so we hand it the table's base location.
func chIcebergTableFunc(obj objstore.Config, tableLocation string) (string, error) {
	loc := strings.TrimRight(tableLocation, "/") + "/"
	switch obj.Backend {
	case objstore.BackendDisk:
		path := strings.TrimPrefix(loc, "file://")
		return fmt.Sprintf("icebergLocal(%s)", chQuote(path)), nil

	case objstore.BackendS3, objstore.BackendMinIO:
		// s3://bucket/key... -> an HTTP(S) URL ClickHouse can fetch.
		rest := strings.TrimPrefix(loc, "s3://")
		bucket, key, _ := strings.Cut(rest, "/")
		var url string
		if obj.S3Endpoint != "" {
			// MinIO / custom endpoint: path-style.
			url = strings.TrimRight(obj.S3Endpoint, "/") + "/" + bucket + "/" + key
		} else {
			url = fmt.Sprintf("https://s3.%s.amazonaws.com/%s/%s", obj.S3Region, bucket, key)
		}
		if obj.S3AccessKey != "" {
			return fmt.Sprintf("icebergS3(%s, %s, %s)", chQuote(url), chQuote(obj.S3AccessKey), chQuote(obj.S3SecretKey)), nil
		}
		return fmt.Sprintf("icebergS3(%s)", chQuote(url)), nil

	case objstore.BackendAzure:
		// icebergAzure(storage_account_url, container, blobpath, account_name, account_key)
		rest := strings.TrimPrefix(strings.TrimPrefix(loc, "abfs://"), "abfss://")
		container, blobPath, _ := strings.Cut(rest, "/")
		endpoint := obj.AzureEndpoint
		if endpoint == "" {
			endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", obj.AzureAccount)
		}
		return fmt.Sprintf("icebergAzure(%s, %s, %s, %s, %s)",
			chQuote(endpoint), chQuote(container), chQuote(blobPath),
			chQuote(obj.AzureAccount), chQuote(obj.AzureKey)), nil
	}
	return "", fmt.Errorf("archive: unsupported backend %q for restore", obj.Backend)
}

// Restore replays a fractal's logs from Iceberg back into the ClickHouse logs
// table for the given event-time window. When dedup is true it skips log_ids
// already present in the same window (idempotent re-restore); when false it does
// a straight insert (fastest for a window that was fully deleted by retention).
// Returns the number of rows inserted.
func (c *Catalog) Restore(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, fractalID string, from, to time.Time, dedup bool) (int64, error) {
	loc, err := c.TableLocation(ctx, fractalID)
	if err != nil {
		return 0, fmt.Errorf("archive: no Iceberg table for fractal %s: %w", fractalID, err)
	}
	tf, err := chIcebergTableFunc(obj, loc)
	if err != nil {
		return 0, err
	}

	where := fmt.Sprintf("fractal_id = %s AND timestamp >= %s AND timestamp < %s",
		chQuote(fractalID), chQuote(chTime(from)), chQuote(chTime(to)))
	if dedup {
		// Windowed anti-join only (bounded by the restore range) so this stays
		// tractable; never a full-table NOT IN.
		where += fmt.Sprintf(
			" AND log_id NOT IN (SELECT log_id FROM logs WHERE fractal_id = %s AND timestamp >= %s AND timestamp < %s)",
			chQuote(fractalID), chQuote(chTime(from)), chQuote(chTime(to)))
	}

	// fields is a MAP in Iceberg; convert to the logs JSON column. norm_log is
	// left to its DEFAULT toString(fields).
	insert := fmt.Sprintf(
		"INSERT INTO logs (timestamp, raw_log, log_id, fields, fractal_id, ingest_timestamp, normalizer) "+
			"SELECT timestamp, raw_log, log_id, toJSONString(fields)::JSON, fractal_id, ingest_timestamp, normalizer "+
			"FROM %s WHERE %s", tf, where)

	before, err := countLogs(ctx, ch, fractalID, from, to)
	if err != nil {
		return 0, err
	}
	if err := ch.Exec(ctx, insert); err != nil {
		return 0, fmt.Errorf("archive: restore insert failed: %w", err)
	}
	after, err := countLogs(ctx, ch, fractalID, from, to)
	if err != nil {
		return 0, err
	}
	return after - before, nil
}

// Reconcile compares the log_id count in ClickHouse vs Iceberg for a window and,
// if Iceberg holds more (a gap in the hot store), restores the missing rows
// (with dedup). Returns rows restored.
func (c *Catalog) Reconcile(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, fractalID string, from, to time.Time) (int64, error) {
	chCount, err := countLogs(ctx, ch, fractalID, from, to)
	if err != nil {
		return 0, err
	}
	iceCount, err := c.countIceberg(ctx, ch, obj, fractalID, from, to)
	if err != nil {
		return 0, err
	}
	if iceCount <= chCount {
		return 0, nil
	}
	return c.Restore(ctx, ch, obj, fractalID, from, to, true)
}

func (c *Catalog) countIceberg(ctx context.Context, ch *storage.ClickHouseClient, obj objstore.Config, fractalID string, from, to time.Time) (int64, error) {
	loc, err := c.TableLocation(ctx, fractalID)
	if err != nil {
		return 0, err
	}
	tf, err := chIcebergTableFunc(obj, loc)
	if err != nil {
		return 0, err
	}
	q := fmt.Sprintf("SELECT count() AS c FROM %s WHERE fractal_id = %s AND timestamp >= %s AND timestamp < %s",
		tf, chQuote(fractalID), chQuote(chTime(from)), chQuote(chTime(to)))
	return scalarCount(ctx, ch, q)
}

func countLogs(ctx context.Context, ch *storage.ClickHouseClient, fractalID string, from, to time.Time) (int64, error) {
	q := fmt.Sprintf("SELECT count() AS c FROM logs WHERE fractal_id = %s AND timestamp >= %s AND timestamp < %s",
		chQuote(fractalID), chQuote(chTime(from)), chQuote(chTime(to)))
	return scalarCount(ctx, ch, q)
}

func scalarCount(ctx context.Context, ch *storage.ClickHouseClient, query string) (int64, error) {
	rows, err := ch.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	switch v := rows[0]["c"].(type) {
	case uint64:
		return int64(v), nil
	case int64:
		return v, nil
	default:
		return 0, fmt.Errorf("archive: unexpected count type %T", v)
	}
}

// chQuote single-quotes a string for a ClickHouse SQL literal, escaping quotes
// and backslashes.
func chQuote(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return "'" + s + "'"
}

func chTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
