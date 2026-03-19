package archives

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"bifract/pkg/backup"
	"bifract/pkg/storage"
)

// archiveLogRowV1 is the NDJSON schema for v1 archives (no fields, re-derived on restore via ingest).
type archiveLogRowV1 struct {
	Timestamp       time.Time `json:"ts"`
	RawLog          string    `json:"raw"`
	LogID           string    `json:"id"`
	FractalID       string    `json:"fid"`
	IngestTimestamp time.Time `json:"its"`
}

// archiveHeader is the first line of a v1 archive file.
type archiveHeader struct {
	Version int `json:"_bifract_archive_version"`
}

// Manager handles archive creation and restoration.
type Manager struct {
	pg       *storage.PostgresClient
	ch       *storage.ClickHouseClient
	store    backup.StorageBackend
	archives *Storage
	key      []byte

	mu      sync.Mutex
	running map[string]context.CancelFunc
	wg      sync.WaitGroup
}

// NewManager creates a new archive manager.
func NewManager(pg *storage.PostgresClient, ch *storage.ClickHouseClient, store backup.StorageBackend, archiveStore *Storage) *Manager {
	key, err := backup.LoadBackupKey()
	if err != nil {
		log.Printf("[Archives] Warning: backup encryption key not configured: %v", err)
	}

	return &Manager{
		pg:       pg,
		ch:       ch,
		store:    store,
		archives: archiveStore,
		key:      key,
		running:  make(map[string]context.CancelFunc),
	}
}

// RecoverInterrupted marks any archives left in an active state as failed.
// This handles the case where the server crashed mid-archive.
func (m *Manager) RecoverInterrupted(ctx context.Context) {
	count, err := m.archives.FailInterruptedArchives(ctx)
	if err != nil {
		log.Printf("[Archives] Failed to recover interrupted archives: %v", err)
		return
	}
	if count > 0 {
		log.Printf("[Archives] Marked %d interrupted archive(s) as failed", count)
	}
}

// CreateArchive starts an asynchronous archive operation for a fractal.
func (m *Manager) CreateArchive(ctx context.Context, fractalID, createdBy string, retentionDays *int, archiveType string) (string, error) {
	if m.store == nil {
		return "", fmt.Errorf("archive storage not configured")
	}
	if len(m.key) == 0 {
		return "", fmt.Errorf("backup encryption key not configured")
	}

	// Check for active operations
	active, err := m.archives.HasActiveOperation(ctx, fractalID)
	if err != nil {
		return "", fmt.Errorf("check active operation: %w", err)
	}
	if active {
		return "", fmt.Errorf("an archive operation is already in progress for this fractal")
	}

	// Determine storage type
	storageType := "disk"
	if _, ok := m.store.(*backup.S3Storage); ok {
		storageType = "s3"
	}

	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("%s-%s.bifract-archive", fractalID, timestamp)
	storagePath := fmt.Sprintf("archives/%s/%s", fractalID, filename)

	// Pin the time window: archive_end_ts is set at creation so new logs
	// arriving after this point are excluded from the archive.
	archive, err := m.archives.CreateArchive(ctx, fractalID, filename, storageType, storagePath, createdBy, archiveType)
	if err != nil {
		return "", err
	}

	archiveCtx, cancel := context.WithCancel(context.Background())

	m.mu.Lock()
	m.running[archive.ID] = cancel
	m.mu.Unlock()

	m.wg.Add(1)
	go m.runArchive(archiveCtx, archive, retentionDays, storagePath)

	return archive.ID, nil
}

func (m *Manager) runArchive(ctx context.Context, archive *Archive, retentionDays *int, storagePath string) {
	defer m.wg.Done()
	defer func() {
		m.mu.Lock()
		delete(m.running, archive.ID)
		m.mu.Unlock()
	}()

	log.Printf("[Archives] Starting archive %s for fractal %s", archive.ID, archive.FractalID)

	if err := m.executeArchive(ctx, archive, retentionDays, storagePath); err != nil {
		log.Printf("[Archives] Archive %s failed: %v", archive.ID, err)
		// Use SetArchiveStatus to preserve the cursor checkpoint and log_count
		// recorded by UpdateArchiveCursor during archiving. UpdateArchiveStatus
		// would overwrite those fields with zeros.
		m.archives.SetArchiveStatusWithError(context.Background(), archive.ID, StatusFailed, err.Error())
		return
	}

	log.Printf("[Archives] Archive %s completed", archive.ID)
}

func (m *Manager) executeArchive(ctx context.Context, archive *Archive, retentionDays *int, storagePath string) error {
	archiveEndTS := archive.ArchiveEndTS
	if archiveEndTS == nil {
		now := time.Now()
		archiveEndTS = &now
	}

	// Set up streaming pipeline: CH HTTP -> reformat -> zstd -> encrypt -> storage
	pr, pw := io.Pipe()

	var writeErr error
	var written int64
	writeDone := make(chan struct{})

	go func() {
		defer close(writeDone)
		written, writeErr = m.store.Write(ctx, storagePath, pr)
		if writeErr != nil {
			pr.CloseWithError(writeErr)
		}
	}()

	encWriter, err := backup.NewEncryptingWriter(pw, m.key)
	if err != nil {
		pw.Close()
		<-writeDone
		return fmt.Errorf("create encrypting writer: %w", err)
	}

	zstdWriter, err := zstd.NewWriter(encWriter,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		encWriter.Close()
		pw.Close()
		<-writeDone
		return fmt.Errorf("create zstd writer: %w", err)
	}

	archiveWriter := bufio.NewWriterSize(zstdWriter, 256*1024) // 256KB buffer

	// Write v1 format header
	header, _ := json.Marshal(archiveHeader{Version: 1})
	header = append(header, '\n')
	if _, err := archiveWriter.Write(header); err != nil {
		zstdWriter.Close()
		encWriter.Close()
		pw.CloseWithError(err)
		<-writeDone
		return fmt.Errorf("write archive header: %w", err)
	}

	var logCount int64
	var timeStart, timeEnd *time.Time
	var cursorTS time.Time
	var cursorID string
	firstQuery := true

	// Stream data from ClickHouse HTTP interface with cursor-based retry.
	// Each iteration opens a streaming query from the cursor position.
	// On transient failure, the cursor resumes from the last written row.
	const maxConsecutiveRetries = 10
	maxArchiveDuration := getArchiveMaxDuration()
	maxErrorTime := getArchiveMaxErrorTime()

	consecutiveErrors := 0
	backoff := 5 * time.Second
	archiveStart := time.Now()
	var cumulativeErrorTime time.Duration

	for {
		if ctx.Err() != nil {
			break
		}

		if elapsed := time.Since(archiveStart); elapsed > maxArchiveDuration {
			zstdWriter.Close()
			encWriter.Close()
			pw.CloseWithError(fmt.Errorf("exceeded max archive duration"))
			<-writeDone
			return fmt.Errorf("archive exceeded max duration of %v (%d logs archived)", maxArchiveDuration, logCount)
		}

		rowsRead, streamErr := m.streamArchiveHTTP(ctx, archive.FractalID, retentionDays, firstQuery, cursorTS, cursorID, *archiveEndTS, archiveWriter, &logCount, &cursorTS, &cursorID, &timeStart, &timeEnd)

		if streamErr != nil {
			if !isTransientClickHouseError(streamErr) {
				zstdWriter.Close()
				encWriter.Close()
				pw.CloseWithError(streamErr)
				<-writeDone
				return fmt.Errorf("stream archive: %w", streamErr)
			}

			consecutiveErrors++
			if consecutiveErrors > maxConsecutiveRetries {
				zstdWriter.Close()
				encWriter.Close()
				pw.CloseWithError(streamErr)
				<-writeDone
				return fmt.Errorf("stream archive after %d consecutive retries: %w", maxConsecutiveRetries, streamErr)
			}

			cumulativeErrorTime += backoff
			if cumulativeErrorTime > maxErrorTime {
				zstdWriter.Close()
				encWriter.Close()
				pw.CloseWithError(streamErr)
				<-writeDone
				return fmt.Errorf("archive exceeded max cumulative error wait time of %v (%d logs archived): %w", maxErrorTime, logCount, streamErr)
			}

			// Save cursor progress if rows were read before the error.
			if rowsRead > 0 {
				firstQuery = false
				m.archives.UpdateArchiveCursor(ctx, archive.ID, cursorTS, cursorID, logCount)
			}

			log.Printf("[Archives] Transient error (attempt %d/%d, %d rows before error, %d total), retrying in %v: %v",
				consecutiveErrors, maxConsecutiveRetries, rowsRead, logCount, backoff, streamErr)
			select {
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 60*time.Second {
					backoff = 60 * time.Second
				}
			case <-ctx.Done():
				break
			}
			if logCount > 0 {
				firstQuery = false
			}
			continue
		}

		// Stream completed without error. If zero rows were read, we're done.
		if rowsRead == 0 {
			break
		}

		// Successful stream resets consecutive error tracking.
		consecutiveErrors = 0
		backoff = 5 * time.Second
		firstQuery = false

		m.archives.UpdateArchiveCursor(ctx, archive.ID, cursorTS, cursorID, logCount)
		if logCount%1000000 == 0 {
			log.Printf("[Archives] Archive %s progress: %d logs archived", archive.ID, logCount)
		}
	}

	// Flush and close pipeline in order: buffer -> zstd -> encrypt -> pipe
	if err := archiveWriter.Flush(); err != nil {
		zstdWriter.Close()
		encWriter.Close()
		pw.CloseWithError(err)
		<-writeDone
		return fmt.Errorf("flush archive writer: %w", err)
	}
	if err := zstdWriter.Close(); err != nil {
		encWriter.Close()
		pw.CloseWithError(err)
		<-writeDone
		return fmt.Errorf("close zstd: %w", err)
	}
	if err := encWriter.Close(); err != nil {
		pw.CloseWithError(err)
		<-writeDone
		return fmt.Errorf("close encrypt: %w", err)
	}
	pw.Close()

	<-writeDone
	if writeErr != nil {
		return fmt.Errorf("write archive: %w", writeErr)
	}

	return m.archives.UpdateArchiveStatus(
		context.Background(), archive.ID, StatusCompleted, "",
		written, logCount, timeStart, timeEnd,
	)
}

// archiveHTTPClient is used for streaming queries to ClickHouse's HTTP interface.
// The timeout is intentionally zero (no limit) because archive streams can run
// for hours on large fractals; cancellation is handled via context.
var archiveHTTPClient = &http.Client{Timeout: 0}

// streamArchiveHTTP opens a single streaming query to ClickHouse via the HTTP
// interface and pipes JSONEachRow output through the archive pipeline. It returns
// the number of rows successfully written and any error encountered.
//
// The HTTP interface streams results row-by-row without buffering the full result
// set, and max_threads=1 ensures ClickHouse decompresses one granule at a time,
// keeping memory usage minimal even for 100GB+ fractals.
func (m *Manager) streamArchiveHTTP(
	ctx context.Context,
	fractalID string,
	retentionDays *int,
	firstQuery bool,
	cursorTS time.Time,
	cursorID string,
	archiveEndTS time.Time,
	w *bufio.Writer,
	logCount *int64,
	outCursorTS *time.Time,
	outCursorID *string,
	timeStart, timeEnd **time.Time,
) (int64, error) {
	query := m.buildArchiveQuery(fractalID, retentionDays, firstQuery, cursorTS, cursorID, archiveEndTS)

	chAddr := m.ch.HTTPAddr()
	maxMem := getArchiveMaxMemory()

	reqURL := fmt.Sprintf("http://%s/?database=%s&max_threads=1&max_execution_time=3600&max_memory_usage=%d&max_block_size=2048",
		chAddr, m.ch.Database, maxMem)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(query))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	if m.ch.User != "" {
		req.Header.Set("X-ClickHouse-User", m.ch.User)
	}
	if m.ch.Password != "" {
		req.Header.Set("X-ClickHouse-Key", m.ch.Password)
	}

	resp, err := archiveHTTPClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("clickhouse HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return 0, fmt.Errorf("clickhouse HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// Stream the response line by line. ClickHouse JSONEachRow format outputs
	// one JSON object per line, which we reformat into the v1 archive schema.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 512*1024), 10*1024*1024) // 10MB max line

	var rowsRead int64
	for scanner.Scan() {
		if ctx.Err() != nil {
			return rowsRead, ctx.Err()
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Parse the ClickHouse JSONEachRow output and reformat to v1 schema.
		archiveLine, ts, logID, parseErr := reformatToV1(line)
		if parseErr != nil {
			log.Printf("[Archives] Warning: skipping malformed row: %v", parseErr)
			continue
		}

		if _, err := w.Write(archiveLine); err != nil {
			return rowsRead, fmt.Errorf("write row: %w", err)
		}
		if err := w.WriteByte('\n'); err != nil {
			return rowsRead, fmt.Errorf("write newline: %w", err)
		}

		*outCursorTS = ts
		*outCursorID = logID
		rowsRead++
		*logCount++

		if *timeStart == nil || ts.Before(**timeStart) {
			*timeStart = &ts
		}
		if *timeEnd == nil || ts.After(**timeEnd) {
			*timeEnd = &ts
		}
	}

	if err := scanner.Err(); err != nil {
		return rowsRead, fmt.Errorf("scan response: %w", err)
	}

	return rowsRead, nil
}

// chRow is the JSON structure returned by ClickHouse JSONEachRow for our SELECT.
type chRow struct {
	Timestamp       string `json:"timestamp"`
	RawLog          string `json:"raw_log"`
	LogID           string `json:"log_id"`
	FractalID       string `json:"fractal_id"`
	IngestTimestamp string `json:"ingest_timestamp"`
}

// reformatToV1 converts a ClickHouse JSONEachRow line into the v1 archive format.
// Returns the formatted JSON bytes, parsed timestamp, and log_id for cursor tracking.
func reformatToV1(line []byte) ([]byte, time.Time, string, error) {
	var row chRow
	if err := json.Unmarshal(line, &row); err != nil {
		return nil, time.Time{}, "", err
	}

	ts, err := time.Parse("2006-01-02 15:04:05.000", row.Timestamp)
	if err != nil {
		return nil, time.Time{}, "", fmt.Errorf("parse timestamp %q: %w", row.Timestamp, err)
	}

	its, err := time.Parse("2006-01-02 15:04:05.000", row.IngestTimestamp)
	if err != nil {
		its = ts // fallback
	}

	v1 := archiveLogRowV1{
		Timestamp:       ts,
		RawLog:          row.RawLog,
		LogID:           row.LogID,
		FractalID:       row.FractalID,
		IngestTimestamp:  its,
	}

	out, err := json.Marshal(v1)
	return out, ts, row.LogID, err
}

// buildArchiveQuery constructs the SQL for streaming archive data.
func (m *Manager) buildArchiveQuery(fractalID string, retentionDays *int, firstQuery bool, cursorTS time.Time, cursorID string, archiveEndTS time.Time) string {
	query := fmt.Sprintf(
		`SELECT timestamp, raw_log, log_id, fractal_id, ingest_timestamp FROM %s WHERE fractal_id = '%s'`,
		m.ch.ReadTable(), escapeCHString(fractalID))

	if !firstQuery {
		query += fmt.Sprintf(` AND (toUnixTimestamp64Milli(timestamp), log_id) > (%d, '%s')`,
			cursorTS.UnixMilli(), escapeCHString(cursorID))
	}

	query += fmt.Sprintf(` AND toUnixTimestamp64Milli(timestamp) <= %d`, archiveEndTS.UnixMilli())

	if retentionDays != nil && *retentionDays > 0 {
		query += fmt.Sprintf(` AND timestamp >= now() - toIntervalDay(%d)`, *retentionDays)
	}

	query += ` ORDER BY fractal_id, timestamp, log_id FORMAT JSONEachRow`
	return query
}

// escapeCHString escapes single quotes for ClickHouse string literals.
func escapeCHString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// isTransientClickHouseError returns true for errors that are likely temporary
// and worth retrying (connection resets, OOM from background merges, etc).
func isTransientClickHouseError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "MEMORY_LIMIT_EXCEEDED") ||
		strings.Contains(msg, "memory limit exceeded") ||
		strings.Contains(msg, "OvercommitTracker") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "EOF")
}

// getArchiveMaxDuration returns the maximum wall-clock time an archive is
// allowed to run. Configurable via BIFRACT_ARCHIVE_MAX_DURATION. Default 24h.
func getArchiveMaxDuration() time.Duration {
	if v := os.Getenv("BIFRACT_ARCHIVE_MAX_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 24 * time.Hour
}

// getArchiveMaxErrorTime returns the maximum cumulative time the archive will
// spend waiting on retries before giving up. Configurable via
// BIFRACT_ARCHIVE_MAX_ERROR_TIME. Default 30m.
func getArchiveMaxErrorTime() time.Duration {
	if v := os.Getenv("BIFRACT_ARCHIVE_MAX_ERROR_TIME"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 30 * time.Minute
}

// getArchiveMaxMemory returns the per-query memory ceiling for archive queries.
// Configurable via BIFRACT_ARCHIVE_MAX_MEMORY (bytes). Default 3GB.
// Safe with max_threads=1 since only one thread processes data at a time.
func getArchiveMaxMemory() uint64 {
	if v := os.Getenv("BIFRACT_ARCHIVE_MAX_MEMORY"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 3_000_000_000
}

// RestoreArchive starts an asynchronous restore operation from an archive.
// The ingestToken is used to POST raw logs to the local ingest endpoint,
// ensuring logs go through the full ingestion pipeline.
func (m *Manager) RestoreArchive(ctx context.Context, archiveID, targetFractalID, ingestToken string, clearExisting bool) error {
	if m.store == nil {
		return fmt.Errorf("archive storage not configured")
	}
	if len(m.key) == 0 {
		return fmt.Errorf("backup encryption key not configured")
	}
	if ingestToken == "" {
		return fmt.Errorf("ingest token is required for restore")
	}

	archive, err := m.archives.GetArchive(ctx, archiveID)
	if err != nil {
		return err
	}

	if archive.Status != StatusCompleted {
		return fmt.Errorf("can only restore completed archives (current status: %s)", archive.Status)
	}

	// Check for active operations on both source and target fractals
	active, err := m.archives.HasActiveOperation(ctx, archive.FractalID)
	if err != nil {
		return fmt.Errorf("check active operation: %w", err)
	}
	if active {
		return fmt.Errorf("an archive operation is already in progress for this fractal")
	}

	if targetFractalID != archive.FractalID {
		active, err = m.archives.HasActiveOperation(ctx, targetFractalID)
		if err != nil {
			return fmt.Errorf("check active operation on target: %w", err)
		}
		if active {
			return fmt.Errorf("an archive operation is already in progress for the target fractal")
		}
	}

	if err := m.archives.SetArchiveStatus(ctx, archiveID, StatusRestoring); err != nil {
		return err
	}

	restoreCtx, cancel := context.WithCancel(context.Background())

	m.mu.Lock()
	m.running[archiveID] = cancel
	m.mu.Unlock()

	m.wg.Add(1)
	go m.runRestore(restoreCtx, archive, targetFractalID, ingestToken, clearExisting)

	return nil
}

func (m *Manager) runRestore(ctx context.Context, archive *Archive, targetFractalID, ingestToken string, clearExisting bool) {
	defer m.wg.Done()
	defer func() {
		m.mu.Lock()
		delete(m.running, archive.ID)
		m.mu.Unlock()
	}()

	log.Printf("[Archives] Starting restore of archive %s into fractal %s", archive.ID, targetFractalID)

	if err := m.executeRestore(ctx, archive, targetFractalID, ingestToken, clearExisting); err != nil {
		log.Printf("[Archives] Restore of archive %s failed: %v", archive.ID, err)
		m.archives.SetArchiveStatus(context.Background(), archive.ID, StatusFailed)
		return
	}

	m.archives.SetArchiveStatus(context.Background(), archive.ID, StatusCompleted)
	log.Printf("[Archives] Restore of archive %s completed", archive.ID)
}

// restoreBatchSize is the number of raw logs sent per HTTP POST to the ingest
// endpoint. Matches the default used by the bifract --ingest CLI.
const restoreBatchSize = 5000

func (m *Manager) executeRestore(ctx context.Context, archive *Archive, targetFractalID, ingestToken string, clearExisting bool) error {
	if clearExisting {
		log.Printf("[Archives] Clearing existing data for fractal %s before restore", targetFractalID)
		if err := m.ch.DeleteLogsByFractalID(ctx, targetFractalID); err != nil {
			return fmt.Errorf("clear existing data: %w", err)
		}
	}

	reader, err := m.store.Read(ctx, archive.StoragePath)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer reader.Close()

	decReader, err := backup.NewDecryptingReader(reader, m.key)
	if err != nil {
		return fmt.Errorf("create decrypting reader: %w", err)
	}

	zstdReader, err := zstd.NewReader(decReader)
	if err != nil {
		return fmt.Errorf("create zstd reader: %w", err)
	}
	defer zstdReader.Close()

	scanner := bufio.NewScanner(zstdReader)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024) // 10MB max line

	// Read and validate the v1 format header.
	if !scanner.Scan() {
		return fmt.Errorf("empty archive file")
	}
	var header archiveHeader
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil || header.Version != 1 {
		return fmt.Errorf("unsupported archive format (expected v1 header)")
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read archive header: %w", err)
	}

	log.Printf("[Archives] Restoring archive %s (format v%d)", archive.ID, header.Version)

	ingestURL := fmt.Sprintf("http://localhost:%s/api/v1/ingest", getBifractPort())

	// Use the same adaptive concurrency pattern as the bifract --ingest CLI:
	// auto-detect workers from CPU cores, adaptive pacer with AIMD that
	// throttles down on 429s and scales back up on sustained success.
	workers := restoreAutoDetectWorkers()
	pacer := newRestorePacer(workers)
	defer pacer.stop()

	log.Printf("[Archives] Restore using %d workers with adaptive pacing", workers)

	type batchJob struct {
		logs   []string
		offset int64
	}
	jobs := make(chan batchJob, workers*2)
	var workerErr error
	var workerErrOnce sync.Once
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				pacer.acquire()
				err, throttled := postIngestBatchWithSignal(ctx, ingestURL, ingestToken, job.logs)
				pacer.release(throttled)
				if err != nil {
					workerErrOnce.Do(func() {
						workerErr = fmt.Errorf("ingest batch at offset %d: %w", job.offset, err)
					})
					return
				}
			}
		}()
	}

	batch := make([]string, 0, restoreBatchSize)
	var totalSent int64

	for scanner.Scan() {
		if ctx.Err() != nil {
			break
		}
		if workerErr != nil {
			break
		}

		rawLog, err := extractRawLog(scanner.Bytes())
		if err != nil {
			log.Printf("[Archives] Warning: skipping malformed line during restore: %v", err)
			continue
		}
		if rawLog == "" {
			continue
		}

		batch = append(batch, rawLog)

		if len(batch) >= restoreBatchSize {
			sending := make([]string, len(batch))
			copy(sending, batch)
			jobs <- batchJob{logs: sending, offset: totalSent}
			totalSent += int64(len(batch))
			batch = batch[:0]

			if totalSent%100000 == 0 {
				log.Printf("[Archives] Restore progress: %d logs sent to ingest", totalSent)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		close(jobs)
		wg.Wait()
		return fmt.Errorf("scan archive: %w", err)
	}

	if len(batch) > 0 && workerErr == nil {
		sending := make([]string, len(batch))
		copy(sending, batch)
		jobs <- batchJob{logs: sending, offset: totalSent}
		totalSent += int64(len(batch))
	}

	close(jobs)
	wg.Wait()

	if workerErr != nil {
		return workerErr
	}

	log.Printf("[Archives] Restore complete: %d logs sent to ingest for fractal %s", totalSent, targetFractalID)
	return nil
}

// extractRawLog pulls the raw_log string from a v1 NDJSON archive line.
func extractRawLog(line []byte) (string, error) {
	var row archiveLogRowV1
	if err := json.Unmarshal(line, &row); err != nil {
		return "", fmt.Errorf("unmarshal archive line: %w", err)
	}
	return row.RawLog, nil
}

// restoreAutoDetectWorkers picks worker count from CPU cores, matching the
// bifract --ingest CLI auto-detection logic.
func restoreAutoDetectWorkers() int {
	cpus := runtime.NumCPU()
	if cpus < 2 {
		return 2
	}
	if cpus > 32 {
		return 32
	}
	return cpus
}

// restorePacer is a minimal adaptive concurrency limiter using AIMD, matching
// the AdaptivePacer from the bifract --ingest CLI. It throttles down on 429s
// and scales back up after sustained success.
type restorePacer struct {
	mu       sync.Mutex
	cond     *sync.Cond
	inflight int
	limit    int
	maxLimit int

	windowSuccesses int64
	windowThrottles int64

	consecutiveStable int
	stopCh            chan struct{}
	stopOnce          sync.Once
}

func newRestorePacer(maxConcurrency int) *restorePacer {
	p := &restorePacer{
		limit:    maxConcurrency,
		maxLimit: maxConcurrency,
		stopCh:   make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)
	go p.tuneLoop()
	return p
}

func (p *restorePacer) acquire() {
	p.mu.Lock()
	for p.inflight >= p.limit {
		p.cond.Wait()
	}
	p.inflight++
	p.mu.Unlock()
}

func (p *restorePacer) release(throttled bool) {
	p.mu.Lock()
	p.inflight--
	if throttled {
		p.windowThrottles++
	} else {
		p.windowSuccesses++
	}
	p.cond.Signal()
	p.mu.Unlock()
}

func (p *restorePacer) stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
}

func (p *restorePacer) tuneLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.evaluate()
		case <-p.stopCh:
			return
		}
	}
}

func (p *restorePacer) evaluate() {
	p.mu.Lock()
	successes := p.windowSuccesses
	throttles := p.windowThrottles
	p.windowSuccesses = 0
	p.windowThrottles = 0
	total := successes + throttles

	if total == 0 {
		p.mu.Unlock()
		return
	}

	throttleRate := float64(throttles) / float64(total)
	oldLimit := p.limit

	if throttleRate > 0.05 {
		// Multiplicative decrease.
		newLimit := int(float64(p.limit) * 0.5)
		if newLimit < 1 {
			newLimit = 1
		}
		p.limit = newLimit
		p.consecutiveStable = 0
		log.Printf("[Archives] Restore pacer: throttle rate %.0f%%, reducing concurrency to %d", throttleRate*100, newLimit)
	} else if throttles == 0 {
		p.consecutiveStable++
		if p.consecutiveStable >= 3 && p.limit < p.maxLimit {
			p.limit++
			p.consecutiveStable = 0
		}
	} else {
		p.consecutiveStable = 0
	}

	changed := p.limit != oldLimit
	p.mu.Unlock()

	if changed {
		p.cond.Broadcast()
	}
}

// postIngestBatchWithSignal is like postIngestBatch but returns a throttle
// signal so the adaptive pacer can adjust concurrency.
func postIngestBatchWithSignal(ctx context.Context, url, token string, batch []string) (error, bool) {
	var buf bytes.Buffer
	for i, line := range batch {
		if i > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(line)
	}
	body := buf.Bytes()

	backoff := 500 * time.Millisecond
	const maxRetries = 5
	var sawThrottle bool

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err(), sawThrottle
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err), false
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := restoreHTTPClient.Do(req)
		if err != nil {
			sawThrottle = true
			if attempt == maxRetries {
				return fmt.Errorf("POST ingest: %w", err), sawThrottle
			}
			select {
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			case <-ctx.Done():
				return ctx.Err(), sawThrottle
			}
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil, sawThrottle
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			sawThrottle = true
			if attempt == maxRetries {
				return fmt.Errorf("ingest returned status %d after %d retries", resp.StatusCode, maxRetries), sawThrottle
			}
			select {
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			case <-ctx.Done():
				return ctx.Err(), sawThrottle
			}
			continue
		}

		return fmt.Errorf("ingest returned status %d", resp.StatusCode), false
	}

	return nil, sawThrottle
}

// restoreHTTPClient is used for restore POST requests. It has a per-request
// timeout to prevent the restore goroutine from blocking indefinitely.
var restoreHTTPClient = &http.Client{Timeout: 60 * time.Second}


// getBifractPort returns the port the Bifract HTTP server is listening on.
func getBifractPort() string {
	if p := os.Getenv("BIFRACT_PORT"); p != "" {
		return p
	}
	return "8080"
}

// GetArchive returns an archive by ID.
func (m *Manager) GetArchive(ctx context.Context, archiveID string) (*Archive, error) {
	return m.archives.GetArchive(ctx, archiveID)
}

// ListArchives returns all archives for a fractal.
func (m *Manager) ListArchives(ctx context.Context, fractalID string) ([]*Archive, error) {
	return m.archives.ListArchives(ctx, fractalID)
}

// DeleteArchive deletes an archive record and its storage file.
func (m *Manager) DeleteArchive(ctx context.Context, archiveID string) error {
	archive, err := m.archives.GetArchive(ctx, archiveID)
	if err != nil {
		return err
	}

	if archive.Status == StatusInProgress || archive.Status == StatusRestoring {
		// Cancel the running operation first
		m.mu.Lock()
		if cancel, ok := m.running[archiveID]; ok {
			cancel()
		}
		m.mu.Unlock()
	}

	// Delete the storage file
	if m.store != nil {
		if err := m.store.Delete(ctx, archive.StoragePath); err != nil {
			log.Printf("[Archives] Warning: failed to delete archive file %s: %v", archive.StoragePath, err)
		}
	}

	return m.archives.DeleteArchive(ctx, archiveID)
}

// EnforceMaxArchives deletes the oldest completed archives that exceed the limit.
func (m *Manager) EnforceMaxArchives(ctx context.Context, fractalID string, maxArchives int) error {
	ids, err := m.archives.GetOldestCompletedArchives(ctx, fractalID, maxArchives)
	if err != nil {
		return err
	}
	for _, id := range ids {
		log.Printf("[Archives] Enforcing max_archives: deleting archive %s for fractal %s", id, fractalID)
		if err := m.DeleteArchive(ctx, id); err != nil {
			log.Printf("[Archives] Failed to delete excess archive %s: %v", id, err)
		}
	}
	return nil
}

// Shutdown cancels all running operations and waits for them to finish.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	for id, cancel := range m.running {
		log.Printf("[Archives] Cancelling archive operation %s", id)
		cancel()
	}
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[Archives] All archive operations stopped")
	case <-time.After(30 * time.Second):
		log.Println("[Archives] Timed out waiting for archive operations to stop")
	}
}
