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
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

// archiveChunkSize is the number of rows fetched per cursor query.
// Kept small to limit ClickHouse memory usage per query. Large raw_log
// values can make even modest row counts exceed memory limits.
const archiveChunkSize = 2000

func (m *Manager) executeArchive(ctx context.Context, archive *Archive, retentionDays *int, storagePath string) error {
	archiveEndTS := archive.ArchiveEndTS
	if archiveEndTS == nil {
		now := time.Now()
		archiveEndTS = &now
	}

	// Set up streaming pipeline: rows -> JSON -> zstd -> encrypt -> storage
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

	encoder := json.NewEncoder(zstdWriter)

	// Write v1 format header
	if err := encoder.Encode(archiveHeader{Version: 1}); err != nil {
		zstdWriter.Close()
		encWriter.Close()
		pw.CloseWithError(err)
		<-writeDone
		return fmt.Errorf("write archive header: %w", err)
	}

	var logCount int64
	var timeStart, timeEnd *time.Time

	// Cursor state for chunked iteration. The logs table is ordered by
	// (fractal_id, timestamp, log_id), so tuple comparison is index-friendly.
	var cursorTS time.Time
	var cursorID string
	firstChunk := true

	for {
		if ctx.Err() != nil {
			break
		}

		chunkRows, err := m.queryArchiveChunk(ctx, archive.FractalID, retentionDays, firstChunk, cursorTS, cursorID, *archiveEndTS)
		if err != nil {
			zstdWriter.Close()
			encWriter.Close()
			pw.CloseWithError(err)
			<-writeDone
			return fmt.Errorf("query chunk: %w", err)
		}

		chunkCount := 0
		var scanErr error
		for chunkRows.Next() {
			if ctx.Err() != nil {
				break
			}

			var row archiveLogRowV1
			if err := chunkRows.Scan(
				&row.Timestamp, &row.RawLog, &row.LogID,
				&row.FractalID, &row.IngestTimestamp,
			); err != nil {
				scanErr = fmt.Errorf("scan row: %w", err)
				break
			}

			if err := encoder.Encode(row); err != nil {
				scanErr = fmt.Errorf("encode row: %w", err)
				break
			}

			cursorTS = row.Timestamp
			cursorID = row.LogID
			chunkCount++
			logCount++

			if timeStart == nil || row.Timestamp.Before(*timeStart) {
				t := row.Timestamp
				timeStart = &t
			}
			if timeEnd == nil || row.Timestamp.After(*timeEnd) {
				t := row.Timestamp
				timeEnd = &t
			}
		}

		if scanErr == nil {
			scanErr = chunkRows.Err()
		}
		chunkRows.Close()

		if scanErr != nil {
			zstdWriter.Close()
			encWriter.Close()
			pw.CloseWithError(scanErr)
			<-writeDone
			return fmt.Errorf("iterate rows: %w", scanErr)
		}

		if chunkCount == 0 {
			break
		}

		firstChunk = false
		m.archives.UpdateArchiveCursor(ctx, archive.ID, cursorTS, cursorID, logCount)
		if logCount%1000000 == 0 {
			log.Printf("[Archives] Archive %s progress: %d logs archived", archive.ID, logCount)
		}
	}

	// Close pipeline in order: zstd -> encrypt -> pipe
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

// queryArchiveChunk fetches the next batch of rows using cursor-based pagination.
// The query deliberately excludes the fields column to avoid forcing ClickHouse
// to reconstruct JSON objects in memory, which was the root cause of ~20GB memory
// spikes on large fractals.
func (m *Manager) queryArchiveChunk(ctx context.Context, fractalID string, retentionDays *int, firstChunk bool, cursorTS time.Time, cursorID string, archiveEndTS time.Time) (driver.Rows, error) {
	// Use toUnixTimestamp64Milli for cursor comparisons to avoid a precision
	// mismatch: the clickhouse-go driver may serialize time.Time as DateTime
	// (second precision) while the column is DateTime64(3) (milliseconds).
	// Comparing epoch-millis integers sidesteps the issue entirely.
	query := fmt.Sprintf(`SELECT timestamp, raw_log, log_id, fractal_id, ingest_timestamp
	          FROM %s WHERE fractal_id = ?`, m.ch.ReadTable())
	args := []interface{}{fractalID}

	if !firstChunk {
		query += ` AND (toUnixTimestamp64Milli(timestamp), log_id) > (?, ?)`
		args = append(args, cursorTS.UnixMilli(), cursorID)
	}

	// Pin upper time bound so newly arriving logs are excluded.
	query += ` AND toUnixTimestamp64Milli(timestamp) <= ?`
	args = append(args, archiveEndTS.UnixMilli())

	if retentionDays != nil && *retentionDays > 0 {
		query += ` AND timestamp >= now() - toIntervalDay(?)`
		args = append(args, *retentionDays)
	}

	query += ` ORDER BY fractal_id, timestamp, log_id LIMIT ?`
	args = append(args, archiveChunkSize)

	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_execution_time": 300,
		"max_memory_usage":   getArchiveMaxMemory(),
	}))

	return m.ch.QueryRows(ctx, query, args...)
}

// getArchiveMaxMemory returns the per-query memory ceiling for archive chunk
// queries. Configurable via BIFRACT_ARCHIVE_MAX_MEMORY (bytes). Default 2GB.
func getArchiveMaxMemory() uint64 {
	if v := os.Getenv("BIFRACT_ARCHIVE_MAX_MEMORY"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 1_000_000_000
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

// restoreBatchSize is the number of raw logs sent per HTTP POST to the ingest endpoint.
const restoreBatchSize = 500

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

	batch := make([]string, 0, restoreBatchSize)
	var totalSent int64

	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
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
			if err := postIngestBatch(ctx, ingestURL, ingestToken, batch); err != nil {
				return fmt.Errorf("ingest batch at offset %d: %w", totalSent, err)
			}
			totalSent += int64(len(batch))
			batch = batch[:0]

			if totalSent%50000 == 0 {
				log.Printf("[Archives] Restore progress: %d logs sent to ingest", totalSent)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan archive: %w", err)
	}

	// Send remaining batch
	if len(batch) > 0 {
		if err := postIngestBatch(ctx, ingestURL, ingestToken, batch); err != nil {
			return fmt.Errorf("ingest final batch: %w", err)
		}
		totalSent += int64(len(batch))
	}

	log.Printf("[Archives] Restore complete: %d logs sent to ingest for fractal %s", totalSent, targetFractalID)
	return nil
}

// extractRawLog pulls the raw_log string from a v1 NDJSON archive line.
func extractRawLog(line []byte) (string, error) {
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(line, &generic); err != nil {
		return "", fmt.Errorf("unmarshal archive line: %w", err)
	}

	rawField, ok := generic["raw"]
	if !ok {
		return "", nil
	}

	var rawLog string
	if err := json.Unmarshal(rawField, &rawLog); err != nil {
		return "", fmt.Errorf("unmarshal raw field: %w", err)
	}
	return rawLog, nil
}

// restoreHTTPClient is used for restore POST requests. It has a per-request
// timeout to prevent the restore goroutine from blocking indefinitely.
var restoreHTTPClient = &http.Client{Timeout: 60 * time.Second}

// postIngestBatch sends a batch of raw log lines to the local ingest endpoint
// as newline-separated text. This works with all parser types (JSON, KV, syslog)
// because the ingest endpoint dispatches based on the token's parser_type:
// JSON tokens parse each line as a JSON object (NDJSON), KV tokens parse each
// line as key=value text, and syslog tokens parse each line as a syslog message.
// Retries with exponential backoff on 429 responses.
func postIngestBatch(ctx context.Context, url, token string, batch []string) error {
	var buf bytes.Buffer
	for i, line := range batch {
		if i > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(line)
	}
	body := buf.Bytes()

	backoff := 500 * time.Millisecond
	const maxRetries = 10

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := restoreHTTPClient.Do(req)
		if err != nil {
			return fmt.Errorf("POST ingest: %w", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt == maxRetries {
				return fmt.Errorf("ingest returned 429 after %d retries", maxRetries)
			}
			select {
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		return fmt.Errorf("ingest returned status %d", resp.StatusCode)
	}

	return nil
}

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
