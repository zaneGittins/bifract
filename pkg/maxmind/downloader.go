package maxmind

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	downloadURL = "https://download.maxmind.com/geoip/databases"
)

// cacheMetadata stores HTTP conditional-request headers alongside cached ZIPs.
type cacheMetadata struct {
	ETag         string    `json:"etag,omitempty"`
	LastModified string    `json:"last_modified,omitempty"`
	DownloadedAt time.Time `json:"downloaded_at"`
}

// Download fetches a MaxMind GeoLite2 edition ZIP archive, extracts CSV files,
// and returns the path to the directory containing them.
// Uses the persistent DataDir to cache downloads and avoids re-downloading
// when the remote file has not changed (HTTP 304).
func Download(cfg *Config, editionID string) (string, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return "", fmt.Errorf("create data dir %s: %w", cfg.DataDir, err)
	}

	zipPath := filepath.Join(cfg.DataDir, editionID+".zip")
	metaPath := filepath.Join(cfg.DataDir, editionID+".meta.json")
	editionDir := filepath.Join(cfg.DataDir, editionID)

	// Load existing cache metadata for conditional request
	meta := loadMetadata(metaPath)

	url := fmt.Sprintf("%s/%s/download?suffix=zip", downloadURL, editionID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request for %s: %w", editionID, err)
	}
	req.SetBasicAuth(cfg.AccountID, cfg.LicenseKey)

	// Add conditional headers if we have cached data
	if meta != nil {
		if meta.ETag != "" {
			req.Header.Set("If-None-Match", meta.ETag)
		}
		if meta.LastModified != "" {
			req.Header.Set("If-Modified-Since", meta.LastModified)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// If download fails but we have cached data, use it
		if fileExists(zipPath) {
			log.Printf("[MaxMind] Download failed for %s, using cached data: %v", editionID, err)
			return extractCachedZip(zipPath, editionDir)
		}
		return "", fmt.Errorf("download %s: %w", editionID, err)
	}
	defer resp.Body.Close()

	// 304 Not Modified: remote data unchanged, use cached ZIP
	if resp.StatusCode == http.StatusNotModified {
		log.Printf("[MaxMind] %s: not modified since last download, using cache", editionID)
		return extractCachedZip(zipPath, editionDir)
	}

	if resp.StatusCode != http.StatusOK {
		// Non-OK response but we have cache: fall back
		if fileExists(zipPath) {
			log.Printf("[MaxMind] Download %s returned HTTP %d, using cached data", editionID, resp.StatusCode)
			return extractCachedZip(zipPath, editionDir)
		}
		return "", fmt.Errorf("download %s: HTTP %d", editionID, resp.StatusCode)
	}

	// Write new ZIP to a temp file in the same directory (atomic rename)
	tmpFile, err := os.CreateTemp(cfg.DataDir, "maxmind-*.zip.tmp")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath) // clean up on error path

	// Limit download size (500MB)
	if _, err := io.Copy(tmpFile, io.LimitReader(resp.Body, 500<<20)); err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("save %s: %w", editionID, err)
	}
	tmpFile.Close()

	// Atomically replace the cached ZIP
	if err := os.Rename(tmpPath, zipPath); err != nil {
		return "", fmt.Errorf("persist cached zip: %w", err)
	}

	// Save cache metadata
	newMeta := &cacheMetadata{
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
		DownloadedAt: time.Now().UTC(),
	}
	saveMetadata(metaPath, newMeta)

	// Extract CSVs
	os.RemoveAll(editionDir)
	if err := os.MkdirAll(editionDir, 0755); err != nil {
		return "", fmt.Errorf("create extract dir: %w", err)
	}
	if err := extractZipCSVs(zipPath, editionDir); err != nil {
		return "", fmt.Errorf("extract %s: %w", editionID, err)
	}

	return editionDir, nil
}

// extractCachedZip extracts CSVs from an existing cached ZIP file.
func extractCachedZip(zipPath, editionDir string) (string, error) {
	os.RemoveAll(editionDir)
	if err := os.MkdirAll(editionDir, 0755); err != nil {
		return "", fmt.Errorf("create extract dir: %w", err)
	}
	if err := extractZipCSVs(zipPath, editionDir); err != nil {
		return "", fmt.Errorf("extract cached zip: %w", err)
	}
	return editionDir, nil
}

// extractZipCSVs opens a ZIP file and extracts only CSV files into destDir.
func extractZipCSVs(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if !strings.HasSuffix(f.Name, ".csv") {
			continue
		}

		// Sanitize: use only the base filename to prevent path traversal
		baseName := filepath.Base(f.Name)
		if baseName == "." || baseName == ".." || strings.Contains(baseName, "..") {
			continue
		}

		outPath := filepath.Join(destDir, baseName)
		outFile, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("create %s: %w", baseName, err)
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return fmt.Errorf("open %s in zip: %w", baseName, err)
		}

		// Limit extraction size (2GB per file)
		if _, err := io.Copy(outFile, io.LimitReader(rc, 2<<30)); err != nil {
			rc.Close()
			outFile.Close()
			return fmt.Errorf("write %s: %w", baseName, err)
		}
		rc.Close()
		outFile.Close()
	}

	return nil
}

func loadMetadata(path string) *cacheMetadata {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var meta cacheMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil
	}
	return &meta
}

func saveMetadata(path string, meta *cacheMetadata) {
	data, err := json.Marshal(meta)
	if err != nil {
		return
	}
	os.WriteFile(path, data, 0644)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
