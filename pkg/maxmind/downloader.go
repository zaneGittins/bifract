package maxmind

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	downloadURL = "https://download.maxmind.com/geoip/databases"
	extractBase = "/tmp/maxmind"
)

// Download fetches a MaxMind GeoLite2 edition ZIP archive, extracts CSV files,
// and returns the path to the directory containing them.
// Uses the current MaxMind API with basic auth (account_id:license_key).
func Download(cfg *Config, editionID string) (string, error) {
	url := fmt.Sprintf("%s/%s/download?suffix=zip", downloadURL, editionID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request for %s: %w", editionID, err)
	}
	req.SetBasicAuth(cfg.AccountID, cfg.LicenseKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", editionID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download %s: HTTP %d", editionID, resp.StatusCode)
	}

	// Write response to a temp file (zip requires random access)
	tmpFile, err := os.CreateTemp("", "maxmind-*.zip")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	// Limit download size (500MB)
	if _, err := io.Copy(tmpFile, io.LimitReader(resp.Body, 500<<20)); err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("save %s: %w", editionID, err)
	}
	tmpFile.Close()

	// Clean previous extraction for this edition
	editionDir := filepath.Join(extractBase, editionID)
	os.RemoveAll(editionDir)
	if err := os.MkdirAll(editionDir, 0755); err != nil {
		return "", fmt.Errorf("create extract dir: %w", err)
	}

	if err := extractZipCSVs(tmpPath, editionDir); err != nil {
		return "", fmt.Errorf("extract %s: %w", editionID, err)
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
