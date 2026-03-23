package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// StoredFile represents a file in the storage backend.
type StoredFile struct {
	Path     string
	Size     int64
	Modified time.Time
}

// StorageBackend abstracts file storage for backups and archives.
type StorageBackend interface {
	Write(ctx context.Context, path string, r io.Reader) (int64, error)
	Read(ctx context.Context, path string) (io.ReadCloser, error)
	Delete(ctx context.Context, path string) error
	List(ctx context.Context, prefix string) ([]StoredFile, error)
}

// StorageConfig holds configuration for storage backends.
type StorageConfig struct {
	// DiskBasePath is used for disk storage.
	DiskBasePath string

	// S3 configuration
	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
}

// NewStorageBackend returns a storage backend based on configuration.
// Returns S3 if S3Bucket is set, otherwise Disk.
func NewStorageBackend(cfg StorageConfig) (StorageBackend, error) {
	if cfg.S3Bucket != "" {
		return NewS3Storage(cfg)
	}
	if cfg.DiskBasePath == "" {
		return nil, fmt.Errorf("disk base path is required when S3 is not configured")
	}
	return NewDiskStorage(cfg.DiskBasePath)
}

// StorageConfigFromEnv builds a StorageConfig from environment variables.
func StorageConfigFromEnv(diskFallback string) StorageConfig {
	return StorageConfig{
		DiskBasePath: diskFallback,
		S3Endpoint:   os.Getenv("BIFRACT_S3_ENDPOINT"),
		S3Bucket:     os.Getenv("BIFRACT_S3_BUCKET"),
		S3AccessKey:  os.Getenv("BIFRACT_S3_ACCESS_KEY"),
		S3SecretKey:  os.Getenv("BIFRACT_S3_SECRET_KEY"),
		S3Region:     os.Getenv("BIFRACT_S3_REGION"),
	}
}

// DiskStorage implements StorageBackend using the local filesystem.
type DiskStorage struct {
	basePath string
}

// NewDiskStorage creates a disk-based storage backend rooted at basePath.
func NewDiskStorage(basePath string) (*DiskStorage, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("create base path %s: %w", basePath, err)
	}
	return &DiskStorage{basePath: basePath}, nil
}

func (d *DiskStorage) resolvePath(path string) (string, error) {
	// Prevent path traversal by resolving and checking the result is under basePath.
	clean := filepath.Clean(path)
	full := filepath.Join(d.basePath, clean)
	full = filepath.Clean(full)
	if !strings.HasPrefix(full, filepath.Clean(d.basePath)+string(filepath.Separator)) && full != filepath.Clean(d.basePath) {
		return "", fmt.Errorf("path traversal detected: %s", path)
	}
	return full, nil
}

func (d *DiskStorage) Write(_ context.Context, path string, r io.Reader) (int64, error) {
	fullPath, err := d.resolvePath(path)
	if err != nil {
		return 0, err
	}

	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return 0, fmt.Errorf("create directory: %w", err)
	}

	// Write to temp file first, then rename for atomicity
	tmpFile, err := os.CreateTemp(filepath.Dir(fullPath), ".bifract-tmp-*")
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	n, err := io.Copy(tmpFile, r)
	if closeErr := tmpFile.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("write file: %w", err)
	}

	if err := os.Rename(tmpPath, fullPath); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("rename temp file: %w", err)
	}

	return n, nil
}

func (d *DiskStorage) Read(_ context.Context, path string) (io.ReadCloser, error) {
	fullPath, err := d.resolvePath(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	return f, nil
}

func (d *DiskStorage) Delete(_ context.Context, path string) error {
	fullPath, err := d.resolvePath(path)
	if err != nil {
		return err
	}
	if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete file: %w", err)
	}
	return nil
}

func (d *DiskStorage) List(_ context.Context, prefix string) ([]StoredFile, error) {
	searchDir, err := d.resolvePath(prefix)
	if err != nil {
		return nil, err
	}

	// If prefix is a file pattern, list the directory
	info, err := os.Stat(searchDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if !info.IsDir() {
		searchDir = filepath.Dir(searchDir)
	}

	entries, err := os.ReadDir(searchDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list directory: %w", err)
	}

	var files []StoredFile
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}

		relPath := entry.Name()
		if prefix != "" && prefix != "." {
			relPath = filepath.Join(prefix, entry.Name())
		}

		files = append(files, StoredFile{
			Path:     relPath,
			Size:     info.Size(),
			Modified: info.ModTime(),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Modified.After(files[j].Modified)
	})

	return files, nil
}
