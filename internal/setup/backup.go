package setup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"

	"bifract/pkg/backup"
)

// backupManifest stores metadata about a PostgreSQL backup.
type backupManifest struct {
	Version   string            `json:"version"`
	Timestamp string            `json:"timestamp"`
	Type      string            `json:"type"`
	EnvVars   map[string]string `json:"env_vars"`
}

// RunBackup creates an encrypted PostgreSQL backup.
func RunBackup(dir string, nonInteractive bool) error {
	PrintBanner()
	fmt.Println(TitleStyle.Render("  PostgreSQL Backup"))
	fmt.Println()

	envPath := filepath.Join(dir, ".env")
	existingEnv, err := ReadEnvFile(envPath)
	if err != nil {
		return fmt.Errorf("read .env: %w", err)
	}

	// Load encryption key
	encKeyHex := existingEnv["BIFRACT_BACKUP_ENCRYPTION_KEY"]
	if encKeyHex == "" {
		return fmt.Errorf("BIFRACT_BACKUP_ENCRYPTION_KEY not found in .env\n  Run bifract --upgrade to generate one")
	}

	// Set env var so backup.LoadBackupKey works
	os.Setenv("BIFRACT_BACKUP_ENCRYPTION_KEY", encKeyHex)
	key, err := backup.LoadBackupKey()
	if err != nil {
		return fmt.Errorf("load backup key: %w", err)
	}

	docker := &DockerOps{Dir: dir}
	if !docker.IsRunning() {
		return fmt.Errorf("containers are not running. Start them with: docker compose up -d")
	}

	// Run pg_dump
	printStep("Running pg_dump...")
	pgUser := existingEnv["POSTGRES_USER"]
	if pgUser == "" {
		pgUser = "bifract"
	}
	pgDB := existingEnv["POSTGRES_DB"]
	if pgDB == "" {
		pgDB = "bifract"
	}

	dumpData, err := docker.ExecPostgresDump(pgUser, pgDB)
	if err != nil {
		return fmt.Errorf("pg_dump failed: %w", err)
	}
	printDone(fmt.Sprintf("Database dumped (%d bytes)", len(dumpData)))

	// Build manifest
	manifest := backupManifest{
		Version:   Version,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Type:      "postgres",
		EnvVars:   existingEnv,
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	// Combine manifest + dump into a single payload: manifest\n---\ndump
	var payload bytes.Buffer
	payload.Write(manifestJSON)
	payload.WriteString("\n---BIFRACT_BACKUP_SEPARATOR---\n")
	payload.Write(dumpData)

	// Compress
	printStep("Compressing...")
	var compressed bytes.Buffer
	zw, err := zstd.NewWriter(&compressed, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return fmt.Errorf("create zstd writer: %w", err)
	}
	if _, err := zw.Write(payload.Bytes()); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("close compressor: %w", err)
	}
	printDone(fmt.Sprintf("Compressed to %d bytes", compressed.Len()))

	// Encrypt
	printStep("Encrypting...")
	var encrypted bytes.Buffer
	ew, err := backup.NewEncryptingWriter(&encrypted, key)
	if err != nil {
		return fmt.Errorf("create encrypting writer: %w", err)
	}
	if _, err := ew.Write(compressed.Bytes()); err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}
	if err := ew.Close(); err != nil {
		return fmt.Errorf("close encryptor: %w", err)
	}
	printDone("Encrypted")

	// Determine storage
	storageCfg := backup.StorageConfig{
		DiskBasePath: filepath.Join(dir, "backups"),
		S3Endpoint:   existingEnv["BIFRACT_S3_ENDPOINT"],
		S3Bucket:     existingEnv["BIFRACT_S3_BUCKET"],
		S3AccessKey:  existingEnv["BIFRACT_S3_ACCESS_KEY"],
		S3SecretKey:  existingEnv["BIFRACT_S3_SECRET_KEY"],
		S3Region:     existingEnv["BIFRACT_S3_REGION"],
	}

	store, err := backup.NewStorageBackend(storageCfg)
	if err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}

	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("bifract-backup-%s.bifract-backup", timestamp)
	storagePath := filename

	printStep("Saving backup...")
	written, err := store.Write(context.Background(), storagePath, bytes.NewReader(encrypted.Bytes()))
	if err != nil {
		return fmt.Errorf("write backup: %w", err)
	}
	printDone(fmt.Sprintf("Backup saved: %s (%d bytes)", filename, written))

	fmt.Println()
	fmt.Println(SuccessStyle.Render("Backup complete"))
	fmt.Println()
	return nil
}

// RunRestore restores a PostgreSQL backup.
func RunRestore(dir string, backupFile string, nonInteractive bool) error {
	PrintBanner()
	fmt.Println(TitleStyle.Render("  PostgreSQL Restore"))
	fmt.Println()

	envPath := filepath.Join(dir, ".env")
	existingEnv, err := ReadEnvFile(envPath)
	if err != nil {
		return fmt.Errorf("read .env: %w", err)
	}

	encKeyHex := existingEnv["BIFRACT_BACKUP_ENCRYPTION_KEY"]
	if encKeyHex == "" {
		return fmt.Errorf("BIFRACT_BACKUP_ENCRYPTION_KEY not found in .env")
	}
	os.Setenv("BIFRACT_BACKUP_ENCRYPTION_KEY", encKeyHex)
	key, err := backup.LoadBackupKey()
	if err != nil {
		return fmt.Errorf("load backup key: %w", err)
	}

	docker := &DockerOps{Dir: dir}
	if !docker.IsRunning() {
		return fmt.Errorf("containers are not running. Start them with: docker compose up -d")
	}

	// Read backup file
	printStep("Reading backup file...")
	storageCfg := backup.StorageConfig{
		DiskBasePath: filepath.Join(dir, "backups"),
		S3Endpoint:   existingEnv["BIFRACT_S3_ENDPOINT"],
		S3Bucket:     existingEnv["BIFRACT_S3_BUCKET"],
		S3AccessKey:  existingEnv["BIFRACT_S3_ACCESS_KEY"],
		S3SecretKey:  existingEnv["BIFRACT_S3_SECRET_KEY"],
		S3Region:     existingEnv["BIFRACT_S3_REGION"],
	}

	store, err := backup.NewStorageBackend(storageCfg)
	if err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}

	reader, err := store.Read(context.Background(), backupFile)
	if err != nil {
		return fmt.Errorf("read backup: %w", err)
	}
	defer reader.Close()

	var encryptedBuf bytes.Buffer
	if _, err := encryptedBuf.ReadFrom(reader); err != nil {
		return fmt.Errorf("buffer backup: %w", err)
	}
	printDone(fmt.Sprintf("Read %d bytes", encryptedBuf.Len()))

	// Decrypt
	printStep("Decrypting...")
	decReader, err := backup.NewDecryptingReader(&encryptedBuf, key)
	if err != nil {
		return fmt.Errorf("create decrypting reader: %w", err)
	}
	var compressed bytes.Buffer
	if _, err := compressed.ReadFrom(decReader); err != nil {
		return fmt.Errorf("decrypt: %w", err)
	}
	printDone("Decrypted")

	// Decompress
	printStep("Decompressing...")
	zr, err := zstd.NewReader(&compressed)
	if err != nil {
		return fmt.Errorf("create zstd reader: %w", err)
	}
	var payload bytes.Buffer
	if _, err := payload.ReadFrom(zr); err != nil {
		return fmt.Errorf("decompress: %w", err)
	}
	zr.Close()
	printDone("Decompressed")

	// Split manifest and dump
	separator := "\n---BIFRACT_BACKUP_SEPARATOR---\n"
	parts := bytes.SplitN(payload.Bytes(), []byte(separator), 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid backup format: missing separator")
	}

	var manifest backupManifest
	if err := json.Unmarshal(parts[0], &manifest); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}

	fmt.Printf("  Backup version: %s\n", manifest.Version)
	fmt.Printf("  Backup date: %s\n", manifest.Timestamp)

	if !nonInteractive {
		fmt.Println()
		fmt.Println(WarningStyle.Render("WARNING: This will replace the current PostgreSQL database."))
		fmt.Print("  Continue? (y/N): ")
		var confirm string
		fmt.Scanln(&confirm)
		if confirm != "y" && confirm != "Y" {
			fmt.Println("  Restore cancelled")
			return nil
		}
	}

	// Restore pg_dump
	printStep("Restoring database...")
	pgUser := existingEnv["POSTGRES_USER"]
	if pgUser == "" {
		pgUser = "bifract"
	}
	pgDB := existingEnv["POSTGRES_DB"]
	if pgDB == "" {
		pgDB = "bifract"
	}

	output, err := docker.ExecPostgresRestore(pgUser, pgDB, bytes.NewReader(parts[1]))
	if err != nil {
		// pg_restore often returns non-zero for harmless warnings (e.g. "relation already exists")
		printWarn(fmt.Sprintf("pg_restore completed with warnings: %s", output))
	} else {
		printDone("Database restored")
	}

	fmt.Println()
	fmt.Println(SuccessStyle.Render("Restore complete"))
	fmt.Println()
	return nil
}

// RunListBackups lists available backups.
func RunListBackups(dir string) error {
	PrintBanner()
	fmt.Println(TitleStyle.Render("  Available Backups"))
	fmt.Println()

	envPath := filepath.Join(dir, ".env")
	existingEnv, err := ReadEnvFile(envPath)
	if err != nil {
		// If no .env, just check disk
		existingEnv = make(map[string]string)
	}

	storageCfg := backup.StorageConfig{
		DiskBasePath: filepath.Join(dir, "backups"),
		S3Endpoint:   existingEnv["BIFRACT_S3_ENDPOINT"],
		S3Bucket:     existingEnv["BIFRACT_S3_BUCKET"],
		S3AccessKey:  existingEnv["BIFRACT_S3_ACCESS_KEY"],
		S3SecretKey:  existingEnv["BIFRACT_S3_SECRET_KEY"],
		S3Region:     existingEnv["BIFRACT_S3_REGION"],
	}

	store, err := backup.NewStorageBackend(storageCfg)
	if err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}

	files, err := store.List(context.Background(), "")
	if err != nil {
		return fmt.Errorf("list backups: %w", err)
	}

	if len(files) == 0 {
		fmt.Println("  No backups found")
		return nil
	}

	for _, f := range files {
		sizeMB := float64(f.Size) / (1024 * 1024)
		fmt.Printf("  %s  %.1f MB  %s\n",
			ValueStyle.Render(f.Path),
			sizeMB,
			DimStyle.Render(f.Modified.Format("2006-01-02 15:04:05")),
		)
	}

	fmt.Println()
	return nil
}
