// Package objstore describes the object-storage backend for the Iceberg archive
// and translates a single BIFRACT_ARCHIVE_* configuration into the concrete
// parameters two different consumers need:
//
//   - the archiver, which hands iceberg-go a warehouse URI + FileIO properties
//     (iceberg-go's gocloud IO performs all Parquet/metadata writes);
//   - the restore path, which hands ClickHouse an iceberg*() table function with
//     the matching storage location and credentials.
//
// It deliberately imports no cloud SDKs so it can be linked into bifract-server
// (status display) without dragging Azure/AWS/gocloud into the hot-path binary.
package objstore

import (
	"fmt"
	"os"
	"strings"
)

// Backend identifies the object-storage target.
type Backend string

const (
	BackendDisk  Backend = "disk"  // local filesystem (file://)
	BackendS3    Backend = "s3"    // AWS S3 or any S3-compatible (virtual-host)
	BackendMinIO Backend = "minio" // S3-compatible with a custom endpoint (path-style)
	BackendAzure Backend = "azure" // Azure Blob / ADLS Gen2
)

// Config is the parsed archive object-storage configuration.
type Config struct {
	Backend Backend

	// Disk
	DiskPath string

	// S3 / MinIO
	S3Endpoint  string // custom endpoint (required for MinIO)
	S3Region    string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string

	// Azure
	AzureAccount   string
	AzureKey       string
	AzureContainer string
	AzureEndpoint  string // optional custom endpoint (e.g. Azurite)

	// Prefix is an optional path prefix within the bucket/container/dir under
	// which all Iceberg namespaces/tables live.
	Prefix string
}

// FromEnv builds a Config from BIFRACT_ARCHIVE_* environment variables. Backend
// defaults to disk. Returns an error only when the selected backend is missing
// required fields.
func FromEnv() (Config, error) {
	c := Config{
		Backend:        Backend(strings.ToLower(getenv("BIFRACT_ARCHIVE_BACKEND", string(BackendDisk)))),
		DiskPath:       getenv("BIFRACT_ARCHIVE_DISK_PATH", "/var/lib/bifract/archive"),
		S3Endpoint:     os.Getenv("BIFRACT_ARCHIVE_S3_ENDPOINT"),
		S3Region:       getenv("BIFRACT_ARCHIVE_S3_REGION", "us-east-1"),
		S3Bucket:       os.Getenv("BIFRACT_ARCHIVE_S3_BUCKET"),
		S3AccessKey:    os.Getenv("BIFRACT_ARCHIVE_S3_ACCESS_KEY"),
		S3SecretKey:    os.Getenv("BIFRACT_ARCHIVE_S3_SECRET_KEY"),
		AzureAccount:   os.Getenv("BIFRACT_ARCHIVE_AZURE_ACCOUNT"),
		AzureKey:       os.Getenv("BIFRACT_ARCHIVE_AZURE_KEY"),
		AzureContainer: os.Getenv("BIFRACT_ARCHIVE_AZURE_CONTAINER"),
		AzureEndpoint:  os.Getenv("BIFRACT_ARCHIVE_AZURE_ENDPOINT"),
		Prefix:         strings.Trim(os.Getenv("BIFRACT_ARCHIVE_PREFIX"), "/"),
	}
	return c, c.Validate()
}

// Validate checks that the selected backend has the fields it needs.
func (c Config) Validate() error {
	switch c.Backend {
	case BackendDisk:
		if c.DiskPath == "" {
			return fmt.Errorf("objstore: disk backend requires BIFRACT_ARCHIVE_DISK_PATH")
		}
	case BackendS3, BackendMinIO:
		if c.S3Bucket == "" {
			return fmt.Errorf("objstore: %s backend requires BIFRACT_ARCHIVE_S3_BUCKET", c.Backend)
		}
		if c.Backend == BackendMinIO && c.S3Endpoint == "" {
			return fmt.Errorf("objstore: minio backend requires BIFRACT_ARCHIVE_S3_ENDPOINT")
		}
	case BackendAzure:
		if c.AzureAccount == "" || c.AzureContainer == "" {
			return fmt.Errorf("objstore: azure backend requires BIFRACT_ARCHIVE_AZURE_ACCOUNT and _CONTAINER")
		}
	default:
		return fmt.Errorf("objstore: unknown backend %q (want disk|s3|minio|azure)", c.Backend)
	}
	return nil
}

// prefixed joins the optional prefix in front of a path segment. Both the
// configured prefix and the segment are normalized (surrounding slashes trimmed)
// so the result never contains empty or doubled path components.
func (c Config) prefixed(p string) string {
	fx := strings.Trim(c.Prefix, "/")
	p = strings.Trim(p, "/")
	switch {
	case fx == "" && p == "":
		return ""
	case fx == "":
		return p
	case p == "":
		return fx
	default:
		return fx + "/" + p
	}
}

// WarehouseURI is the base location iceberg-go uses for the catalog warehouse.
// All per-fractal tables are created beneath it.
func (c Config) WarehouseURI() string {
	switch c.Backend {
	case BackendDisk:
		base := strings.TrimRight(c.DiskPath, "/")
		if p := c.prefixed(""); p != "" {
			base += "/" + p
		}
		return "file://" + base
	case BackendS3, BackendMinIO:
		return joinURI("s3://"+c.S3Bucket, c.prefixed(""))
	case BackendAzure:
		// iceberg-go's ADLS parser requires <container>@<account>.<host>, with the
		// container in the URI's userinfo position (it reads uri.User.Username()).
		return joinURI("abfs://"+c.AzureContainer+"@"+c.AzureAccount+".dfs.core.windows.net", c.prefixed(""))
	}
	return ""
}

// IcebergProps returns the FileIO properties for iceberg-go's catalog/IO layer.
// Keys match iceberg-go's io package (s3.*, adls.*). Empty for disk.
func (c Config) IcebergProps() map[string]string {
	props := map[string]string{}
	switch c.Backend {
	case BackendS3, BackendMinIO:
		if c.S3Endpoint != "" {
			props["s3.endpoint"] = c.S3Endpoint
		}
		props["s3.region"] = c.S3Region
		if c.S3AccessKey != "" {
			props["s3.access-key-id"] = c.S3AccessKey
		}
		if c.S3SecretKey != "" {
			props["s3.secret-access-key"] = c.S3SecretKey
		}
		// MinIO (and most custom endpoints) require path-style addressing.
		// force-virtual-addressing=false selects path-style.
		if c.Backend == BackendMinIO || c.S3Endpoint != "" {
			props["s3.force-virtual-addressing"] = "false"
		}
	case BackendAzure:
		props["adls.auth.shared-key.account.name"] = c.AzureAccount
		if c.AzureKey != "" {
			props["adls.auth.shared-key.account.key"] = c.AzureKey
		}
		if c.AzureEndpoint != "" {
			props["adls.endpoint"] = c.AzureEndpoint
		}
	}
	return props
}

// RequiresCustomEndpoint reports whether reading this store means dialing an
// endpoint other than the cloud provider's public one. It is a pure objstore
// fact; who cares about it, and what they do, is decided by the caller.
//
// It matters because Bifract writes archives itself, through the Go SDK from its
// own pods, but restore and recall are read BY ClickHouse. A ClickHouse the
// operator does not run may have no route to a self-hosted endpoint at all.
func (c Config) RequiresCustomEndpoint() bool {
	return c.Backend == BackendMinIO || (c.Backend == BackendS3 && c.S3Endpoint != "")
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func joinURI(base, sub string) string {
	if sub == "" {
		return base
	}
	return strings.TrimRight(base, "/") + "/" + sub
}
