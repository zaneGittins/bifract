package archive

import (
	"os"

	"bifract/pkg/objstore"
)

// ApplyBackendEnv exports the standard cloud-SDK environment variables derived
// from the archive object-storage config.
//
// iceberg-go builds a table's WRITE FileIO (used during Append/Commit) from the
// table's own metadata properties, which do NOT carry connection settings. Those
// code paths fall back to the standard AWS/Azure environment. Exporting the env
// here makes both the catalog IO and the transaction IO resolve the same
// endpoint/region/credentials, so operators only need to set BIFRACT_ARCHIVE_*.
// Notably, AWS_S3_ENDPOINT is what lets the transaction path reach MinIO with
// path-style addressing.
func ApplyBackendEnv(obj objstore.Config) {
	switch obj.Backend {
	case objstore.BackendS3, objstore.BackendMinIO:
		setenvIfEmpty("AWS_REGION", obj.S3Region)
		setenvIfEmpty("AWS_DEFAULT_REGION", obj.S3Region)
		if obj.S3AccessKey != "" {
			setenvIfEmpty("AWS_ACCESS_KEY_ID", obj.S3AccessKey)
			setenvIfEmpty("AWS_SECRET_ACCESS_KEY", obj.S3SecretKey)
		}
		if obj.S3Endpoint != "" {
			// The var iceberg-go's gocloud S3 reads as an endpoint fallback.
			setenvIfEmpty("AWS_S3_ENDPOINT", obj.S3Endpoint)
			// Avoid slow IMDS probes when using a static-credential custom endpoint.
			setenvIfEmpty("AWS_EC2_METADATA_DISABLED", "true")
		}
	case objstore.BackendAzure:
		setenvIfEmpty("AZURE_STORAGE_ACCOUNT", obj.AzureAccount)
		if obj.AzureKey != "" {
			setenvIfEmpty("AZURE_STORAGE_KEY", obj.AzureKey)
		}
	}
}

func setenvIfEmpty(k, v string) {
	if v == "" {
		return
	}
	if _, ok := os.LookupEnv(k); !ok {
		os.Setenv(k, v)
	}
}
