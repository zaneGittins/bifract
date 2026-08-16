package archive

import (
	"strings"
	"testing"

	"bifract/pkg/objstore"
)

// TestChIcebergTableFunc pins the generated iceberg*() SQL for every backend in
// both single-node and cluster mode. These expressions are only exercised against
// a real cluster, so a regression here (wrong argument order, missing Cluster
// suffix) would otherwise surface as a runtime failure during a restore or recall
// rather than at build time. Argument order follows the ClickHouse docs: the
// cluster name is always first, remaining args identical to the single-node form.
func TestChIcebergTableFunc(t *testing.T) {
	s3 := objstore.Config{
		Backend: objstore.BackendS3, S3Region: "us-east-1",
		S3AccessKey: "AK", S3SecretKey: "SK",
	}
	s3Anon := objstore.Config{Backend: objstore.BackendS3, S3Region: "us-east-1"}
	minio := objstore.Config{
		Backend: objstore.BackendMinIO, S3Endpoint: "http://minio:9000/",
		S3AccessKey: "AK", S3SecretKey: "SK",
	}
	azure := objstore.Config{
		Backend: objstore.BackendAzure, AzureAccount: "acct", AzureKey: "KEY",
	}
	disk := objstore.Config{Backend: objstore.BackendDisk}

	tests := []struct {
		name    string
		obj     objstore.Config
		loc     string
		cluster string
		want    string
	}{
		{
			name: "s3 single node",
			obj:  s3, loc: "s3://bkt/warehouse/f_1",
			want: "icebergS3('https://s3.us-east-1.amazonaws.com/bkt/warehouse/f_1/', 'AK', 'SK')",
		},
		{
			name: "s3 cluster",
			obj:  s3, loc: "s3://bkt/warehouse/f_1", cluster: "bifract",
			want: "icebergS3Cluster('bifract', 'https://s3.us-east-1.amazonaws.com/bkt/warehouse/f_1/', 'AK', 'SK')",
		},
		{
			name: "s3 anonymous cluster keeps arity",
			obj:  s3Anon, loc: "s3://bkt/warehouse/f_1", cluster: "bifract",
			want: "icebergS3Cluster('bifract', 'https://s3.us-east-1.amazonaws.com/bkt/warehouse/f_1/')",
		},
		{
			name: "minio path style cluster",
			obj:  minio, loc: "s3://bkt/warehouse/f_1", cluster: "bifract",
			want: "icebergS3Cluster('bifract', 'http://minio:9000/bkt/warehouse/f_1/', 'AK', 'SK')",
		},
		{
			name: "azure single node",
			obj:  azure, loc: "abfs://cont@acct.blob.core.windows.net/warehouse/f_1",
			want: "icebergAzure('https://acct.blob.core.windows.net', 'cont', 'warehouse/f_1/', 'acct', 'KEY')",
		},
		{
			name: "azure cluster",
			obj:  azure, loc: "abfs://cont@acct.blob.core.windows.net/warehouse/f_1", cluster: "bifract",
			want: "icebergAzureCluster('bifract', 'https://acct.blob.core.windows.net', 'cont', 'warehouse/f_1/', 'acct', 'KEY')",
		},
		{
			// No icebergLocal cluster variant exists upstream; disk must never
			// gain a Cluster suffix even if a cluster name is somehow present.
			name: "disk never clusters",
			obj:  disk, loc: "file:///var/lib/bifract/warehouse/f_1", cluster: "bifract",
			want: "icebergLocal('/var/lib/bifract/warehouse/f_1/')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := chIcebergTableFunc(tt.obj, tt.loc, tt.cluster)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got  %s\nwant %s", got, tt.want)
			}
		})
	}
}

// TestChIcebergTableFuncUnsupportedBackend ensures an unknown backend is a hard
// error rather than malformed SQL handed to ClickHouse.
func TestChIcebergTableFuncUnsupportedBackend(t *testing.T) {
	if _, err := chIcebergTableFunc(objstore.Config{Backend: "gcs"}, "gs://b/x", ""); err == nil {
		t.Fatal("expected error for unsupported backend, got nil")
	}
}

// TestChQuoteEscaping guards the literal escaping used for every argument above,
// including the cluster name.
func TestChQuoteEscaping(t *testing.T) {
	if got := chQuote(`a'b\c`); got != `'a\'b\\c'` {
		t.Errorf("chQuote escaping regressed: got %s", got)
	}
	if got := chIcebergArgSmoke(t); strings.Count(got, "'") == 0 {
		t.Errorf("expected quoted args, got %s", got)
	}
}

func chIcebergArgSmoke(t *testing.T) string {
	t.Helper()
	got, err := chIcebergTableFunc(
		objstore.Config{Backend: objstore.BackendS3, S3Region: "r"}, "s3://b/k", "cl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return got
}

// The Cloud fanout cluster is "default" and may be a warehouse-scoped name
// containing a dot. Both must render as the *Cluster variant with the cluster as
// the FIRST argument, because the credential redaction in restore.go is
// positional: a reordering here would start leaking secrets into query_log.
func TestChIcebergTableFuncCloudFanout(t *testing.T) {
	s3 := objstore.Config{
		Backend: objstore.BackendS3, S3Bucket: "bkt", S3Region: "us-east-1",
		S3AccessKey: "AK", S3SecretKey: "SK",
	}
	for _, tc := range []struct{ name, cluster, want string }{
		{
			name:    "cloud default fanout",
			cluster: "default",
			want:    "icebergS3Cluster('default', 'https://s3.us-east-1.amazonaws.com/bkt/warehouse/f_1/', 'AK', 'SK')",
		},
		{
			name:    "warehouse scoped fanout keeps the dot",
			cluster: "all_groups.default",
			want:    "icebergS3Cluster('all_groups.default', 'https://s3.us-east-1.amazonaws.com/bkt/warehouse/f_1/', 'AK', 'SK')",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := chIcebergTableFunc(s3, "s3://bkt/warehouse/f_1", tc.cluster)
			if err != nil {
				t.Fatalf("chIcebergTableFunc: %v", err)
			}
			if got != tc.want {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

// Archive writing goes through the Go SDK from Bifract's own pods, so a custom
// endpoint is only a problem for the paths ClickHouse reads. This is the fact the
// managed-ClickHouse gate keys on.
func TestRequiresCustomEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  objstore.Config
		want bool
	}{
		{"minio", objstore.Config{Backend: objstore.BackendMinIO, S3Endpoint: "http://minio:9000"}, true},
		{"s3 with a custom endpoint", objstore.Config{Backend: objstore.BackendS3, S3Endpoint: "https://nyc3.digitaloceanspaces.com"}, true},
		{"plain s3", objstore.Config{Backend: objstore.BackendS3, S3Region: "us-east-1"}, false},
		{"azure", objstore.Config{Backend: objstore.BackendAzure, AzureAccount: "acct"}, false},
		{"disk", objstore.Config{Backend: objstore.BackendDisk}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.RequiresCustomEndpoint(); got != tc.want {
				t.Errorf("RequiresCustomEndpoint = %v, want %v", got, tc.want)
			}
		})
	}
}
