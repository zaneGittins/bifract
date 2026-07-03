package objstore

import "testing"

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"disk ok", Config{Backend: BackendDisk, DiskPath: "/data"}, false},
		{"disk no path", Config{Backend: BackendDisk}, true},
		{"s3 ok", Config{Backend: BackendS3, S3Bucket: "b"}, false},
		{"s3 no bucket", Config{Backend: BackendS3}, true},
		{"minio needs endpoint", Config{Backend: BackendMinIO, S3Bucket: "b"}, true},
		{"minio ok", Config{Backend: BackendMinIO, S3Bucket: "b", S3Endpoint: "http://minio:9000"}, false},
		{"azure ok", Config{Backend: BackendAzure, AzureAccount: "a", AzureContainer: "c"}, false},
		{"azure missing", Config{Backend: BackendAzure, AzureAccount: "a"}, true},
		{"unknown", Config{Backend: "gcs"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestWarehouseURI(t *testing.T) {
	cases := []struct {
		cfg  Config
		want string
	}{
		{Config{Backend: BackendDisk, DiskPath: "/var/lib/bifract/archive"}, "file:///var/lib/bifract/archive"},
		{Config{Backend: BackendDisk, DiskPath: "/data/", Prefix: "prod"}, "file:///data/prod"},
		{Config{Backend: BackendS3, S3Bucket: "logs"}, "s3://logs"},
		{Config{Backend: BackendS3, S3Bucket: "logs", Prefix: "/archive/"}, "s3://logs/archive"},
		{Config{Backend: BackendMinIO, S3Bucket: "logs"}, "s3://logs"},
		{Config{Backend: BackendAzure, AzureContainer: "arc", Prefix: "p"}, "abfs://arc/p"},
	}
	for _, tc := range cases {
		if got := tc.cfg.WarehouseURI(); got != tc.want {
			t.Errorf("WarehouseURI(%+v) = %q, want %q", tc.cfg, got, tc.want)
		}
	}
}

func TestIcebergProps(t *testing.T) {
	// MinIO must request path-style addressing.
	p := Config{Backend: BackendMinIO, S3Bucket: "b", S3Endpoint: "http://minio:9000",
		S3AccessKey: "ak", S3SecretKey: "sk", S3Region: "us-east-1"}.IcebergProps()
	if p["s3.endpoint"] != "http://minio:9000" {
		t.Errorf("missing endpoint: %v", p)
	}
	if p["s3.force-virtual-addressing"] != "false" {
		t.Errorf("minio must use path-style (force-virtual-addressing=false): %v", p)
	}
	if p["s3.access-key-id"] != "ak" || p["s3.secret-access-key"] != "sk" {
		t.Errorf("creds not propagated: %v", p)
	}

	// Plain AWS S3 (no endpoint) should NOT force path-style.
	p2 := Config{Backend: BackendS3, S3Bucket: "b", S3Region: "us-west-2"}.IcebergProps()
	if _, ok := p2["s3.force-virtual-addressing"]; ok {
		t.Errorf("AWS S3 should use default (virtual-host) addressing: %v", p2)
	}

	// Azure shared-key.
	p3 := Config{Backend: BackendAzure, AzureAccount: "acct", AzureKey: "key", AzureContainer: "c"}.IcebergProps()
	if p3["adls.auth.shared-key.account.name"] != "acct" || p3["adls.auth.shared-key.account.key"] != "key" {
		t.Errorf("azure shared-key not set: %v", p3)
	}

	// Disk has no IO props.
	if len(Config{Backend: BackendDisk, DiskPath: "/d"}.IcebergProps()) != 0 {
		t.Error("disk should have no iceberg props")
	}
}
