//go:build chcluster

// Recall's identity against the two-shard cluster in test/chcluster (see the README
// there): go test -tags chcluster ./pkg/archive/ -run TestRecallRunsOnTheRecallIdentity
package archive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	dbsql "bifract/db"
	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

// The archive client shares the server's identities, so a user's recall SQL runs on
// the read-only recall user under the recall workload instead of as admin, and that
// user may still reach the object store through every table function recall emits.
func TestRecallRunsOnTheRecallIdentity(t *testing.T) {
	ctx := context.Background()
	env := storage.ClickHouseEnv{
		Deployment: storage.DeploymentSelfManagedCluster,
		Hosts:      []string{"localhost:19001", "localhost:19002"},
		Port:       9000,
		Database:   "logs",
		User:       "default",
		Password:   "bifract",
		Cluster:    "bftest",
	}
	opts, err := env.ClientOptions(storage.DefaultQueryPoolConfig(), storage.RoleControlPlane)
	if err != nil {
		t.Fatal(err)
	}
	main, err := storage.NewClickHouseClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer main.Close()
	if err := main.Initialize(ctx, dbsql.ClickHouseSQL, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if err := main.ReconcileQueryWorkloads(ctx, storage.WorkloadLimits{SearchCPUPercent: 50, SearchMemoryPercent: 50, RecallCPUPercent: 25, RecallMemoryPercent: 25}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	ch, err := NewCHClient(Config{CH: env, Identities: main})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	recall := storage.RecallContext(ctx)
	scalar := func(ctx context.Context, sql string) (string, error) {
		var v string
		err := ch.QueryRow(ctx, sql).Scan(&v)
		return v, err
	}
	for name, tc := range map[string]struct {
		ctx       context.Context
		sql, want string
	}{
		"recall user":   {recall, "SELECT currentUser()", storage.RecallCHUser},
		"recall tag":    {recall, "SELECT getSetting('workload')", storage.QueryRecallWorkload},
		"unmarked user": {ctx, "SELECT currentUser()", storage.ReaderCHUser},
		"admin user":    {storage.AdminReadContext(ctx), "SELECT currentUser()", "default"},
	} {
		if got, err := scalar(tc.ctx, tc.sql); err != nil || got != tc.want {
			t.Errorf("%s: got %q %v, want %q", name, got, err, tc.want)
		}
	}

	// Unreachable endpoints: ClickHouse checks the grant before it connects, so the
	// recall user must fail on the network and the reader on the privilege.
	stores := map[string]struct {
		obj objstore.Config
		loc string
	}{
		"s3":    {objstore.Config{Backend: objstore.BackendMinIO, S3Endpoint: "http://127.0.0.1:1", S3AccessKey: "k", S3SecretKey: "s"}, "s3://bucket/ns/t"},
		"azure": {objstore.Config{Backend: objstore.BackendAzure, AzureAccount: "acct", AzureKey: "a2V5", AzureContainer: "c", AzureEndpoint: "http://127.0.0.1:1/acct"}, "abfs://c@acct.blob.core.windows.net/ns/t"},
	}
	for name, s := range stores {
		for _, cluster := range []string{"", "bftest"} {
			fn, err := chIcebergTableFunc(s.obj, s.loc, cluster)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			// Bounded: past the grant check, the client retries the dead endpoint.
			sql := "SELECT count() FROM " + fn + " SETTINGS max_execution_time = 10"
			probe := func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
				defer cancel()
				_, err := scalar(ctx, sql)
				return err
			}
			var ex *proto.Exception
			if err := probe(recall); err == nil || errors.As(err, &ex) && ex.Code == 497 {
				t.Errorf("%s cluster=%q: recall user err = %v, want a network failure or timeout", name, cluster, err)
			}
			if err := probe(ctx); !errors.As(err, &ex) || ex.Code != 497 {
				t.Errorf("%s cluster=%q: reader err = %v, want access denied", name, cluster, err)
			}
		}
	}
}
