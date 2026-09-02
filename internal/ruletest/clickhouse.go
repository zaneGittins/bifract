package ruletest

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	dbsql "bifract/db"
	"bifract/pkg/ruleeval"
	"bifract/pkg/storage"
)

// ClickHouseImage is the image the tester starts when no endpoint is supplied. It is
// pinned to the same version docker-compose.yml runs so a rule behaves identically in
// a test and in a real deployment; version_test.go enforces that they stay in step.
const ClickHouseImage = "clickhouse/clickhouse-server:26.6.2.81-alpine"

// logsDatabase is the database the schema lives in (see db/init-clickhouse.sql).
const logsDatabase = "logs"

// readyTimeout bounds the wait for ClickHouse to accept queries. Generous because a
// first-time container start includes provisioning and a server restart.
const readyTimeout = 90 * time.Second

// Target identifies the ClickHouse the tester should use.
type Target struct {
	Host     string
	Port     int
	User     string
	Password string
}

// Addr renders the target as host:port.
func (t Target) Addr() string { return net.JoinHostPort(t.Host, strconv.Itoa(t.Port)) }

// ParseTarget accepts "host:port" or a bare "host" (defaulting to the native port).
func ParseTarget(s, user, password string) (Target, error) {
	t := Target{Port: 9000, User: user, Password: password}
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(strings.TrimPrefix(s, "clickhouse://"), "tcp://")

	if !strings.Contains(s, ":") {
		t.Host = s
		return t, nil
	}

	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return t, fmt.Errorf("invalid ClickHouse address %q: %w", s, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return t, fmt.Errorf("invalid ClickHouse port in %q", s)
	}
	t.Host, t.Port = host, port
	return t, nil
}

// Backend is a connected ClickHouse plus the scratch table the tester writes to.
type Backend struct {
	Client  *storage.ClickHouseClient
	Scratch *ruleeval.Scratch

	container string // docker container id, when the tester started one
	verbose   bool
}

// Connect resolves a ClickHouse, provisions the schema if the server is blank, and
// creates a scratch table for this run. Call Close when finished.
//
// The scratch table is a clone of `logs`, so it carries the real column types, JSON
// type hints, the norm_log DEFAULT expression and the skip indexes. Nothing is written
// to the real logs table, which is what makes it safe to point the tester at a running
// deployment.
func Connect(ctx context.Context, target *Target, verbose bool) (*Backend, error) {
	b := &Backend{verbose: verbose}

	if target == nil {
		started, err := startContainer(ctx, verbose)
		if err != nil {
			return nil, err
		}
		b.container = started.id
		target = &started.target
	}

	// Wait for readiness even when the endpoint was supplied. A ClickHouse doing
	// first-time init runs a temporary server to apply its entrypoint scripts and then
	// restarts, so an endpoint that answers once can still reset the next connection.
	// Retrying here means CI does not depend on getting a healthcheck exactly right.
	if err := waitReady(ctx, *target, readyTimeout, verbose); err != nil {
		b.teardownContainer()
		return nil, err
	}

	if err := ensureDatabase(ctx, *target); err != nil {
		b.teardownContainer()
		return nil, err
	}

	client, err := storage.NewClickHouseClient(storage.SingleNodeOptions(target.Host, target.Port, logsDatabase, target.User, target.Password))
	if err != nil {
		b.teardownContainer()
		return nil, fmt.Errorf("connecting to ClickHouse at %s: %w", target.Addr(), err)
	}
	b.Client = client

	if err := b.ensureSchema(ctx); err != nil {
		b.Close(ctx)
		return nil, err
	}
	scratch, err := ruleeval.NewScratch(ctx, client)
	if err != nil {
		b.Close(ctx)
		return nil, err
	}
	b.Scratch = scratch
	if verbose {
		fmt.Println(dim("  scratch table " + scratch.Qualified()))
	}

	return b, nil
}

// ensureDatabase creates the logs database if the server has never been provisioned.
// It connects through `default` because the driver's handshake names a database and
// would fail outright against a blank server.
func ensureDatabase(ctx context.Context, t Target) error {
	boot, err := storage.NewClickHouseClient(storage.SingleNodeOptions(t.Host, t.Port, "default", t.User, t.Password))
	if err != nil {
		return fmt.Errorf("connecting to ClickHouse at %s: %w", t.Addr(), err)
	}
	defer boot.Close()

	if err := boot.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+logsDatabase); err != nil {
		return fmt.Errorf("creating %s database: %w", logsDatabase, err)
	}
	return nil
}

// ensureSchema provisions the full Bifract schema when the target has no logs table,
// which is the normal state of a CI service container.
func (b *Backend) ensureSchema(ctx context.Context) error {
	rows, err := b.Client.Query(ctx, fmt.Sprintf(
		"SELECT count() AS c FROM system.tables WHERE database = '%s' AND name = 'logs'", logsDatabase))
	if err != nil {
		return fmt.Errorf("probing schema: %w", err)
	}
	if len(rows) > 0 && toUint64(rows[0]["c"]) > 0 {
		return nil
	}

	if b.verbose {
		fmt.Println(dim("  provisioning ClickHouse schema (blank server)"))
	}
	if err := b.Client.Initialize(ctx, dbsql.ClickHouseSQL, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		return fmt.Errorf("provisioning schema: %w", err)
	}
	return nil
}

// Close drops the scratch table, closes the connection and removes the container if
// the tester started one. Safe to call on a partially constructed Backend.
func (b *Backend) Close(ctx context.Context) {
	if b.Client != nil {
		if b.Scratch != nil {
			if err := b.Scratch.Drop(ctx); err != nil {
				fmt.Println(warn("warning: " + err.Error()))
			}
			b.Scratch = nil
		}
		b.Client.Close()
		b.Client = nil
	}
	b.teardownContainer()
}

type startedContainer struct {
	id     string
	target Target
}

// startContainer runs a throwaway ClickHouse. Readiness is awaited by Connect, which
// does it for supplied endpoints too. The container uses --rm so a hard kill of the
// tester still cleans up.
func startContainer(ctx context.Context, verbose bool) (*startedContainer, error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("no --clickhouse endpoint given and docker is not available; " +
			"either start a ClickHouse and pass --clickhouse host:port, or install docker")
	}

	port, err := freePort()
	if err != nil {
		return nil, err
	}

	target := Target{Host: "127.0.0.1", Port: port, User: "default", Password: "bifract"}

	fmt.Println(dim(fmt.Sprintf("Starting %s on port %d", ClickHouseImage, port)))

	args := []string{
		"run", "-d", "--rm",
		"-p", fmt.Sprintf("127.0.0.1:%d:9000", port),
		"-e", "CLICKHOUSE_DB=" + logsDatabase,
		"-e", "CLICKHOUSE_USER=" + target.User,
		"-e", "CLICKHOUSE_PASSWORD=" + target.Password,
		"--ulimit", "nofile=262144:262144",
		ClickHouseImage,
	}
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("starting ClickHouse container: %w", err)
	}
	id := strings.TrimSpace(string(out))

	return &startedContainer{id: id, target: target}, nil
}

func (b *Backend) teardownContainer() {
	if b.container == "" {
		return
	}
	if b.verbose {
		fmt.Println(dim("  removing ClickHouse container"))
	}
	_ = exec.Command("docker", "rm", "-f", b.container).Run()
	b.container = ""
}

// waitReady polls until ClickHouse answers a trivial query or the deadline passes.
func waitReady(ctx context.Context, t Target, timeout time.Duration, verbose bool) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		client, err := storage.NewClickHouseClient(storage.SingleNodeOptions(t.Host, t.Port, "default", t.User, t.Password))
		if err == nil {
			probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			// version() returns a String; a bare SELECT 1 is UInt8, which the
			// generic row scanner cannot map into map[string]interface{}.
			_, qerr := client.Query(probeCtx, "SELECT version() AS v")
			cancel()
			client.Close()
			if qerr == nil {
				return nil
			}
			lastErr = qerr
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("ClickHouse at %s did not become ready within %s: %w", t.Addr(), timeout, lastErr)
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("finding a free port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func toUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case int64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case int:
		if n < 0 {
			return 0
		}
		return uint64(n)
	}
	return 0
}
