package setup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

type DockerOps struct {
	Dir string
}

func (d *DockerOps) compose(args ...string) *exec.Cmd {
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = d.Dir
	return cmd
}

func (d *DockerOps) Pull() error {
	cmd := d.compose("pull")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (d *DockerOps) Up() (string, error) {
	out, err := d.compose("up", "-d").CombinedOutput()
	return string(out), err
}

func (d *DockerOps) Down() (string, error) {
	out, err := d.compose("down").CombinedOutput()
	return string(out), err
}

func (d *DockerOps) Restart(container string) (string, error) {
	out, err := exec.Command("docker", "restart", container).CombinedOutput()
	return string(out), err
}

func (d *DockerOps) IsRunning() bool {
	out, err := d.compose("ps", "--status", "running", "-q").Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) != ""
}

// HealthCheck polls Docker's native health status for the bifract container.
// The bifract service has a HEALTHCHECK in docker-compose that calls the binary's
// built-in -healthcheck flag, so we just wait for Docker to report "healthy".
func (d *DockerOps) HealthCheck(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("health check timed out after %s", timeout)
		case <-ticker.C:
			out, err := exec.Command("docker", "inspect", "--format", "{{.State.Health.Status}}", "bifract-app").Output()
			if err != nil {
				continue
			}
			if strings.TrimSpace(string(out)) == "healthy" {
				return nil
			}
		}
	}
}

// CheckImageAvailable verifies that a container image exists on the registry
// using docker manifest inspect (a lightweight metadata-only check).
func CheckImageAvailable(image string) error {
	out, err := exec.Command("docker", "manifest", "inspect", image).CombinedOutput()
	if err != nil {
		return fmt.Errorf("image %s not available: %s", image, strings.TrimSpace(string(out)))
	}
	return nil
}

// BifractImage returns the full ghcr image reference for the given version tag.
func BifractImage(tag string) string {
	return fmt.Sprintf("ghcr.io/zanegittins/bifract:%s", tag)
}

// ExecSQL runs a SQL command inside a running container via docker compose exec.
func (d *DockerOps) ExecPostgres(user, db, sql string) (string, error) {
	out, err := d.compose("exec", "-T", "postgres", "psql", "-U", user, "-d", db, "-c", sql).CombinedOutput()
	return string(out), err
}

// ExecPostgresDump runs pg_dump inside the postgres container and returns the output.
func (d *DockerOps) ExecPostgresDump(user, db string) ([]byte, error) {
	cmd := d.compose("exec", "-T", "postgres", "pg_dump", "-U", user, "-d", db, "-Fc")
	return cmd.Output()
}

// ExecPostgresRestore pipes data to pg_restore inside the postgres container.
func (d *DockerOps) ExecPostgresRestore(user, db string, data io.Reader) (string, error) {
	cmd := d.compose("exec", "-T", "postgres", "pg_restore", "-U", user, "-d", db, "--clean", "--if-exists")
	cmd.Stdin = data
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stderr.String(), err
	}
	return "", nil
}

func (d *DockerOps) ExecClickHouse(user, password, sql string) (string, error) {
	out, err := d.compose("exec", "-T", "clickhouse", "clickhouse-client",
		"--user", user, "--password", password, "--query", sql).CombinedOutput()
	return string(out), err
}
