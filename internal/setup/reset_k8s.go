package setup

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"bifract/pkg/storage"
)

const (
	k8sNamespace     = "bifract"
	k8sSecretName    = "bifract-secrets"
	k8sCHPodSelector = "app.kubernetes.io/name=clickhouse-server"
	k8sCHPodPrefix   = "bifract-ch"

	chDatabase = "logs"
	// The clickhouse-client default is 300s, which a large DROP ... SYNC exceeds.
	chClientTimeoutSec = "3600"
)

// RunResetLogsK8s drops all ClickHouse log data in a Kubernetes deployment, then
// restarts the app so it re-provisions the schema.
//
// Statements go to every ClickHouse pod individually rather than through one
// connection: with more than one shard, each holds its own local tables and its
// own migration ledger, so a single cluster-wide answer would leave the other
// shards half-reset and serving short results.
func RunResetLogsK8s(nonInteractive bool) error {
	if _, err := exec.LookPath("kubectl"); err != nil {
		return fmt.Errorf("kubectl not found in PATH")
	}

	pods, err := clickHousePods()
	if err != nil {
		return err
	}
	secrets, reason := tryReadLiveSecrets(k8sNamespace, k8sSecretName)
	if reason != "" {
		return fmt.Errorf("read secret %s/%s: %s", k8sNamespace, k8sSecretName, reason)
	}
	chPassword := secrets["CLICKHOUSE_PASSWORD"]
	if chPassword == "" {
		return fmt.Errorf("CLICKHOUSE_PASSWORD is not set in secret %s/%s", k8sNamespace, k8sSecretName)
	}

	fmt.Printf("  Target: %d ClickHouse pod(s) in namespace %s\n", len(pods), k8sNamespace)
	if !nonInteractive && !confirmDestructive("Reset all ClickHouse log data?") {
		fmt.Println("  Reset cancelled")
		return nil
	}

	resetSteps(4 + len(pods))

	printStep("Scaling down writers")
	// Recorded before scaling down so the restore below puts back what was there.
	// A workload that is absent (single-tier installs run ingest inside the app)
	// simply has no entry.
	replicas := currentReplicas(writerWorkloads)
	for _, w := range replicas {
		if err := kubectl("scale", w.kind, w.name, "-n", k8sNamespace, "--replicas=0"); err != nil {
			abandonStep()
			return fmt.Errorf("scale down %s/%s: %w", w.kind, w.name, err)
		}
	}
	// Restore on every exit path: an abort partway through otherwise strands the
	// cluster with no app and no ingest.
	restored := false
	defer func() {
		if !restored {
			restoreReplicas(replicas)
		}
	}()
	printDone(fmt.Sprintf("Scaled %d workload(s) to zero", len(replicas)))

	pgUser, pgDB := secrets["POSTGRES_USER"], secrets["POSTGRES_DB"]
	if pgUser == "" {
		pgUser = "bifract"
	}
	if pgDB == "" {
		pgDB = "bifract"
	}

	printStep("Listing analytics model tables")
	modelViews, modelTables, err := readModelCHObjectsK8s(pgUser, pgDB)
	if err != nil {
		abandonStep()
		return err
	}
	printDone(fmt.Sprintf("Found %d model object(s)", len(modelViews)+len(modelTables)))

	stmts := storage.ResetLogDataStatements(modelViews, modelTables)
	for i, pod := range pods {
		// Per-pod progress: one spinner for a multi-minute drop looks like a hang.
		printStep(fmt.Sprintf("Dropping ClickHouse log data (%d/%d: %s)", i+1, len(pods), pod))
		for _, stmt := range stmts {
			if out, err := chExec(pod, chPassword, stmt); err != nil {
				abandonStep()
				return fmt.Errorf("clickhouse reset on %s (%s): %w\n%s", pod, stmt, err, out)
			}
		}
		if err := cleanKeeperPaths(pod, chPassword, modelTables); err != nil {
			abandonStep()
			return fmt.Errorf("keeper cleanup on %s: %w", pod, err)
		}
		printDone(fmt.Sprintf("Dropped on %s", pod))
	}

	printStep("Clearing stale Postgres state")
	if err := resetPostgresStateK8s(pgUser, pgDB); err != nil {
		abandonStep()
		return err
	}
	printDone("Postgres state cleared")

	printStep("Restarting Bifract")
	restoreReplicas(replicas)
	restored = true
	printDone("Writers scaled back up")

	fmt.Println()
	fmt.Println(SuccessStyle.Render("  Reset complete. The schema is recreated when the app pod becomes ready."))
	return nil
}

// writerWorkloads write to ClickHouse and must be stopped for the drop. Kinds
// differ: bifract is a Deployment, bifract-ingest a StatefulSet. `kubectl scale
// deployment` on a StatefulSet silently matches nothing.
var writerWorkloads = []k8sWorkload{
	{kind: "deployment", name: "bifract"},
	{kind: "statefulset", name: "bifract-ingest"},
}

type k8sWorkload struct {
	kind     string
	name     string
	replicas int
}

// currentReplicas reads each workload's replica count, skipping any that is absent
// or already zero, so the restore only touches what this run scaled down.
func currentReplicas(workloads []k8sWorkload) []k8sWorkload {
	var out []k8sWorkload
	for _, w := range workloads {
		res, err := kubectlOut("get", w.kind, w.name, "-n", k8sNamespace, "-o", "jsonpath={.spec.replicas}")
		if err != nil {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(res))
		if err != nil || n <= 0 {
			continue
		}
		w.replicas = n
		out = append(out, w)
	}
	return out
}

func restoreReplicas(workloads []k8sWorkload) {
	for _, w := range workloads {
		if err := kubectl("scale", w.kind, w.name, "-n", k8sNamespace,
			fmt.Sprintf("--replicas=%d", w.replicas)); err != nil {
			printWarn(fmt.Sprintf("Scale %s/%s back to %d failed: %v", w.kind, w.name, w.replicas, err))
		}
	}
}

// chExec runs one statement in a ClickHouse pod. receive_timeout is raised because
// the client default is 300s and a large DROP ... SYNC exceeds it, killing the
// client while the server keeps going.
func chExec(pod, password, stmt string) (string, error) {
	return kubectlOut("exec", "-n", k8sNamespace, pod, "--", "clickhouse-client",
		"--password", password, "--database", chDatabase,
		"--receive_timeout", chClientTimeoutSec, "--send_timeout", chClientTimeoutSec,
		"--query", stmt)
}

// cleanKeeperPaths removes the Keeper registration of every table just dropped.
// DROP TABLE only frees the path once its last replica is gone, so entries left by
// earlier pod incarnations keep it alive and re-provisioning fails with code 342.
func cleanKeeperPaths(pod, password string, modelTables []string) error {
	shard, err := chExec(pod, password, storage.ShardMacroQuery)
	if err != nil {
		return fmt.Errorf("read shard macro: %w\n%s", err, shard)
	}
	shard = strings.TrimSpace(shard)
	if shard == "" {
		return nil // not a replicated cluster; nothing registered in Keeper
	}

	for _, table := range storage.ResetKeeperTables(modelTables) {
		path := storage.KeeperTablePath(shard, chDatabase, table)
		// An absent path errors, which is the healthy case.
		out, err := chExec(pod, password, storage.KeeperReplicasQuery(path))
		if err != nil {
			continue
		}
		for _, replica := range nonEmptyLines(out) {
			// Non-fatal: code 305 means the replica is still active or a local table
			// holds the path, i.e. it is not an orphan and must be left alone.
			if _, err := chExec(pod, password, storage.DropKeeperReplicaStatement(replica, path)); err != nil {
				continue
			}
		}
	}
	return nil
}

// clickHousePods lists the ClickHouse pods, by operator label first and by name
// prefix as a fallback, matching what scripts/reset_clickhouse.sh looks for.
func clickHousePods() ([]string, error) {
	out, err := kubectlOut("get", "pods", "-n", k8sNamespace, "-l", k8sCHPodSelector,
		"--no-headers", "-o", "custom-columns=:metadata.name")
	var pods []string
	if err == nil {
		pods = nonEmptyLines(out)
	}
	if len(pods) == 0 {
		out, err = kubectlOut("get", "pods", "-n", k8sNamespace, "--no-headers", "-o", "custom-columns=:metadata.name")
		if err != nil {
			return nil, fmt.Errorf("list ClickHouse pods: %w\n%s", err, out)
		}
		pods = nil
		for _, l := range nonEmptyLines(out) {
			if strings.HasPrefix(l, k8sCHPodPrefix) {
				pods = append(pods, l)
			}
		}
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("no ClickHouse pods found in namespace %s", k8sNamespace)
	}
	return pods, nil
}

// postgresPod returns the Postgres pod name. It is a StatefulSet with one replica,
// so the first match is the only one.
func postgresPod() (string, error) {
	out, err := kubectlOut("get", "pods", "-n", k8sNamespace, "--no-headers", "-o", "custom-columns=:metadata.name")
	if err != nil {
		return "", fmt.Errorf("list pods: %w\n%s", err, out)
	}
	for _, l := range nonEmptyLines(out) {
		if strings.HasPrefix(l, "bifract-postgres") || strings.HasPrefix(l, "postgres") {
			return l, nil
		}
	}
	return "", fmt.Errorf("no Postgres pod found in namespace %s", k8sNamespace)
}

func readModelCHObjectsK8s(pgUser, pgDB string) (views, tables []string, err error) {
	pod, err := postgresPod()
	if err != nil {
		return nil, nil, err
	}
	out, err := kubectlOut("exec", "-n", k8sNamespace, pod, "--",
		"psql", "-U", pgUser, "-d", pgDB, "-tAc", storage.ModelIDsQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("list analytics models: %w\n%s", err, out)
	}
	views, tables = modelCHObjects(out)
	return views, tables, nil
}

func resetPostgresStateK8s(pgUser, pgDB string) error {
	pod, err := postgresPod()
	if err != nil {
		return err
	}
	out, err := kubectlOut("exec", "-n", k8sNamespace, pod, "--",
		"psql", "-U", pgUser, "-d", pgDB, "-c", postgresResetSQL())
	if err != nil {
		return fmt.Errorf("clear postgres state: %w\n%s", err, out)
	}
	if strings.Contains(out, "ERROR") {
		return fmt.Errorf("clear postgres state:\n%s", out)
	}
	return nil
}

func kubectl(args ...string) error {
	_, err := kubectlOut(args...)
	return err
}

func kubectlOut(args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}
