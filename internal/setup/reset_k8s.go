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

	resetSteps(5)

	printStep("Scaling down writers")
	// Recorded before scaling down so the restore below puts back what was there.
	// A deployment that is absent (single-tier installs run ingest inside the app)
	// simply has no entry.
	replicas := currentReplicas(writerDeployments)
	for deploy := range replicas {
		if err := kubectl("scale", "deployment", deploy, "-n", k8sNamespace, "--replicas=0"); err != nil {
			abandonStep()
			return fmt.Errorf("scale down %s: %w", deploy, err)
		}
	}
	printDone(fmt.Sprintf("Scaled %d deployment(s) to zero", len(replicas)))

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

	printStep("Dropping ClickHouse log data")
	stmts := storage.ResetLogDataStatements(modelViews, modelTables)
	for _, pod := range pods {
		for _, stmt := range stmts {
			if out, err := kubectlOut("exec", "-n", k8sNamespace, pod, "--",
				"clickhouse-client", "--password", chPassword, "--database", "logs", "--query", stmt); err != nil {
				abandonStep()
				return fmt.Errorf("clickhouse reset on %s (%s): %w\n%s", pod, stmt, err, out)
			}
		}
	}
	printDone(fmt.Sprintf("Dropped on %d pod(s)", len(pods)))

	printStep("Clearing stale Postgres state")
	if err := resetPostgresStateK8s(pgUser, pgDB); err != nil {
		abandonStep()
		return err
	}
	printDone("Postgres state cleared")

	printStep("Restarting Bifract")
	for deploy, n := range replicas {
		if err := kubectl("scale", "deployment", deploy, "-n", k8sNamespace, fmt.Sprintf("--replicas=%d", n)); err != nil {
			printWarn(fmt.Sprintf("Scale %s back to %d failed: %v", deploy, n, err))
		}
	}
	printDone("Writers scaled back up")

	fmt.Println()
	fmt.Println(SuccessStyle.Render("  Reset complete. The schema is recreated when the app pod becomes ready."))
	return nil
}

// writerDeployments are the deployments that write to ClickHouse and so must be
// stopped for the drop.
var writerDeployments = []string{"bifract", "bifract-ingest"}

// currentReplicas reads each deployment's replica count, skipping any that does
// not exist. Returning a map means the restore only touches what it scaled down.
func currentReplicas(deploys []string) map[string]int {
	out := make(map[string]int, len(deploys))
	for _, d := range deploys {
		res, err := kubectlOut("get", "deployment", d, "-n", k8sNamespace, "-o", "jsonpath={.spec.replicas}")
		if err != nil {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(res))
		if err != nil || n <= 0 {
			continue
		}
		out[d] = n
	}
	return out
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
