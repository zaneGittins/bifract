package setup

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"bifract/pkg/storage"
)

// resetWarning is shown before anything is destroyed. It names what survives as
// specifically as what does not, because the difference is the whole reason this
// is a targeted reset rather than a volume wipe.
const resetWarning = `This DESTROYS all ClickHouse log data.

Deleted:  logs, raw logs, the alert hot table, histogram rollups,
          process lineage and frequency tables, and analytics model tables.
Kept:     everything in Postgres (users, fractals, saved searches, notebooks,
          dashboards, alerts, API keys), ClickHouse dictionaries, and the
          Iceberg archive.

This version changed the logs partition key from event time to ingest time.
ClickHouse cannot alter a partition key in place, so there is no migration.`

// confirmDestructive asks for an explicit yes. Anything else aborts. Mirrors the
// restore prompt in backup.go rather than a bare y/N: the cost of a mistaken
// keystroke here is every log in the install.
func confirmDestructive(prompt string) bool {
	fmt.Println()
	fmt.Println(WarningStyle.Render(resetWarning))
	fmt.Println()
	fmt.Print(PromptStyle.Render("  " + prompt + " Type 'yes' to continue: "))
	answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	return strings.TrimSpace(strings.ToLower(answer)) == "yes"
}

// RunResetLogs drops all ClickHouse log data for a docker install, then restarts
// the app so it re-provisions the schema from its embedded init SQL.
//
// Nothing is recreated here. The app owns ClickHouse provisioning (see
// templates.go: the installer deliberately never writes init-clickhouse.sql to
// disk), so dropping and restarting is what produces the new schema, and the
// migration ledger is stamped in the same step.
func RunResetLogs(dir string, nonInteractive bool) error {
	envPath := filepath.Join(dir, ".env")
	env, err := ReadEnvFile(envPath)
	if err != nil {
		return fmt.Errorf("read .env at %s: %w", envPath, err)
	}
	if env["POSTGRES_PASSWORD"] == "" || env["CLICKHOUSE_PASSWORD"] == "" {
		return fmt.Errorf("missing POSTGRES_PASSWORD or CLICKHOUSE_PASSWORD in %s", envPath)
	}

	docker := &DockerOps{Dir: dir}
	if !docker.IsRunning() {
		return fmt.Errorf("the stack is not running\n  Start it first (bifract --start): the reset works through the running containers")
	}

	if !nonInteractive && !confirmDestructive("Reset all ClickHouse log data?") {
		fmt.Println("  Reset cancelled")
		return nil
	}

	resetSteps(5)

	// Stop the writers first, or ingest recreates tables under the drop and the
	// app's own retry loop races the reset.
	printStep("Stopping ingest")
	if out, err := docker.compose("stop", "bifract", "bifract-ingest").CombinedOutput(); err != nil {
		abandonStep()
		return fmt.Errorf("stop app containers: %w\n%s", err, out)
	}
	printDone("Ingest stopped")

	pgUser, pgDB := postgresCreds(env)

	printStep("Listing analytics model tables")
	modelViews, modelTables, err := readModelCHObjects(docker, pgUser, pgDB)
	if err != nil {
		abandonStep()
		return err
	}
	printDone(fmt.Sprintf("Found %d model object(s)", len(modelViews)+len(modelTables)))

	printStep("Dropping ClickHouse log data")
	if err := dropClickHouseLogData(env, docker, modelViews, modelTables); err != nil {
		abandonStep()
		return err
	}
	printDone("ClickHouse log data dropped")

	printStep("Clearing stale Postgres state")
	if err := resetPostgresState(docker, pgUser, pgDB); err != nil {
		abandonStep()
		return err
	}
	printDone("Postgres state cleared")

	printStep("Restarting Bifract")
	if out, err := docker.Up(); err != nil {
		abandonStep()
		return fmt.Errorf("restart: %w\n%s", err, out)
	}
	if err := docker.HealthCheck(120 * time.Second); err != nil {
		printWarn("Started, but the health check did not pass yet")
	} else {
		printDone("Bifract restarted")
	}

	fmt.Println()
	fmt.Println(SuccessStyle.Render("  Reset complete. The schema was recreated on startup."))
	fmt.Println(DimStyle.Render("  Custom schema fields are re-applied in the background; models need a fresh backfill."))
	return nil
}

// clickHouseNeedsReset reports whether an existing logs table still carries the
// pre-ingest-time partition key. Read from system.tables rather than tracked as a
// version, so it describes the live schema and cannot drift out of sync with it.
// A fresh install with no logs table needs nothing.
func clickHouseNeedsReset(docker *DockerOps, chPassword string) (bool, error) {
	out, err := docker.ExecClickHouse("default", chPassword,
		"SELECT partition_key FROM system.tables WHERE database = 'logs' AND name = 'logs'")
	if err != nil {
		return false, fmt.Errorf("%w\n%s", err, out)
	}
	key := strings.TrimSpace(out)
	if key == "" {
		return false, nil
	}
	return !strings.Contains(key, "ingest_timestamp"), nil
}

// resetBeforeUpgrade performs the drop inline during an upgrade. The app is left
// stopped: the upgrade's own restart brings it up on the new image, which then
// provisions the new schema.
func resetBeforeUpgrade(env map[string]string, docker *DockerOps) error {
	pgUser, pgDB := postgresCreds(env)

	printStep("Dropping incompatible ClickHouse log data")
	modelViews, modelTables, err := readModelCHObjects(docker, pgUser, pgDB)
	if err != nil {
		abandonStep()
		return err
	}
	if err := dropClickHouseLogData(env, docker, modelViews, modelTables); err != nil {
		abandonStep()
		return err
	}
	if err := resetPostgresState(docker, pgUser, pgDB); err != nil {
		abandonStep()
		return err
	}
	printDone("ClickHouse log data dropped")
	return nil
}

// postgresCreds reads the Postgres user and database, falling back to the values
// the installer writes by default.
func postgresCreds(env map[string]string) (user, db string) {
	user, db = env["POSTGRES_USER"], env["POSTGRES_DB"]
	if user == "" {
		user = "bifract"
	}
	if db == "" {
		db = "bifract"
	}
	return user, db
}

// dropClickHouseLogData runs the drop statements against whichever ClickHouse the
// install points at. A bundled server has no published port, so it is reached
// through its container; an external or Cloud endpoint is reachable directly and
// has no container to exec into.
func dropClickHouseLogData(env map[string]string, docker *DockerOps, extraViews, extraTables []string) error {
	stmts := storage.ResetLogDataStatements(extraViews, extraTables)

	target := TargetFromEnv(env)
	if target.Bundled() {
		for _, stmt := range stmts {
			if out, err := docker.ExecClickHouse("default", env["CLICKHOUSE_PASSWORD"], stmt); err != nil {
				return fmt.Errorf("clickhouse reset (%s): %w\n%s", stmt, err, out)
			}
		}
		return nil
	}

	client, err := externalClickHouseClient(target, env)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	return client.ResetLogData(ctx, extraViews, extraTables)
}

// externalClickHouseClient opens a client against an external or Cloud ClickHouse.
// The CLICKHOUSE_* variables are applied to this process and read back through
// storage's own env contract, so the installer resolves hosts, ports and TLS
// exactly the way the app container would rather than reimplementing it.
func externalClickHouseClient(target ClickHouseTarget, env map[string]string) (*storage.ClickHouseClient, error) {
	for k, v := range env {
		if strings.HasPrefix(k, "CLICKHOUSE_") {
			os.Setenv(k, v)
		}
	}
	chEnv, err := storage.ClickHouseEnvFromOS()
	if err != nil {
		return nil, fmt.Errorf("resolve ClickHouse config: %w", err)
	}
	opts, err := chEnv.ClientOptions(storage.DefaultQueryPoolConfig(), storage.RoleControlPlane)
	if err != nil {
		return nil, fmt.Errorf("resolve ClickHouse config: %w", err)
	}
	client, err := storage.NewClickHouseClient(opts)
	if err != nil {
		return nil, fmt.Errorf("connect to %s ClickHouse: %w", target.Deployment, err)
	}
	return client, nil
}

// readModelCHObjects lists the ClickHouse objects owned by the analytics models.
// Postgres is authoritative for which models exist; models.CHObjectNames is
// authoritative for what each one is called, since a model owns more objects than
// the two names Postgres records.
func readModelCHObjects(docker *DockerOps, pgUser, pgDB string) (views, tables []string, err error) {
	out, err := docker.ExecPostgresTuples(pgUser, pgDB, storage.ModelIDsQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("list analytics models: %w\n%s", err, out)
	}
	views, tables = modelCHObjects(out)
	return views, tables, nil
}

// modelCHObjects expands psql -tAc output, one model id per line, into every
// ClickHouse object those models own.
func modelCHObjects(out string) (views, tables []string) {
	for _, id := range nonEmptyLines(out) {
		v, t := storage.ModelCHObjectNames(id)
		views = append(views, v...)
		tables = append(tables, t...)
	}
	return views, tables
}

// postgresResetSQL wraps the clear in one transaction so a failure part-way
// cannot leave a cursor pointing into the gap between what was dropped and what
// still claims to be there.
func postgresResetSQL() string {
	return "BEGIN;\n" + strings.Join(storage.ResetPostgresStateStatements(), ";\n") + ";\nCOMMIT;"
}

// resetPostgresState clears the rows describing log data that no longer exists.
func resetPostgresState(docker *DockerOps, pgUser, pgDB string) error {
	out, err := docker.ExecPostgres(pgUser, pgDB, postgresResetSQL())
	if err != nil {
		return fmt.Errorf("clear postgres state: %w\n%s", err, out)
	}
	if strings.Contains(out, "ERROR") {
		return fmt.Errorf("clear postgres state:\n%s", out)
	}
	return nil
}
