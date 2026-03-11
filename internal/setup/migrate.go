package setup

import (
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/postgres/*.sql
var postgresMigrations embed.FS

//go:embed migrations/clickhouse/*.sql
var clickhouseMigrations embed.FS

type Migration struct {
	Number int
	Name   string
	SQL    string
}

func LoadMigrations(fsys embed.FS, dir string) ([]Migration, error) {
	var migrations []Migration

	err := fs.WalkDir(fsys, dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}

		name := filepath.Base(path)
		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 2 {
			return nil
		}
		num, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil
		}

		content, err := fsys.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", path, err)
		}

		migrations = append(migrations, Migration{
			Number: num,
			Name:   strings.TrimSuffix(name, ".sql"),
			SQL:    string(content),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Number < migrations[j].Number
	})
	return migrations, nil
}

const createMigrationsTablePG = `
CREATE TABLE IF NOT EXISTS _bifract_migrations (
    number INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT NOW()
);`

const createMigrationsTableCH = `
CREATE TABLE IF NOT EXISTS logs._bifract_migrations (
    number UInt32,
    name String,
    applied_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY number;`

// RunPostgresMigrations applies unapplied migrations via docker compose exec.
func RunPostgresMigrations(docker *DockerOps, user, db string) (int, error) {
	migrations, err := LoadMigrations(postgresMigrations, "migrations/postgres")
	if err != nil {
		return 0, fmt.Errorf("load postgres migrations: %w", err)
	}
	if len(migrations) == 0 {
		return 0, nil
	}

	if _, err := docker.ExecPostgres(user, db, createMigrationsTablePG); err != nil {
		return 0, fmt.Errorf("create migrations table: %w", err)
	}

	out, err := docker.ExecPostgres(user, db, "SELECT COALESCE(MAX(number), 0) FROM _bifract_migrations;")
	if err != nil {
		return 0, fmt.Errorf("query migration state: %w", err)
	}
	maxApplied := parseMaxNumber(out)

	applied := 0
	for _, m := range migrations {
		if m.Number <= maxApplied {
			continue
		}
		if _, err := docker.ExecPostgres(user, db, m.SQL); err != nil {
			return applied, fmt.Errorf("migration %s failed: %w", m.Name, err)
		}
		safeName := strings.ReplaceAll(m.Name, "'", "''")
		record := fmt.Sprintf("INSERT INTO _bifract_migrations (number, name) VALUES (%d, '%s');", m.Number, safeName)
		if _, err := docker.ExecPostgres(user, db, record); err != nil {
			return applied, fmt.Errorf("record migration %s: %w", m.Name, err)
		}
		applied++
	}
	return applied, nil
}

// RunClickHouseMigrations applies unapplied migrations via docker compose exec.
func RunClickHouseMigrations(docker *DockerOps, user, password string) (int, error) {
	migrations, err := LoadMigrations(clickhouseMigrations, "migrations/clickhouse")
	if err != nil {
		return 0, fmt.Errorf("load clickhouse migrations: %w", err)
	}
	if len(migrations) == 0 {
		return 0, nil
	}

	if _, err := docker.ExecClickHouse(user, password, createMigrationsTableCH); err != nil {
		return 0, fmt.Errorf("create migrations table: %w", err)
	}

	out, err := docker.ExecClickHouse(user, password, "SELECT max(number) FROM logs._bifract_migrations;")
	if err != nil {
		return 0, fmt.Errorf("query migration state: %w", err)
	}
	maxApplied := parseMaxNumber(out)

	applied := 0
	for _, m := range migrations {
		if m.Number <= maxApplied {
			continue
		}
		// ClickHouse needs statements executed one at a time
		stmts := splitStatements(m.SQL)
		for _, stmt := range stmts {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if _, err := docker.ExecClickHouse(user, password, stmt); err != nil {
				return applied, fmt.Errorf("migration %s failed: %w", m.Name, err)
			}
		}
		safeName := strings.ReplaceAll(m.Name, "'", "''")
		record := fmt.Sprintf("INSERT INTO logs._bifract_migrations (number, name) VALUES (%d, '%s');", m.Number, safeName)
		if _, err := docker.ExecClickHouse(user, password, record); err != nil {
			return applied, fmt.Errorf("record migration %s: %w", m.Name, err)
		}
		applied++
	}
	return applied, nil
}

// SetMigrationBaseline marks the initial migration as applied without running it.
func SetMigrationBaseline(docker *DockerOps, pgUser, pgDB, chUser, chPassword string) error {
	if _, err := docker.ExecPostgres(pgUser, pgDB, createMigrationsTablePG); err != nil {
		return fmt.Errorf("create pg migrations table: %w", err)
	}
	if _, err := docker.ExecPostgres(pgUser, pgDB,
		"INSERT INTO _bifract_migrations (number, name) VALUES (1, '001_initial') ON CONFLICT DO NOTHING;"); err != nil {
		return fmt.Errorf("set pg baseline: %w", err)
	}

	if _, err := docker.ExecClickHouse(chUser, chPassword, createMigrationsTableCH); err != nil {
		return fmt.Errorf("create ch migrations table: %w", err)
	}
	if _, err := docker.ExecClickHouse(chUser, chPassword,
		"INSERT INTO logs._bifract_migrations (number, name) VALUES (1, '001_initial');"); err != nil {
		return fmt.Errorf("set ch baseline: %w", err)
	}
	return nil
}

func parseMaxNumber(output string) int {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, "-") || strings.Contains(line, "coalesce") || strings.Contains(line, "max") {
			continue
		}
		n, err := strconv.Atoi(line)
		if err == nil {
			return n
		}
	}
	return 0
}

func splitStatements(sql string) []string {
	return strings.Split(sql, ";")
}
