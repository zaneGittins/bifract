package setup

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// backupTestKey is the only key these tests use: pkg/backup loads its key once
// per process, so a second key would be silently ignored.
const backupTestKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func writeBackupInstall(t *testing.T, extra string) string {
	t.Helper()
	// RunBackup exports the key into the process environment; restore it after.
	t.Setenv("BIFRACT_BACKUP_ENCRYPTION_KEY", "")
	dir := t.TempDir()
	env := "POSTGRES_USER=bf_user\nPOSTGRES_DB=bf_db\nPOSTGRES_PASSWORD=pg-secret-value\n" +
		"BIFRACT_BACKUP_ENCRYPTION_KEY=" + backupTestKey + "\n" + extra
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(env), 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// withStdin feeds input to code that prompts on os.Stdin.
func withStdin(t *testing.T, input string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	w.WriteString(input)
	w.Close()
	orig := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = orig; r.Close() })
}

var runningStack = fakeRule{Cmd: "docker", Match: []string{"compose ps --status running -q"}, Stdout: []byte("c0ffee\n")}

// The backup format (manifest, separator, zstd, AES-GCM chunks) is only ever
// read back by a later restore, possibly by a newer binary, so the pair is pinned
// end to end: what pg_dump produced is exactly what pg_restore receives.
func TestBackupRestoreRoundTrip(t *testing.T) {
	dir := writeBackupInstall(t, "")
	// Arbitrary bytes, including the payload separator itself: only the first
	// occurrence may be treated as the boundary.
	var dump bytes.Buffer
	dump.WriteString("PGDMP\x00\x01")
	for i := 0; i < 70000; i++ { // more than one 64 KiB encryption chunk
		dump.WriteByte(byte(i))
	}
	dump.WriteString("\n---BIFRACT_BACKUP_SEPARATOR---\ntrailing")

	fake := newFakeExec(t, runningStack,
		fakeRule{Cmd: "docker", Match: []string{"pg_dump"}, Stdout: dump.Bytes()})

	var err error
	captureStdout(t, func() { err = RunBackup(dir, true) })
	if err != nil {
		t.Fatalf("RunBackup: %v", err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "backups", "bifract-backup-*.bifract-backup"))
	if len(files) != 1 {
		t.Fatalf("found %d backup files, want 1", len(files))
	}
	stored, _ := os.ReadFile(files[0])
	if bytes.Contains(stored, []byte("pg-secret-value")) || bytes.Contains(stored, []byte("PGDMP")) {
		t.Error("backup file holds plaintext; it embeds the .env and the database")
	}
	var dumpCall *fakeCall
	for _, c := range fake.calls() {
		if strings.Contains(c.joined(), "pg_dump") {
			c := c
			dumpCall = &c
		}
	}
	if dumpCall == nil || dumpCall.joined() != "compose exec -T postgres pg_dump -U bf_user -d bf_db -Fc" {
		t.Errorf("pg_dump call = %+v", dumpCall)
	}

	out := captureStdout(t, func() { err = RunListBackups(dir) })
	if err != nil || !strings.Contains(out, filepath.Base(files[0])) {
		t.Errorf("RunListBackups did not list the backup (err %v):\n%s", err, out)
	}

	fake.setRules(runningStack)
	out = captureStdout(t, func() { err = RunRestore(dir, filepath.Base(files[0]), true) })
	if err != nil {
		t.Fatalf("RunRestore: %v", err)
	}
	var restore *fakeCall
	for _, c := range fake.calls() {
		if strings.Contains(c.joined(), "pg_restore") {
			c := c
			restore = &c
		}
	}
	if restore == nil {
		t.Fatal("pg_restore never ran")
	}
	if restore.joined() != "compose exec -T postgres pg_restore -U bf_user -d bf_db --clean --if-exists" {
		t.Errorf("pg_restore call = %s", restore.joined())
	}
	if !bytes.Equal(restore.Stdin, dump.Bytes()) {
		t.Errorf("pg_restore received %d bytes, want the %d pg_dump produced", len(restore.Stdin), dump.Len())
	}
	if !strings.Contains(out, "Backup version: "+Version) {
		t.Errorf("restore did not report the backup's manifest:\n%s", out)
	}
}

func TestRestoreRejectsTamperedBackup(t *testing.T) {
	dir := writeBackupInstall(t, "")
	fake := newFakeExec(t, runningStack, fakeRule{Cmd: "docker", Match: []string{"pg_dump"}, Stdout: []byte("dump")})
	var err error
	captureStdout(t, func() { err = RunBackup(dir, true) })
	if err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "backups", "*.bifract-backup"))
	data, _ := os.ReadFile(files[0])
	data[len(data)/2] ^= 0xff
	os.WriteFile(files[0], data, 0600)

	captureStdout(t, func() { err = RunRestore(dir, filepath.Base(files[0]), true) })
	if err == nil {
		t.Fatal("restored a tampered backup")
	}
	for _, c := range fake.calls() {
		if strings.Contains(c.joined(), "pg_restore") {
			t.Error("pg_restore ran on a backup that failed authentication")
		}
	}
}

func TestRestoreAsksBeforeReplacing(t *testing.T) {
	dir := writeBackupInstall(t, "")
	fake := newFakeExec(t, runningStack, fakeRule{Cmd: "docker", Match: []string{"pg_dump"}, Stdout: []byte("dump")})
	var err error
	captureStdout(t, func() { err = RunBackup(dir, true) })
	if err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "backups", "*.bifract-backup"))

	withStdin(t, "n\n")
	out := captureStdout(t, func() { err = RunRestore(dir, filepath.Base(files[0]), false) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "Restore cancelled") {
		t.Errorf("no cancellation reported:\n%s", out)
	}
	for _, c := range fake.calls() {
		if strings.Contains(c.joined(), "pg_restore") {
			t.Error("pg_restore ran without confirmation")
		}
	}
}

func TestBackupRefuses(t *testing.T) {
	t.Run("no key", func(t *testing.T) {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, ".env"), []byte("POSTGRES_PASSWORD=x\n"), 0600)
		fake := newFakeExec(t, runningStack)
		var err error
		captureStdout(t, func() { err = RunBackup(dir, true) })
		if err == nil || !strings.Contains(err.Error(), "BIFRACT_BACKUP_ENCRYPTION_KEY not found") {
			t.Errorf("backup err = %v", err)
		}
		captureStdout(t, func() { err = RunRestore(dir, "x.bifract-backup", true) })
		if err == nil || !strings.Contains(err.Error(), "BIFRACT_BACKUP_ENCRYPTION_KEY not found") {
			t.Errorf("restore err = %v", err)
		}
		if len(fake.calls()) != 0 {
			t.Error("touched docker without a key")
		}
	})
	t.Run("no install", func(t *testing.T) {
		var err error
		captureStdout(t, func() { err = RunBackup(t.TempDir(), true) })
		if err == nil || !strings.Contains(err.Error(), "read .env") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("stack stopped", func(t *testing.T) {
		dir := writeBackupInstall(t, "")
		fake := newFakeExec(t)
		var err error
		captureStdout(t, func() { err = RunBackup(dir, true) })
		if err == nil || !strings.Contains(err.Error(), "not running") {
			t.Errorf("backup err = %v", err)
		}
		captureStdout(t, func() { err = RunRestore(dir, "x.bifract-backup", true) })
		if err == nil || !strings.Contains(err.Error(), "not running") {
			t.Errorf("restore err = %v", err)
		}
		for _, c := range fake.calls() {
			if strings.Contains(c.joined(), "pg_") {
				t.Errorf("ran %s against a stopped stack", c.joined())
			}
		}
	})
	t.Run("missing backup file", func(t *testing.T) {
		dir := writeBackupInstall(t, "")
		newFakeExec(t, runningStack)
		var err error
		captureStdout(t, func() { err = RunRestore(dir, "nope.bifract-backup", true) })
		if err == nil || !strings.Contains(err.Error(), "read backup") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("path traversal", func(t *testing.T) {
		dir := writeBackupInstall(t, "")
		newFakeExec(t, runningStack)
		var err error
		captureStdout(t, func() { err = RunRestore(dir, "../.env", true) })
		if err == nil {
			t.Error("restore read a file outside the backups directory")
		}
	})
}

func TestListBackupsEmpty(t *testing.T) {
	var err error
	out := captureStdout(t, func() { err = RunListBackups(t.TempDir()) })
	if err != nil || !strings.Contains(out, "No backups found") {
		t.Errorf("err = %v, out:\n%s", err, out)
	}
}
