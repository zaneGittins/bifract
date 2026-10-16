package setup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func composeDir(t *testing.T, env string) string {
	t.Helper()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "docker-compose.yml"), []byte("services: {}\n"), 0644)
	if env != "" {
		os.WriteFile(filepath.Join(dir, ".env"), []byte(env), 0600)
	}
	return dir
}

func dockerCalls(f *fakeExec) string {
	var out []string
	for _, c := range f.calls() {
		out = append(out, c.joined())
	}
	return strings.Join(out, "\n")
}

func TestRunStartStop(t *testing.T) {
	t.Run("start when running is a no-op", func(t *testing.T) {
		fake := newFakeExec(t, runningStack)
		var err error
		out := captureStdout(t, func() { err = RunStart(composeDir(t, "")) })
		if err != nil || !strings.Contains(out, "already running") || strings.Contains(dockerCalls(fake), "compose up") {
			t.Errorf("err %v, calls:\n%s", err, dockerCalls(fake))
		}
	})
	t.Run("start reports compose output on failure", func(t *testing.T) {
		newFakeExec(t, fakeRule{Match: []string{"compose up -d"}, Stdout: []byte("port 443 already allocated\n"), Exit: 1})
		var err error
		captureStdout(t, func() { err = RunStart(composeDir(t, "")) })
		if err == nil || !strings.Contains(err.Error(), "port 443 already allocated") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("stop when running", func(t *testing.T) {
		fake := newFakeExec(t, runningStack)
		var err error
		captureStdout(t, func() { err = RunStop(composeDir(t, "")) })
		if err != nil || !strings.Contains(dockerCalls(fake), "compose down") {
			t.Errorf("err %v, calls:\n%s", err, dockerCalls(fake))
		}
	})
	t.Run("stop when stopped is a no-op", func(t *testing.T) {
		fake := newFakeExec(t)
		var err error
		out := captureStdout(t, func() { err = RunStop(composeDir(t, "")) })
		if err != nil || !strings.Contains(out, "already stopped") || strings.Contains(dockerCalls(fake), "compose down") {
			t.Errorf("err %v, calls:\n%s", err, dockerCalls(fake))
		}
	})
	t.Run("stop failure", func(t *testing.T) {
		newFakeExec(t, runningStack, fakeRule{Match: []string{"compose down"}, Stdout: []byte("denied"), Exit: 1})
		var err error
		captureStdout(t, func() { err = RunStop(composeDir(t, "")) })
		if err == nil || !strings.Contains(err.Error(), "denied") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("no install", func(t *testing.T) {
		withoutExternalCommands(t)
		for name, run := range map[string]func(string) error{"start": RunStart, "stop": RunStop, "status": RunStatus} {
			var err error
			captureStdout(t, func() { err = run(t.TempDir()) })
			if err == nil || !strings.Contains(err.Error(), "no installation found") {
				t.Errorf("%s: err = %v", name, err)
			}
		}
	})
}

func TestRunStatus(t *testing.T) {
	tests := []struct {
		name   string
		rules  []fakeRule
		want   string
		absent string
	}{
		{"healthy", []fakeRule{runningStack, {Match: []string{"inspect"}, Stdout: []byte("healthy\n")}}, "Bifract is healthy", ""},
		{"starting", []fakeRule{runningStack, {Match: []string{"inspect"}, Stdout: []byte("starting\n")}}, "Bifract is starting", ""},
		{"unhealthy", []fakeRule{runningStack, {Match: []string{"inspect"}, Stdout: []byte("unhealthy\n")}}, "Bifract is unhealthy", ""},
		{"inspect fails", []fakeRule{runningStack, {Match: []string{"inspect"}, Exit: 1}}, "Could not determine health", ""},
		{"stopped", nil, "Bifract is not running", "inspect"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeExec(t, tt.rules...)
			var err error
			out := captureStdout(t, func() { err = RunStatus(composeDir(t, "BIFRACT_VERSION=v1.2.3\n")) })
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out, tt.want) || !strings.Contains(out, "v1.2.3") {
				t.Errorf("output lacks %q or the version:\n%s", tt.want, out)
			}
			if tt.absent != "" && strings.Contains(dockerCalls(fake), tt.absent) {
				t.Errorf("ran %q:\n%s", tt.absent, dockerCalls(fake))
			}
		})
	}
}

func TestCheckPrereqs(t *testing.T) {
	newFakeExec(t,
		fakeRule{Match: []string{"--version"}, Stdout: []byte("Docker version 27.0.1\n")},
		fakeRule{Match: []string{"compose version"}, Stdout: []byte("Docker Compose version v2.29.0\n"), Exit: 1},
	)
	r := CheckPrereqs()
	if !r.DockerOK || r.DockerVer != "Docker version 27.0.1" || r.ComposeOK || r.OK() {
		t.Errorf("prereqs = %+v", r)
	}
	if s := r.Summary(); !strings.Contains(s, "Docker version 27.0.1") || !strings.Contains(s, "Docker Compose not found") {
		t.Errorf("summary = %q", s)
	}
}

func TestCheckImageAvailable(t *testing.T) {
	fake := newFakeExec(t, fakeRule{Match: []string{"manifest inspect", ":v9.9.9"}, Stdout: []byte("manifest unknown\n"), Exit: 1})
	if err := CheckImageAvailable(BifractImage("v1.0.0")); err != nil {
		t.Errorf("published image reported missing: %v", err)
	}
	if err := CheckImageAvailable(BifractImage("v9.9.9")); err == nil || !strings.Contains(err.Error(), "manifest unknown") {
		t.Errorf("err = %v", err)
	}
	if c := fake.calls(); c[0].joined() != "manifest inspect ghcr.io/zanegittins/bifract:v1.0.0" {
		t.Errorf("call = %s", c[0].joined())
	}
}
