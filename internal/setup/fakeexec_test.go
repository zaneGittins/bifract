package setup

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The installer drives docker and kubectl as subprocesses. Tests put a fake of
// each first (and alone) on PATH: a small shell script generated from canned
// rules, which logs every call. A test can then assert exactly what would have
// been sent to Docker or the cluster without either being touched.

// fakeRule answers a call whose arguments, joined by spaces, contain every Match
// substring. The first matching rule wins; a call no rule matches succeeds with
// no output.
type fakeRule struct {
	Cmd    string // "docker" or "kubectl"; empty matches either
	Match  []string
	Stdout []byte
	Exit   int
}

type fakeCall struct {
	Cmd   string
	Args  []string
	Stdin []byte
}

// joined is the argument list as one string, for substring assertions.
func (c fakeCall) joined() string { return strings.Join(c.Args, " ") }

// lastArg is the final argument: the SQL of an exec'd psql or clickhouse-client.
func (c fakeCall) lastArg() string {
	if len(c.Args) == 0 {
		return ""
	}
	return c.Args[len(c.Args)-1]
}

type fakeExec struct {
	t     *testing.T
	bin   string
	state string
}

// newFakeExec makes docker and kubectl the only commands on PATH, both answered
// by rules.
func newFakeExec(t *testing.T, rules ...fakeRule) *fakeExec {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the exec fake is a POSIX shell script")
	}
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("no /bin/sh")
	}
	f := &fakeExec{t: t, bin: t.TempDir(), state: t.TempDir()}
	f.setRules(rules...)
	t.Setenv("PATH", f.bin)
	return f
}

// withoutExternalCommands leaves PATH empty, so docker and kubectl are simply
// not installed as far as the code under test can tell.
func withoutExternalCommands(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// setRules (re)writes both fakes. Rules can change between calls, but not
// during one: the code under test sees whatever was current when it ran.
func (f *fakeExec) setRules(rules ...fakeRule) {
	f.t.Helper()
	for _, name := range []string{"docker", "kubectl"} {
		var b strings.Builder
		b.WriteString("#!/bin/sh\n")
		// The code under test sees only the fakes on PATH; the fake needs cat.
		b.WriteString("PATH=/usr/bin:/bin\n")
		b.WriteString("m() { case \"$1\" in *\"$2\"*) return 0;; esac; return 1; }\n")
		fmt.Fprintf(&b, "state=%s\n", shQuote(f.state))
		fmt.Fprintf(&b, "{ printf '%%s\\037%%s' %s \"$$\"; for a in \"$@\"; do printf '\\037%%s' \"$a\"; done; printf '\\036'; } >> \"$state/calls\"\n", name)
		b.WriteString("cat > \"$state/stdin.$$\"\n")
		b.WriteString("j=\"$*\"\n")
		for i, r := range rules {
			if r.Cmd != "" && r.Cmd != name {
				continue
			}
			out := filepath.Join(f.state, fmt.Sprintf("%s.%d.out", name, i))
			if err := os.WriteFile(out, r.Stdout, 0600); err != nil {
				f.t.Fatal(err)
			}
			cond := "true"
			for _, m := range r.Match {
				cond += " && m \"$j\" " + shQuote(m)
			}
			fmt.Fprintf(&b, "if %s; then cat %s; exit %d; fi\n", cond, shQuote(out), r.Exit)
		}
		b.WriteString("exit 0\n")
		path := filepath.Join(f.bin, name)
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, []byte(b.String()), 0700); err != nil {
			f.t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *fakeExec) calls() []fakeCall {
	f.t.Helper()
	data, err := os.ReadFile(filepath.Join(f.state, "calls"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		f.t.Fatal(err)
	}
	var out []fakeCall
	for _, rec := range strings.Split(string(data), "\x1e") {
		if rec == "" {
			continue
		}
		fields := strings.Split(rec, "\x1f")
		c := fakeCall{Cmd: fields[0], Args: fields[2:]}
		c.Stdin, _ = os.ReadFile(filepath.Join(f.state, "stdin."+fields[1]))
		out = append(out, c)
	}
	return out
}

// sql returns the statement of every call that ran SQL, in order.
func (f *fakeExec) sql() []string {
	var out []string
	for _, c := range f.calls() {
		j := c.joined()
		if strings.Contains(j, "clickhouse-client") || strings.Contains(j, "psql") {
			out = append(out, c.lastArg())
		}
	}
	return out
}
