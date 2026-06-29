package bqlvars

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSubstitute(t *testing.T) {
	vars := json.RawMessage(`[
		{"name":"host","value":"web01"},
		{"name":"hostname","value":"db-primary"},
		{"name":"empty","value":""},
		{"name":"user","value":"alice"}
	]`)

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"simple value", "status=500 host=@host", "status=500 host=web01"},
		{"empty becomes wildcard", "field=@empty", "field=*"},
		{"repeated", "@host and @host again", "web01 and web01 again"},
		// The whole point of the rewrite: @host must not corrupt @hostname.
		{"no prefix collision", "a=@host b=@hostname", "a=web01 b=db-primary"},
		// Quoted text is opaque: an email inside quotes is never touched even
		// though it contains @user.
		{"quoted email untouched", `msg="mail from user@host" user=@user`, `msg="mail from user@host" user=alice`},
		// Unquoted, '@' preceded by a word char is not a variable.
		{"email-like not preceded variable", "from=bob@host", "from=bob@host"},
		// Unknown name is left intact so typos still error downstream.
		{"unknown left intact", "x=@nope", "x=@nope"},
		{"single quotes opaque", `f='a@user b' g=@user`, `f='a@user b' g=alice`},
		{"escaped quote inside string", `f="a\"@user" g=@user`, `f="a\"@user" g=alice`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Substitute(c.in, vars); got != c.want {
				t.Errorf("Substitute(%q)\n got: %q\nwant: %q", c.in, got, c.want)
			}
		})
	}
}

func TestSubstituteNoVars(t *testing.T) {
	if got := Substitute("host=@host", nil); got != "host=@host" {
		t.Errorf("expected unchanged query with nil vars, got %q", got)
	}
	if got := Substitute("plain query", json.RawMessage(`[{"name":"x","value":"y"}]`)); got != "plain query" {
		t.Errorf("expected unchanged query with no @ tokens, got %q", got)
	}
}

func TestDetect(t *testing.T) {
	got := Detect(`a=@host b=@user c=@host "ignore @inside" d=@svc bob@nope`)
	want := []string{"host", "user", "svc"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Detect mismatch\n got: %v\nwant: %v", got, want)
	}
}
