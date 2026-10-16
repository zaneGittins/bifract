package clientip

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func request(remote string, headers map[string][]string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = remote
	for k, vs := range headers {
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}
	return r
}

func xff(v ...string) map[string][]string { return map[string][]string{"X-Forwarded-For": v} }

func TestFromRequestDefaults(t *testing.T) {
	tests := []struct {
		name    string
		remote  string
		headers map[string][]string
		want    string
	}{
		{name: "ipv4 peer", remote: "203.0.113.7:51234", want: "203.0.113.7"},
		{name: "peer without port", remote: "203.0.113.7", want: "203.0.113.7"},
		{name: "ipv6 peer", remote: "[2001:db8::1]:443", want: "2001:db8::1"},
		{name: "ipv6 loopback peer", remote: "[::1]:8080", want: "::1"},
		{name: "ipv4-mapped peer", remote: "[::ffff:203.0.113.7]:1", want: "203.0.113.7"},
		{name: "not an address", remote: "pipe", want: "pipe"},

		// An untrusted peer cannot choose its own address.
		{name: "untrusted peer, forwarded for ignored", remote: "203.0.113.7:1", headers: xff("198.51.100.4"), want: "203.0.113.7"},
		{name: "untrusted peer, real ip ignored", remote: "203.0.113.7:1", headers: map[string][]string{"X-Real-IP": {"198.51.100.9"}}, want: "203.0.113.7"},
		{name: "untrusted peer claiming loopback", remote: "203.0.113.7:1", headers: xff("127.0.0.1"), want: "203.0.113.7"},

		// Behind the bundled proxy, which overwrites the header.
		{name: "docker proxy", remote: "172.18.0.5:1", headers: xff("198.51.100.4"), want: "198.51.100.4"},
		{name: "ipv6 client via proxy", remote: "172.18.0.5:1", headers: xff("2001:db8::7"), want: "2001:db8::7"},
		// A proxy that appends: the client's own left-hand entries are not
		// believed, only what the trusted hop recorded.
		{name: "spoofed leftmost entry", remote: "10.0.0.2:1", headers: xff("1.2.3.4, 198.51.100.4"), want: "198.51.100.4"},
		{name: "chain of trusted proxies", remote: "10.0.0.2:1", headers: xff("198.51.100.4, 10.0.0.9, 192.168.1.1"), want: "198.51.100.4"},
		{name: "repeated header lines", remote: "10.0.0.2:1", headers: xff("1.2.3.4", "198.51.100.4, 10.0.0.9"), want: "198.51.100.4"},
		{name: "internal client via proxy", remote: "10.0.0.2:1", headers: xff("192.168.4.4"), want: "192.168.4.4"},
		{name: "hop with port", remote: "10.0.0.2:1", headers: xff("198.51.100.4:5555"), want: "198.51.100.4"},
		{name: "bracketed ipv6 hop", remote: "10.0.0.2:1", headers: xff("[2001:db8::7]:5555"), want: "2001:db8::7"},
		{name: "malformed hop stops the walk", remote: "10.0.0.2:1", headers: xff("198.51.100.4, garbage, 10.0.0.9"), want: "10.0.0.9"},
		{name: "empty hop stops the walk", remote: "10.0.0.2:1", headers: xff(" ,10.0.0.1"), want: "10.0.0.1"},
		{name: "real ip from trusted peer", remote: "10.0.0.2:1", headers: map[string][]string{"X-Real-IP": {" 198.51.100.9 "}}, want: "198.51.100.9"},
		{name: "forwarded for wins over real ip", remote: "10.0.0.2:1", headers: map[string][]string{"X-Forwarded-For": {"198.51.100.4"}, "X-Real-IP": {"198.51.100.9"}}, want: "198.51.100.4"},
		{name: "unparseable real ip", remote: "10.0.0.2:1", headers: map[string][]string{"X-Real-IP": {"nope"}}, want: "10.0.0.2"},
		{name: "link-local zoned peer", remote: "[fe80::1%eth0]:1", headers: xff("198.51.100.4"), want: "198.51.100.4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromRequest(request(tt.remote, tt.headers)); got != tt.want {
				t.Errorf("FromRequest = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSetTrustedProxies(t *testing.T) {
	t.Cleanup(func() {
		if err := SetTrustedProxies(DefaultTrustedProxies); err != nil {
			t.Fatal(err)
		}
	})

	// Trust only the edge proxy: a docker-range peer is now an ordinary client.
	if err := SetTrustedProxies([]string{"203.0.113.10"}); err != nil {
		t.Fatal(err)
	}
	if got := FromRequest(request("172.18.0.5:1", xff("198.51.100.4"))); got != "172.18.0.5" {
		t.Errorf("untrusted docker peer: %q", got)
	}
	if got := FromRequest(request("203.0.113.10:1", xff("198.51.100.4"))); got != "198.51.100.4" {
		t.Errorf("trusted edge proxy: %q", got)
	}
	// With the private ranges no longer trusted, a private hop is the client.
	if got := FromRequest(request("203.0.113.10:1", xff("198.51.100.4, 10.0.0.9"))); got != "10.0.0.9" {
		t.Errorf("untrusted private hop: %q", got)
	}

	// An empty list trusts nobody.
	if err := SetTrustedProxies(nil); err != nil {
		t.Fatal(err)
	}
	if got := FromRequest(request("127.0.0.1:1", xff("198.51.100.4"))); got != "127.0.0.1" {
		t.Errorf("empty list honored a header: %q", got)
	}

	// A bad list is refused and leaves the one in force alone.
	if err := SetTrustedProxies([]string{"10.0.0.0/8", "not-a-range"}); err == nil {
		t.Fatal("bad list accepted")
	}
	if got := TrustedProxies(); len(got) != 0 {
		t.Errorf("bad list replaced the one in force: %v", got)
	}
}

func TestNormalize(t *testing.T) {
	got, err := Normalize([]string{" 10.1.2.3/8 ", "", "203.0.113.5", "2001:db8::1", "::ffff:192.168.0.0/112", "fd00::/8"})
	if err != nil {
		t.Fatal(err)
	}
	want := "10.0.0.0/8 203.0.113.5/32 2001:db8::1/128 192.168.0.0/16 fd00::/8"
	if strings.Join(got, " ") != want {
		t.Errorf("Normalize = %v, want %s", got, want)
	}

	for _, bad := range []string{"10.0.0.0/33", "300.1.1.1", "example.com", "10.0.0.0/", "fe80::1%eth0", "::ffff:0.0.0.0/80"} {
		if _, err := Normalize([]string{bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}

	many := make([]string, MaxTrustedProxies+1)
	for i := range many {
		many[i] = "10.0.0.1"
	}
	if _, err := Normalize(many); err == nil {
		t.Error("over-long list accepted")
	}
}

func TestDefaultsParse(t *testing.T) {
	if got := TrustedProxies(); strings.Join(got, ",") != strings.Join(DefaultTrustedProxies, ",") {
		t.Errorf("TrustedProxies = %v, want the defaults %v", got, DefaultTrustedProxies)
	}
}
