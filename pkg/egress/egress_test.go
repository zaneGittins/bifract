package egress

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestAllowed(t *testing.T) {
	cases := []struct {
		ip                  string
		public, withPrivate bool
	}{
		{"93.184.216.34", true, true},
		{"2606:2800:220:1::1", true, true},
		{"127.0.0.1", false, true},
		{"::1", false, true},
		{"10.1.2.3", false, true},
		{"192.168.0.1", false, true},
		{"172.16.5.4", false, true},
		{"100.64.0.1", false, true},
		{"fd12::1", false, true},
		{"::ffff:127.0.0.1", false, true},
		// Metadata endpoints stay blocked even when private space is allowed.
		{"169.254.169.254", false, false},
		{"fe80::1", false, false},
		{"fd00:ec2::254", false, false},
		{"100.100.100.200", false, false},
		{"0.0.0.0", false, false},
		{"0.1.2.3", false, false},
		{"::", false, false},
		{"255.255.255.255", false, false},
		{"224.0.0.1", false, false},
		{"168.63.129.16", false, false},
		{"240.0.0.1", false, false},
		{"192.0.0.8", false, true},
		{"198.18.0.1", false, true},
		{"fec0::1", false, true},
		// IPv6 forms carrying an IPv4 address: NAT64 is judged by the address it
		// embeds, the rest are refused.
		{"64:ff9b::5db8:d822", true, true},
		{"64:ff9b::a00:1", false, true},
		{"64:ff9b::a9fe:a9fe", false, false},
		{"64:ff9b:1::a00:1", false, true},
		{"2002:7f00:1::1", false, false},
		{"2001:0:4136:e378::1", false, false},
		{"::127.0.0.1", false, false},
	}
	for _, c := range cases {
		ip := netip.MustParseAddr(c.ip)
		if got := Allowed(ip, false); got != c.public {
			t.Errorf("Allowed(%s, false) = %v, want %v", c.ip, got, c.public)
		}
		if got := Allowed(ip, true); got != c.withPrivate {
			t.Errorf("Allowed(%s, true) = %v, want %v", c.ip, got, c.withPrivate)
		}
	}
}

func TestValidateURL(t *testing.T) {
	for _, bad := range []string{"", "file:///etc/passwd", "gopher://x", "ftp://example.com/a", "https://", "https://user:pass@example.com/a"} {
		if ValidateURL(bad) == nil {
			t.Errorf("ValidateURL(%q) accepted", bad)
		}
	}
	if err := ValidateURL("https://raw.githubusercontent.com/a/b.csv"); err != nil {
		t.Errorf("rejected a plain https URL: %v", err)
	}
}

// The check runs on the address actually dialed, so a loopback server is refused
// unless private space is allowed.
func TestClientRefusesLoopbackUnlessAllowed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	ctx := context.Background()

	_, err := Get(ctx, NewClient(Options{Timeout: 5 * time.Second}), srv.URL)
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("loopback fetch: got %v, want ErrBlocked", err)
	}

	resp, err := Get(ctx, NewClient(Options{AllowPrivate: true, Timeout: 5 * time.Second}), srv.URL)
	if err != nil {
		t.Fatalf("loopback fetch with private allowed: %v", err)
	}
	resp.Body.Close()
}

func TestRedirectToAnotherSchemeIsRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "file:///etc/passwd", http.StatusFound)
	}))
	defer srv.Close()
	if _, err := Get(context.Background(), NewClient(Options{AllowPrivate: true, Timeout: 5 * time.Second}), srv.URL); err == nil {
		t.Fatal("followed a redirect to a file URL")
	}
}

func TestNonSuccessStatusFails(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	_, err := Get(context.Background(), NewClient(Options{AllowPrivate: true, Timeout: 5 * time.Second}), srv.URL)
	if Describe(err) != "the server answered 404 Not Found" {
		t.Fatalf("a 404 came back as %v", err)
	}
}

// A public name that redirects to an internal address is refused at the
// redirect's own connection.
func TestRedirectToPrivateIsRefused(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("secret"))
	}))
	defer target.Close()
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer front.Close()

	client := NewClient(Options{Timeout: 5 * time.Second})
	transport := client.Transport.(*http.Transport)
	checked := transport.DialContext
	// Stands in for a public host: the first hop is let through, the redirect is
	// dialed with the usual check.
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr == "public.example:80" {
			return (&net.Dialer{}).DialContext(ctx, network, front.Listener.Addr().String())
		}
		return checked(ctx, network, addr)
	}
	_, err := Get(context.Background(), client, "http://public.example/")
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("redirect to loopback: got %v, want ErrBlocked", err)
	}
	if msg := Describe(err); msg != "the host resolves to an address that is not allowed" {
		t.Errorf("Describe(%v) = %q", err, msg)
	}
}

func TestDowngradeRedirectIsRefused(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.com/", http.StatusFound)
	}))
	defer srv.Close()
	client := NewClient(Options{AllowPrivate: true, Timeout: 5 * time.Second})
	client.Transport.(*http.Transport).TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig
	if _, err := Get(context.Background(), client, srv.URL); !errors.Is(err, ErrRedirect) {
		t.Fatalf("https to http redirect: got %v, want ErrRedirect", err)
	}
}
