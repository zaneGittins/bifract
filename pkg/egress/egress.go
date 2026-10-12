// Package egress makes outbound HTTP requests to addresses a user supplied,
// without letting that address reach the instance's own network.
package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// Options tunes a Client.
type Options struct {
	// AllowPrivate admits private, loopback and shared address space, for a
	// source hosted inside the deployment's own network. Cloud metadata
	// endpoints stay blocked regardless.
	AllowPrivate bool
	// Timeout bounds a whole request, redirects and body included.
	Timeout time.Duration
}

// maxRedirects bounds how far a request may be sent on.
const maxRedirects = 5

var (
	// ErrBlocked marks a request refused because of where it would connect.
	ErrBlocked = errors.New("destination address is not allowed")
	// ErrRedirect marks a redirect the client refused to follow.
	ErrRedirect = errors.New("redirect not followed")
)

// StatusError is a response outside 2xx.
type StatusError struct{ Status string }

func (e StatusError) Error() string { return "the server answered " + e.Status }

// alwaysBlocked are refused even when private space is allowed: cloud metadata
// and platform endpoints outside link-local space (which is refused anyway),
// reserved space, and IPv6 forms that carry an IPv4 address this check would
// otherwise never see.
var alwaysBlocked = []netip.Prefix{
	netip.MustParsePrefix("fd00:ec2::254/128"),  // AWS metadata, IPv6
	netip.MustParsePrefix("100.100.100.200/32"), // Alibaba Cloud metadata
	netip.MustParsePrefix("168.63.129.16/32"),   // Azure WireServer
	netip.MustParsePrefix("240.0.0.0/4"),        // reserved
	netip.MustParsePrefix("2002::/16"),          // 6to4
	netip.MustParsePrefix("2001::/32"),          // Teredo
	netip.MustParsePrefix("::/96"),              // IPv4-compatible
}

// nonPublic is address space that is not on the internet but that
// net/netip does not count as private.
var nonPublic = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),  // IETF protocol assignments
	netip.MustParsePrefix("198.18.0.0/15"), // benchmarking
	netip.MustParsePrefix("fec0::/10"),     // deprecated site-local
}

// nat64 prefixes embed an IPv4 destination in their last 32 bits. On an
// IPv6-only network every public IPv4 address is reached through one, so the
// embedded address is checked rather than the prefix refused.
var nat64 = []netip.Prefix{
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
}

// NewClient returns a client that checks every address it connects to, after
// DNS resolution and on every redirect, so a name that resolves somewhere
// internal is refused at the moment of connecting rather than when the URL was
// checked. Proxies from the environment are not used: a proxy would be the only
// address the check ever saw.
func NewClient(opts Options) *http.Client {
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
		Control: func(network, address string, _ syscall.RawConn) error {
			return checkAddress(address, opts.AllowPrivate)
		},
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		// Fetches are minutes apart, so a kept-alive connection is only a leak.
		DisableKeepAlives: true,
	}
	return &http.Client{
		Timeout:   opts.Timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return fmt.Errorf("%w: more than %d redirects", ErrRedirect, maxRedirects)
			}
			if via[0].URL.Scheme == "https" && req.URL.Scheme != "https" {
				return fmt.Errorf("%w: https redirected to plain http", ErrRedirect)
			}
			if err := ValidateURL(req.URL.String()); err != nil {
				return fmt.Errorf("%w: %v", ErrRedirect, err)
			}
			return nil
		},
	}
}

// ValidateURL rejects a URL that is not plain http(s) to a named host. It is the
// shape check; where the host resolves to is checked when connecting.
func ValidateURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("not a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("only http and https URLs are allowed")
	}
	if u.Hostname() == "" {
		return fmt.Errorf("the URL has no host")
	}
	// Credentials in a URL would be stored and shown wherever the URL is.
	if u.User != nil {
		return fmt.Errorf("the URL must not carry a username or password")
	}
	return nil
}

// checkAddress refuses a resolved ip:port that is not a public unicast address.
func checkAddress(address string, allowPrivate bool) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrBlocked, address)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrBlocked, host)
	}
	if !Allowed(ip, allowPrivate) {
		return fmt.Errorf("%w: %s", ErrBlocked, ip)
	}
	return nil
}

// Allowed reports whether a connection to ip may be made.
func Allowed(ip netip.Addr, allowPrivate bool) bool {
	ip = ip.Unmap()
	for _, p := range nat64 {
		if p.Contains(ip) {
			b := ip.As16()
			ip = netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
			break
		}
	}
	if !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() {
		return false
	}
	if ip.Is4() && ip.As4()[0] == 0 || ip == netip.AddrFrom4([4]byte{255, 255, 255, 255}) {
		return false
	}
	if ip.IsLoopback() {
		return allowPrivate
	}
	for _, p := range alwaysBlocked {
		if p.Contains(ip) {
			return false
		}
	}
	if allowPrivate {
		return true
	}
	if ip.IsPrivate() {
		return false
	}
	for _, p := range nonPublic {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// Get fetches rawURL with the given client, failing with a StatusError on a
// response outside 2xx.
func Get(ctx context.Context, client *http.Client, rawURL string) (*http.Response, error) {
	if err := ValidateURL(rawURL); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSpace(rawURL), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Bifract")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		resp.Body.Close()
		return nil, StatusError{Status: resp.Status}
	}
	return resp, nil
}

// Describe says why a request failed in terms safe to show anyone who can see
// the URL: never the address a name resolved to, a redirect's target, or text
// from the far end.
func Describe(err error) string {
	var status StatusError
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &status):
		return status.Error()
	case errors.Is(err, ErrBlocked):
		return "the host resolves to an address that is not allowed"
	case errors.Is(err, ErrRedirect):
		return "the server redirected somewhere that is not allowed"
	case errors.As(err, &dnsErr):
		return "the host name could not be resolved"
	case errors.Is(err, context.DeadlineExceeded), isTimeout(err):
		return "the server did not answer in time"
	}
	return "the server could not be reached"
}

func isTimeout(err error) bool {
	var t interface{ Timeout() bool }
	return errors.As(err, &t) && t.Timeout()
}
