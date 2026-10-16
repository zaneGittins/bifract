// Package clientip resolves the address of the client behind a request, for
// rate limiting and audit records.
//
// Forwarding headers (X-Forwarded-For, X-Real-IP) are honored only when the
// direct peer is a trusted proxy. Anyone can send those headers, so trusting
// them from an arbitrary peer would let a client pick its own address and step
// around every per-address limit.
package clientip

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync/atomic"
)

// MaxTrustedProxies bounds the configured list. Real deployments need a
// handful of ranges; the bound keeps a pasted mistake from making every request
// walk a huge list.
const MaxTrustedProxies = 256

// DefaultTrustedProxies covers where a reverse proxy in front of Bifract sits
// in a standard deployment: the bundled Caddy on the docker network, a
// Kubernetes ingress on the pod network, or a proxy on the same host. None of
// these ranges is routable from the internet, so a client there is already
// inside the network.
var DefaultTrustedProxies = []string{
	"127.0.0.0/8",
	"::1/128",
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"fc00::/7",
	"fe80::/10",
}

var trusted atomic.Pointer[[]netip.Prefix]

func init() {
	p, err := Parse(DefaultTrustedProxies)
	if err != nil {
		panic(err)
	}
	trusted.Store(&p)
}

// Parse validates a list of CIDR ranges or bare addresses (taken as a single
// host) and returns them as prefixes. Blank entries are skipped.
func Parse(entries []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(entries))
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		var p netip.Prefix
		if strings.Contains(e, "/") {
			parsed, err := netip.ParsePrefix(e)
			if err != nil {
				return nil, fmt.Errorf("%q is not a CIDR range or IP address", e)
			}
			p = parsed
		} else {
			a, err := netip.ParseAddr(e)
			if err != nil || a.Zone() != "" {
				return nil, fmt.Errorf("%q is not a CIDR range or IP address", e)
			}
			p = netip.PrefixFrom(a, a.BitLen())
		}
		if p.Addr().Is4In6() {
			// ::ffff:10.0.0.0/104 and 10.0.0.0/8 name the same hosts; peers
			// are unmapped before matching, so store the IPv4 form.
			if p.Bits() < 96 {
				return nil, fmt.Errorf("%q is not a CIDR range or IP address", e)
			}
			p = netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96)
		}
		out = append(out, p.Masked())
		if len(out) > MaxTrustedProxies {
			return nil, fmt.Errorf("at most %d trusted proxy ranges are allowed", MaxTrustedProxies)
		}
	}
	return out, nil
}

// Normalize validates entries and returns them in canonical form, ready to
// store and display.
func Normalize(entries []string) ([]string, error) {
	p, err := Parse(entries)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(p))
	for i, pr := range p {
		out[i] = pr.String()
	}
	return out, nil
}

// SetTrustedProxies replaces the trusted proxy list. It takes effect for the
// next request.
func SetTrustedProxies(entries []string) error {
	p, err := Parse(entries)
	if err != nil {
		return err
	}
	trusted.Store(&p)
	return nil
}

// TrustedProxies returns the list in force, in canonical form.
func TrustedProxies() []string {
	p := *trusted.Load()
	out := make([]string, len(p))
	for i, pr := range p {
		out[i] = pr.String()
	}
	return out
}

// FromRequest returns the client address for r using the trusted proxy list
// in force.
func FromRequest(r *http.Request) string {
	return resolve(r, *trusted.Load())
}

func isTrusted(a netip.Addr, proxies []netip.Prefix) bool {
	for _, p := range proxies {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// parseHop reads one address from a forwarding header or RemoteAddr: a bare
// address, an address with a port, or a bracketed IPv6 address. The zone is
// dropped (a prefix never contains a zoned address) and IPv4-mapped IPv6 is
// unmapped so it matches IPv4 ranges.
func parseHop(s string) (netip.Addr, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return netip.Addr{}, false
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		host, _, splitErr := net.SplitHostPort(s)
		if splitErr != nil {
			host = strings.TrimSuffix(strings.TrimPrefix(s, "["), "]")
		}
		if a, err = netip.ParseAddr(host); err != nil {
			return netip.Addr{}, false
		}
	}
	return a.WithZone("").Unmap(), true
}

func resolve(r *http.Request, proxies []netip.Prefix) string {
	peer, ok := parseHop(r.RemoteAddr)
	if !ok {
		// Not an address (a test harness or a unix socket). Use it as is,
		// minus any port, so it still keys a limiter consistently.
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			return host
		}
		return r.RemoteAddr
	}
	if !isTrusted(peer, proxies) {
		return peer.String()
	}

	// Each proxy appends the address it received the request from, so the
	// list is read from the right. The first address that is not itself a
	// trusted proxy is the client; everything to its left was supplied by
	// that client and proves nothing.
	if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
		hops := strings.Split(strings.Join(xff, ","), ",")
		client := peer
		for i := len(hops) - 1; i >= 0; i-- {
			a, ok := parseHop(hops[i])
			if !ok {
				// A malformed hop ends what can be trusted; the last
				// good address to its right is the best known.
				break
			}
			client = a
			if !isTrusted(a, proxies) {
				break
			}
		}
		return client.String()
	}

	if a, ok := parseHop(r.Header.Get("X-Real-IP")); ok {
		return a.String()
	}
	return peer.String()
}
