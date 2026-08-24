// Package deeplink serves stable, hand-constructible entry URLs into the web UI.
//
// External tools (EDR rules, SOAR playbooks, alert notifications, MCP replies)
// need to hand an analyst a link that runs a specific BQL query. The SPA's own
// share links carry base64(encodeURIComponent(bql)) plus an internal time-range
// vocabulary, which almost nothing outside the browser can produce. This package
// accepts a plain, documented query string, validates and resolves it against the
// caller's access, and redirects into the SPA's internal share form. The external
// contract is therefore stable even if the internal one changes.
package deeplink

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/auth"
	"bifract/pkg/fractals"
	"bifract/pkg/prisms"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

const (
	// maxQueryLen bounds the BQL a link may carry. Proxies and browsers truncate
	// long URLs well before this; anything larger belongs in a saved query.
	maxQueryLen = 4000
	maxVarLen   = 512
	maxVars     = 32
)

var (
	varNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	// relativeRe matches "-24h" and "now-24h". A single magnitude and unit only:
	// compound offsets have no representation in the time picker.
	relativeRe = regexp.MustCompile(`^(?:now)?-(\d+)([smhdw])$`)
)

// Handler resolves deep links for the browser.
type Handler struct {
	fractals *fractals.Manager
	prisms   *prisms.Manager
	auth     *auth.AuthHandler
	rbac     *rbac.Resolver
}

func NewHandler(f *fractals.Manager, p *prisms.Manager, a *auth.AuthHandler, r *rbac.Resolver) *Handler {
	return &Handler{fractals: f, prisms: p, auth: a, rbac: r}
}

// scope is the resolved fractal or prism a link targets.
type scope struct {
	id      string
	isPrism bool
}

// HandleSearch answers GET /go/search.
//
//	q       BQL, percent-encoded (or q64, base64/base64url)
//	fractal fractal name or id (or prism, for a prism); optional
//	from    -24h | now-24h | RFC3339 | epoch | all   (default -24h)
//	to      now | RFC3339 | epoch                    (default now)
//	var.X   value bound to @X in the query
func (h *Handler) HandleSearch(w http.ResponseWriter, r *http.Request) {
	// The link carries a query in its URL, so keep it out of caches and out of
	// the Referer header of whatever the SPA loads next.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")

	user := h.auth.SessionUser(r)
	if user == nil {
		// Bounce through login and come back, so an expired session costs the
		// analyst a password and not the link.
		http.Redirect(w, r, "/login.html?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusFound)
		return
	}

	params := r.URL.Query()

	bql, err := readQuery(params)
	if err != nil {
		h.renderError(w, http.StatusBadRequest, "Invalid link", err.Error())
		return
	}

	fractalRef, prismRef := strings.TrimSpace(params.Get("fractal")), strings.TrimSpace(params.Get("prism"))
	if fractalRef != "" && prismRef != "" {
		h.renderError(w, http.StatusBadRequest, "Invalid link", "Pass either fractal or prism, not both.")
		return
	}

	target, err := h.resolveScope(r.Context(), user, fractalRef, prismRef)
	if err != nil {
		h.renderError(w, http.StatusNotFound, "Unavailable", err.Error())
		return
	}

	sel, err := parseTimeRange(params.Get("from"), params.Get("to"), time.Now().UTC())
	if err != nil {
		h.renderError(w, http.StatusBadRequest, "Invalid time range", err.Error())
		return
	}

	vars, err := encodeVars(params)
	if err != nil {
		h.renderError(w, http.StatusBadRequest, "Invalid variables", err.Error())
		return
	}

	http.Redirect(w, r, buildShareURL(bql, target, sel, vars), http.StatusFound)
}

// readQuery pulls the BQL out of either q (plain) or q64 (base64). The two are
// separate parameters rather than one sniffed value: guessing the encoding of a
// string that is itself valid base64 is how deep links silently run the wrong
// query.
func readQuery(params url.Values) (string, error) {
	plain := strings.TrimSpace(params.Get("q"))
	encoded := strings.TrimSpace(params.Get("q64"))

	switch {
	case plain != "" && encoded != "":
		return "", fmt.Errorf("pass either q or q64, not both")
	case plain == "" && encoded == "":
		return "", fmt.Errorf("missing q: pass the BQL query as ?q=<url-encoded query>")
	case encoded != "":
		raw, err := decodeBase64Any(encoded)
		if err != nil {
			return "", fmt.Errorf("q64 is not valid base64")
		}
		plain = strings.TrimSpace(string(raw))
		if plain == "" {
			return "", fmt.Errorf("q64 decoded to an empty query")
		}
	}

	if len(plain) > maxQueryLen {
		return "", fmt.Errorf("query is %d characters, limit is %d", len(plain), maxQueryLen)
	}
	if !isPrintable(plain) {
		return "", fmt.Errorf("query contains control characters")
	}
	return plain, nil
}

// decodeBase64Any accepts standard or URL-safe base64, padded or not, so a link
// producer does not have to know which variant we want.
func decodeBase64Any(s string) ([]byte, error) {
	s = strings.TrimRight(s, "=")
	if strings.ContainsAny(s, "-_") {
		return base64.RawURLEncoding.DecodeString(s)
	}
	return base64.RawStdEncoding.DecodeString(s)
}

func isPrintable(s string) bool {
	for _, r := range s {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
		if r == 0x7f {
			return false
		}
	}
	return true
}

// resolveScope maps a fractal/prism name or id onto something the user can
// actually see. Lookup happens inside the accessible set, so an unknown name and
// a forbidden one are indistinguishable to the caller by design.
func (h *Handler) resolveScope(ctx context.Context, user *storage.User, fractalRef, prismRef string) (scope, error) {
	if prismRef != "" {
		list, err := h.accessiblePrisms(ctx, user)
		if err != nil {
			return scope{}, fmt.Errorf("could not load prisms")
		}
		for _, p := range list {
			if p.ID == prismRef || strings.EqualFold(p.Name, prismRef) {
				return scope{id: p.ID, isPrism: true}, nil
			}
		}
		return scope{}, fmt.Errorf("no prism named %q is available to you", prismRef)
	}

	list, err := h.accessibleFractals(ctx, user)
	if err != nil {
		return scope{}, fmt.Errorf("could not load fractals")
	}

	if fractalRef != "" {
		for _, f := range list {
			if f.ID == fractalRef || strings.EqualFold(f.Name, fractalRef) {
				return scope{id: f.ID}, nil
			}
		}
		return scope{}, fmt.Errorf("no fractal named %q is available to you", fractalRef)
	}

	// No scope given: fall back to the default fractal, or the only one there is.
	for _, f := range list {
		if f.IsDefault {
			return scope{id: f.ID}, nil
		}
	}
	if len(list) == 1 {
		return scope{id: list[0].ID}, nil
	}
	if len(list) == 0 {
		return scope{}, fmt.Errorf("you do not have access to any fractal")
	}
	return scope{}, fmt.Errorf("more than one fractal is available: add ?fractal=<name> to the link")
}

func (h *Handler) accessibleFractals(ctx context.Context, user *storage.User) ([]*fractals.Fractal, error) {
	all, err := h.fractals.ListFractals(ctx)
	if err != nil {
		return nil, err
	}
	if user.IsAdmin {
		return all, nil
	}
	if h.rbac == nil {
		return nil, nil
	}
	access, err := h.rbac.GetAccessibleFractals(ctx, user.Username)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]bool, len(access))
	for _, a := range access {
		allowed[a.FractalID] = true
	}
	var out []*fractals.Fractal
	for _, f := range all {
		if allowed[f.ID] {
			out = append(out, f)
		}
	}
	return out, nil
}

func (h *Handler) accessiblePrisms(ctx context.Context, user *storage.User) ([]*prisms.Prism, error) {
	if h.prisms == nil {
		return nil, nil
	}
	all, err := h.prisms.ListPrisms(ctx)
	if err != nil {
		return nil, err
	}
	if user.IsAdmin {
		return all, nil
	}
	if h.rbac == nil {
		return nil, nil
	}
	access, err := h.rbac.GetAccessiblePrisms(ctx, user.Username)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]bool, len(access))
	for _, a := range access {
		allowed[a.PrismID] = true
	}
	var out []*prisms.Prism
	for _, p := range all {
		if allowed[p.ID] {
			out = append(out, p)
		}
	}
	return out, nil
}

// timeSel is the time picker state a link resolves to.
type timeSel struct {
	tr string // preset key, or "relative", "custom", "all"
	ts string // custom start (RFC3339)
	te string // custom end (RFC3339)
	rn int    // relative magnitude
	ru string // relative unit: minutes|hours|days|weeks
}

// presetFor maps an exact duration onto a time picker preset so the UI shows
// "Last 24h" rather than an equivalent but unfamiliar relative selection.
var presetFor = map[time.Duration]string{
	5 * time.Minute:     "5m",
	15 * time.Minute:    "15m",
	30 * time.Minute:    "30m",
	time.Hour:           "1h",
	2 * time.Hour:       "2h",
	4 * time.Hour:       "4h",
	6 * time.Hour:       "6h",
	12 * time.Hour:      "12h",
	24 * time.Hour:      "24h",
	7 * 24 * time.Hour:  "7d",
	30 * 24 * time.Hour: "30d",
}

var relativeUnit = map[string]string{
	"m": "minutes",
	"h": "hours",
	"d": "days",
	"w": "weeks",
}

var unitDuration = map[string]time.Duration{
	"s": time.Second,
	"m": time.Minute,
	"h": time.Hour,
	"d": 24 * time.Hour,
	"w": 7 * 24 * time.Hour,
}

// parseTimeRange turns from/to into a time picker selection. A relative window
// ending now stays relative so the link means the same thing whenever it is
// opened; anything else is pinned to absolute timestamps.
func parseTimeRange(fromRaw, toRaw string, now time.Time) (timeSel, error) {
	from := strings.TrimSpace(fromRaw)
	to := strings.TrimSpace(toRaw)

	if strings.EqualFold(from, "all") {
		// Silently dropping an end bound would run a wider search than the link
		// asked for, so say so instead.
		if to != "" && !strings.EqualFold(to, "now") {
			return timeSel{}, fmt.Errorf("from=all covers everything and cannot be combined with to=%q", to)
		}
		return timeSel{tr: "all"}, nil
	}
	if from == "" && to == "" {
		return timeSel{tr: "24h"}, nil
	}
	if from == "" {
		from = "-24h"
	}

	endIsNow := to == "" || strings.EqualFold(to, "now")

	if n, unit, ok := parseRelative(from); ok {
		if endIsNow {
			d := time.Duration(n) * unitDuration[unit]
			if preset, found := presetFor[d]; found {
				return timeSel{tr: preset}, nil
			}
			ru, ok := relativeUnit[unit]
			if !ok {
				return timeSel{}, fmt.Errorf("relative windows support m, h, d and w (got %q)", from)
			}
			return timeSel{tr: "relative", rn: n, ru: ru}, nil
		}
		end, err := parseInstant(to)
		if err != nil {
			return timeSel{}, err
		}
		start := end.Add(-time.Duration(n) * unitDuration[unit])
		return custom(start, end)
	}

	start, err := parseInstant(from)
	if err != nil {
		return timeSel{}, err
	}
	end := now
	if !endIsNow {
		if end, err = parseInstant(to); err != nil {
			return timeSel{}, err
		}
	}
	return custom(start, end)
}

func custom(start, end time.Time) (timeSel, error) {
	if !start.Before(end) {
		return timeSel{}, fmt.Errorf("from must be earlier than to")
	}
	return timeSel{
		tr: "custom",
		ts: start.UTC().Format(time.RFC3339),
		te: end.UTC().Format(time.RFC3339),
	}, nil
}

func parseRelative(s string) (int, string, bool) {
	m := relativeRe.FindStringSubmatch(s)
	if m == nil {
		return 0, "", false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n <= 0 {
		return 0, "", false
	}
	return n, m[2], true
}

// parseInstant accepts RFC3339 or a Unix timestamp in seconds or milliseconds.
func parseInstant(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	// Only positive epochs: a negative or zero one is almost always a relative
	// offset that lost its unit ("-24"), and silently reading it as 1969 would
	// hand back an empty result set instead of an error.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil && n > 0 {
		// Anything past this magnitude cannot be seconds without landing in the
		// year 5138, so treat it as milliseconds.
		if n > 1e11 {
			return time.UnixMilli(n).UTC(), nil
		}
		return time.Unix(n, 0).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("%q is not a timestamp: use -24h, now, RFC3339, or a Unix epoch", s)
}

// encodeVars collects var.<name>=<value> pairs into the SPA's vars parameter.
// Repeated plain params are used instead of an encoded JSON blob because every
// templating system that might build one of these links can emit them.
func encodeVars(params url.Values) (string, error) {
	type pair struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	var out []pair
	for key, values := range params {
		name, ok := strings.CutPrefix(key, "var.")
		if !ok {
			continue
		}
		if !varNameRe.MatchString(name) {
			return "", fmt.Errorf("%q is not a valid variable name", name)
		}
		value := values[len(values)-1]
		if len(value) > maxVarLen {
			return "", fmt.Errorf("value for @%s exceeds %d characters", name, maxVarLen)
		}
		if !isPrintable(value) {
			return "", fmt.Errorf("value for @%s contains control characters", name)
		}
		out = append(out, pair{Name: name, Value: value})
	}
	if len(out) == 0 {
		return "", nil
	}
	if len(out) > maxVars {
		return "", fmt.Errorf("too many variables: %d, limit is %d", len(out), maxVars)
	}
	// Map iteration is random; sort so a given link always redirects identically.
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	blob, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("could not encode variables")
	}
	return encodeShareValue(string(blob)), nil
}

// encodeShareValue reproduces the browser's btoa(encodeURIComponent(s)), which
// is what the SPA's share-link reader expects.
func encodeShareValue(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(encodeURIComponent(s)))
}

// encodeURIComponent matches the JavaScript function of the same name: every
// byte outside its unreserved set becomes an uppercase percent escape. Go's
// url escapers differ (notably space as '+'), and decodeURIComponent would not
// undo that.
func encodeURIComponent(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~*'()"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)
			continue
		}
		b.WriteString(fmt.Sprintf("%%%02X", c))
	}
	return b.String()
}

// buildShareURL renders the SPA's internal share-link form.
func buildShareURL(bql string, target scope, sel timeSel, vars string) string {
	p := url.Values{}
	p.Set("q", encodeShareValue(bql))
	p.Set("tr", sel.tr)
	switch sel.tr {
	case "custom":
		p.Set("ts", sel.ts)
		p.Set("te", sel.te)
	case "relative":
		p.Set("rn", strconv.Itoa(sel.rn))
		p.Set("ru", sel.ru)
	}
	if target.isPrism {
		p.Set("p", target.id)
	} else {
		p.Set("f", target.id)
	}
	if vars != "" {
		p.Set("vars", vars)
	}
	return "/?" + p.Encode()
}

var errorPage = template.Must(template.New("deeplink-error").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="referrer" content="no-referrer">
<title>{{.Title}} - Bifract</title>
<link rel="stylesheet" href="/static/css/01-base.css">
<style>
body { display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; }
.dl-card { max-width: 32rem; padding: 2rem; background: var(--bg-secondary); border: 1px solid var(--border-color);
           border-radius: 10px; box-shadow: var(--shadow-lg); }
.dl-card h1 { margin: 0 0 .75rem; font-size: 1.1rem; font-weight: 600; color: var(--text-primary); }
.dl-card p { margin: 0 0 1.25rem; color: var(--text-secondary); font-size: .9rem; line-height: 1.55; }
.dl-card a { color: var(--accent-secondary); text-decoration: none; font-size: .85rem; }
.dl-card a:hover { color: var(--accent-primary); }
</style>
</head>
<body>
<script>
  var t = localStorage.getItem('bifract-theme');
  if (t === 'light') document.documentElement.setAttribute('data-theme', 'light');
</script>
<div class="dl-card">
  <h1>{{.Title}}</h1>
  <p>{{.Detail}}</p>
  <a href="/">Return to Bifract</a>
</div>
</body>
</html>`))

func (h *Handler) renderError(w http.ResponseWriter, status int, title, detail string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = errorPage.Execute(w, struct{ Title, Detail string }{title, detail})
}
