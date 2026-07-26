package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"
)

const (
	rootProcs         = 3 // per-host long-lived roots, never evicted
	maxProcsPerHost   = 24
	tailImagePerMille = 4 // per-mille of spawns using a one-off binary
)

// process is one live process on a host. Its GUID is reused across every file, network,
// and DNS event it emits, which is what makes the provenance edges real rather than
// synthetic noise. Without reuse pgr() has nothing to traverse.
type process struct {
	guid    string // "{8-4-4-4-12}", JSON-safe as-is
	pid     int
	img     imageEntry
	userIdx int
	logon   string
	logonID string
	cmdLine string // escaped, built once at spawn
	sessID  int
	integ   string
}

type host struct {
	idx      int    // global index, for cardinality accounting
	name     string // escaped FQDN
	nameRaw  string
	ipPrefix string // "10.4.12"
	ipLast   int
	users    []string // raw usernames, for path building
	usersEsc []string // escaped "CORP\\user", for JSON
	procs    []process
	nextPID  int
}

func (s *sampler) newGUID() string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], s.rng.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], s.rng.Uint64())
	h := hex.EncodeToString(b[:])
	return "{" + h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32] + "}"
}

var hostPrefixes = []string{"WKSTN", "LAPTOP", "SRV", "VDI", "KIOSK"}

func newHost(c *corpus, s *sampler, idx int) *host {
	pfx := hostPrefixes[idx%len(hostPrefixes)]
	raw := fmt.Sprintf("%s-%05d", pfx, idx)
	h := &host{
		idx:      idx,
		name:     esc(raw + ".corp.example.com"),
		nameRaw:  raw,
		ipPrefix: c.intNets[s.rng.IntN(len(c.intNets))],
		ipLast:   s.rng.IntN(250) + 2,
		nextPID:  (s.rng.IntN(2000) + 600) * 4,
	}
	nUsers := 1 + s.rng.IntN(3)
	for i := 0; i < nUsers; i++ {
		u := c.users[s.rng.IntN(len(c.users))]
		h.users = append(h.users, u)
		h.usersEsc = append(h.usersEsc, esc(`CORP\`+u))
	}
	// Roots, in tree order: services.exe and explorer.exe are unparented; svchost descends
	// from services. Everything else must find a legitimate parent among these.
	for _, base := range []string{"services.exe", "explorer.exe", "svchost.exe"} {
		if img, ok := imageByBase(c, base); ok {
			h.procs = append(h.procs, s.newProcess(c, h, img, 0))
		}
	}
	return h
}

func (s *sampler) newProcess(c *corpus, h *host, img imageEntry, userIdx int) process {
	h.nextPID += 4
	p := process{
		guid:    s.newGUID(),
		pid:     h.nextPID,
		img:     img,
		userIdx: userIdx,
		logon:   s.newGUID(),
		logonID: fmt.Sprintf("0x%x", s.rng.IntN(0xffffff)),
		sessID:  s.rng.IntN(3),
		integ:   []string{"Medium", "High", "System", "Low"}[s.rng.IntN(4)],
	}
	p.cmdLine = s.buildCmdLine(c, h, img, h.users[userIdx])
	return p
}

func imageByBase(c *corpus, base string) (imageEntry, bool) {
	for _, im := range c.images {
		if im.Base == base {
			return im, true
		}
	}
	return imageEntry{}, false
}

// pickProc returns a live process, biased toward recently created ones so activity
// clusters on new processes the way it does on a real host.
func (s *sampler) pickProc(h *host) *process {
	n := len(h.procs)
	if n == 0 {
		return nil
	}
	if n > rootProcs && s.rng.IntN(100) < 75 {
		lo := n - min(8, n-rootProcs)
		return &h.procs[lo+s.rng.IntN(n-lo)]
	}
	return &h.procs[s.rng.IntN(n)]
}

// pickCapable returns a live process that legitimately performs the requested activity.
// Falls back to svchost, which does all three, so an event is never assigned to a process
// that would not plausibly generate it.
func (s *sampler) pickCapable(h *host, can func(*process) bool) *process {
	for attempt := 0; attempt < 6; attempt++ {
		if p := s.pickProc(h); p != nil && can(p) {
			return p
		}
	}
	for i := range h.procs {
		if can(&h.procs[i]) {
			return &h.procs[i]
		}
	}
	return s.pickProc(h)
}

func canNet(p *process) bool  { return len(p.img.Ports) > 0 }
func canDNS(p *process) bool  { return p.img.DNS }
func canFile(p *process) bool { return p.img.Files }

// spawn creates a child process under a LEGITIMATE parent for that image and returns both.
// Picking an arbitrary parent is what lights up the whole "Uncommon Parent Process" family
// of detections, which is most of the false-positive volume on synthetic data.
func (s *sampler) spawn(c *corpus, h *host, malice float64) (child *process, parent *process) {
	var img imageEntry
	switch {
	case s.rng.Float64() < malice:
		img = s.pickLolbin(c)
	case s.rng.IntN(1000) < tailImagePerMille:
		img = s.tailImage(c, h.users[s.rng.IntN(len(h.users))])
	default:
		img = s.pickImage(c)
	}

	parent = s.pickLegitParent(h, img)
	userIdx := parent.userIdx
	if s.rng.IntN(100) < 20 && len(h.users) > 1 {
		userIdx = s.rng.IntN(len(h.users))
	}

	p := s.newProcess(c, h, img, userIdx)
	if len(h.procs) >= maxProcsPerHost {
		copy(h.procs[rootProcs:], h.procs[rootProcs+1:])
		h.procs = h.procs[:len(h.procs)-1]
	}
	h.procs = append(h.procs, p)
	return &h.procs[len(h.procs)-1], parent
}

// pickLegitParent finds a live process whose basename is in img.Parents. Falls back to
// explorer.exe (index 1), which is a defensible parent for anything user-launched.
func (s *sampler) pickLegitParent(h *host, img imageEntry) *process {
	if len(img.Parents) > 0 {
		var cands []int
		for i := range h.procs {
			for _, want := range img.Parents {
				if h.procs[i].img.Base == want {
					cands = append(cands, i)
					break
				}
			}
		}
		if len(cands) > 0 {
			return &h.procs[cands[s.rng.IntN(len(cands))]]
		}
	}
	for attempt := 0; attempt < 4; attempt++ {
		if p := s.pickProc(h); p != nil && p.img.Spawns {
			return p
		}
	}
	return &h.procs[1%len(h.procs)]
}

func (s *sampler) buildCmdLine(c *corpus, h *host, img imageEntry, user string) string {
	pool := c.cmdTmpl
	if img.Lolbin && len(c.lolTmpl) > 0 {
		pool = c.lolTmpl
	}
	tmpl := pool[s.rng.IntN(len(pool))]
	out := make([]byte, 0, 256)
	for i := 0; i < len(tmpl); i++ {
		if tmpl[i] != '{' {
			out = append(out, tmpl[i])
			continue
		}
		end := -1
		for j := i + 1; j < len(tmpl) && j < i+8; j++ {
			if tmpl[j] == '}' {
				end = j
				break
			}
		}
		if end < 0 {
			out = append(out, tmpl[i])
			continue
		}
		switch tmpl[i+1 : end] {
		case "img":
			out = append(out, img.Path...)
		case "n":
			out = append(out, fmt.Sprintf("%d", s.rng.IntN(999999))...)
		case "u":
			out = append(out, esc(user)...)
		case "host":
			out = append(out, h.name...)
		case "guid":
			out = append(out, s.newGUID()...)
		case "dir":
			out = append(out, esc(fmt.Sprintf(`C:\Users\%s\AppData\Local\%s`, user, c.words[s.rng.IntN(len(c.words))]))...)
		case "dom":
			out = append(out, s.pickDomain(c)...)
		case "word":
			out = append(out, c.words[s.rng.IntN(len(c.words))]...)
		case "b64":
			out = append(out, s.b64blob(700)...)
		case "b64short":
			out = append(out, s.b64blob(48)...)
		default:
			out = append(out, tmpl[i:end+1]...)
		}
		i = end
	}
	return string(out)
}

const b64alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

// b64blob emits base64-looking filler. Encoded PowerShell is common in real telemetry
// and is a large share of why real command lines are long.
func (s *sampler) b64blob(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = b64alpha[s.rng.IntN(len(b64alpha))]
	}
	return string(b)
}

func (s *sampler) srcIP(h *host) string {
	return fmt.Sprintf("%s.%d", h.ipPrefix, h.ipLast)
}

// dstIP returns an internal or external destination. Internal traffic dominates on a
// real network, and the abstraction in proc_freq collapses internal to a /24.
func (s *sampler) dstIP(c *corpus) string {
	if s.rng.IntN(100) < 45 {
		return fmt.Sprintf("%s.%d", c.intNets[s.rng.IntN(len(c.intNets))], s.rng.IntN(254)+1)
	}
	return s.pickExtIP(c)
}

func (s *sampler) filePath(c *corpus, user string) string {
	dir := c.fileDirs[s.rng.IntN(len(c.fileDirs))]
	w := c.words[s.rng.IntN(len(c.words))]
	ext := fileExts[s.rng.IntN(len(fileExts))]
	name := fmt.Sprintf("%s%d.%s", w, s.rng.IntN(1000000), ext)
	if s.rng.IntN(100) < 30 {
		name = s.newGUID() + "." + ext
	}
	// dir templates carry {u}; substitute the escaped user.
	return replaceUser(dir, esc(user)) + esc(`\`) + esc(name)
}

func replaceUser(dir, userEsc string) string {
	const tok = "{u}"
	i := indexOf(dir, tok)
	if i < 0 {
		return dir
	}
	return dir[:i] + userEsc + dir[i+len(tok):]
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func utcBase(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05") }

// injectorBases are the processes that legitimately open handles into others on a normal
// host: the AV engine, WMI, and the service controller. Anything else doing this is the
// signal detections are looking for.
var injectorBases = []string{"msmpeng.exe", "wmiprvse.exe", "services.exe", "svchost.exe"}

func (s *sampler) pickInjector(h *host) *process {
	return s.pickCapable(h, func(p *process) bool {
		for _, b := range injectorBases {
			if p.img.Base == b {
				return true
			}
		}
		return false
	})
}

// pickInjectTarget never returns lsass. A remote thread into lsass is credential dumping,
// not background noise, and emitting it at random floods those rules.
func (s *sampler) pickInjectTarget(h *host) *process {
	return s.pickCapable(h, func(p *process) bool { return p.img.Base != "lsass.exe" })
}
