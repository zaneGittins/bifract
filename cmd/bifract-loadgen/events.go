package main

import (
	"fmt"
	"strconv"
)

// Event kinds, indexed for per-type counters.
const (
	kindNetwork = iota
	kindFile
	kindDNS
	kindProcess
	kindRemoteThread
	kindProcAccess
	kindCount
)

var kindNames = [kindCount]string{"net_connect(3)", "file_create(11)", "dns_query(22)", "process_create(1)", "remote_thread(8)", "process_access(10)"}

// Cumulative event mix, Sysmon-realistic weighting. Matches the benchmark plan.
var kindCumulative = [kindCount]int{35, 65, 85, 95, 98, 100}

// jbuf writes JSON directly. Corpus values are pre-escaped so the hot path never calls a
// marshaler; at 4,000 events/sec through goja-equivalent reflection this is the difference
// between the generator being the bottleneck and not.
type jbuf struct {
	b     []byte
	first bool
}

func (j *jbuf) open()  { j.b = append(j.b, '{'); j.first = true }
func (j *jbuf) close() { j.b = append(j.b, '}') }

func (j *jbuf) key(k string) {
	if j.first {
		j.first = false
	} else {
		j.b = append(j.b, ',')
	}
	j.b = append(j.b, '"')
	j.b = append(j.b, k...)
	j.b = append(j.b, '"', ':')
}

func (j *jbuf) s(k, v string) {
	j.key(k)
	j.b = append(j.b, '"')
	j.b = append(j.b, v...)
	j.b = append(j.b, '"')
}

func (j *jbuf) i(k string, v int) {
	j.key(k)
	j.b = strconv.AppendInt(j.b, int64(v), 10)
}

func (j *jbuf) bl(k string, v bool) {
	j.key(k)
	j.b = strconv.AppendBool(j.b, v)
}

// ts writes a Sysmon UtcTime. The seconds portion is formatted once per batch by the
// caller; only the millisecond field varies per event.
func (j *jbuf) ts(k, base string, ms int) {
	j.key(k)
	j.b = append(j.b, '"')
	j.b = append(j.b, base...)
	j.b = append(j.b, '.')
	if ms < 100 {
		j.b = append(j.b, '0')
	}
	if ms < 10 {
		j.b = append(j.b, '0')
	}
	j.b = strconv.AppendInt(j.b, int64(ms), 10)
	j.b = append(j.b, '"')
}

func (j *jbuf) header(h *host, id int, base string, ms int) {
	j.open()
	j.i("EventID", id)
	j.s("Channel", "Microsoft-Windows-Sysmon/Operational")
	j.s("Provider", "Microsoft-Windows-Sysmon")
	j.s("Computer", h.name)
	j.s("RuleName", "-")
	j.ts("UtcTime", base, ms)
}

// emit selects an event kind, mutates host state as needed, appends one JSON object,
// and returns the kind emitted.
func (s *sampler) emit(j *jbuf, c *corpus, h *host, base string, ms int, malice float64) int {
	roll := s.rng.IntN(100)
	switch {
	case roll < kindCumulative[kindNetwork]:
		s.writeNetwork(j, c, h, base, ms)
		return kindNetwork
	case roll < kindCumulative[kindFile]:
		s.writeFile(j, c, h, base, ms)
		return kindFile
	case roll < kindCumulative[kindDNS]:
		s.writeDNS(j, c, h, base, ms)
		return kindDNS
	case roll < kindCumulative[kindProcess]:
		s.writeProcess(j, c, h, base, ms, malice)
		return kindProcess
	case roll < kindCumulative[kindRemoteThread]:
		s.writeRemoteThread(j, c, h, base, ms)
		return kindRemoteThread
	default:
		s.writeProcAccess(j, c, h, base, ms)
		return kindProcAccess
	}
}

func (s *sampler) writeProcess(j *jbuf, c *corpus, h *host, base string, ms int, malice float64) {
	child, parent := s.spawn(c, h, malice)
	j.header(h, 1, base, ms)
	j.s("ProcessGuid", child.guid)
	j.i("ProcessId", child.pid)
	j.s("Image", child.img.Path)
	j.s("FileVersion", fmt.Sprintf("10.0.22621.%d", s.rng.IntN(4000)))
	j.s("Description", child.img.Desc)
	j.s("Product", child.img.Product)
	j.s("Company", child.img.Company)
	j.s("OriginalFileName", child.img.Original)
	j.s("CommandLine", child.cmdLine)
	j.s("CurrentDirectory", esc(`C:\Users\`+h.users[child.userIdx]+`\`))
	j.s("User", h.usersEsc[child.userIdx])
	j.s("LogonGuid", child.logon)
	j.s("LogonId", child.logonID)
	j.i("TerminalSessionId", child.sessID)
	j.s("IntegrityLevel", child.integ)
	j.s("Hashes", child.img.Hashes)
	j.s("ParentProcessGuid", parent.guid)
	j.i("ParentProcessId", parent.pid)
	j.s("ParentImage", parent.img.Path)
	j.s("ParentCommandLine", parent.cmdLine)
	j.s("ParentUser", h.usersEsc[parent.userIdx])
	j.close()
}

func (s *sampler) writeNetwork(j *jbuf, c *corpus, h *host, base string, ms int) {
	p := s.pickCapable(h, canNet)
	dstPort := 443
	if len(p.img.Ports) > 0 {
		dstPort = p.img.Ports[s.rng.IntN(len(p.img.Ports))]
	}
	j.header(h, 3, base, ms)
	j.s("ProcessGuid", p.guid)
	j.i("ProcessId", p.pid)
	j.s("Image", p.img.Path)
	j.s("User", h.usersEsc[p.userIdx])
	j.s("Protocol", "tcp")
	j.bl("Initiated", true)
	j.bl("SourceIsIpv6", false)
	j.s("SourceIp", s.srcIP(h))
	j.s("SourceHostname", h.name)
	j.i("SourcePort", 49152+s.rng.IntN(16000))
	j.s("SourcePortName", "")
	j.bl("DestinationIsIpv6", false)
	j.s("DestinationIp", s.dstIP(c))
	if s.rng.IntN(100) < 40 {
		j.s("DestinationHostname", s.pickDomain(c))
	} else {
		j.s("DestinationHostname", "")
	}
	j.i("DestinationPort", dstPort)
	j.s("DestinationPortName", portName(dstPort))
	j.close()
}

func (s *sampler) writeFile(j *jbuf, c *corpus, h *host, base string, ms int) {
	p := s.pickCapable(h, canFile)
	j.header(h, 11, base, ms)
	j.s("ProcessGuid", p.guid)
	j.i("ProcessId", p.pid)
	j.s("Image", p.img.Path)
	j.s("TargetFilename", s.filePath(c, h.users[p.userIdx]))
	j.ts("CreationUtcTime", base, ms)
	j.s("User", h.usersEsc[p.userIdx])
	j.close()
}

func (s *sampler) writeDNS(j *jbuf, c *corpus, h *host, base string, ms int) {
	p := s.pickCapable(h, canDNS)
	dom := s.pickDomain(c)
	j.header(h, 22, base, ms)
	j.s("ProcessGuid", p.guid)
	j.i("ProcessId", p.pid)
	j.s("QueryName", dom)
	if s.rng.IntN(100) < 92 {
		j.s("QueryStatus", "0")
		j.s("QueryResults", fmt.Sprintf("type:  5 %s;::ffff:%s;", dom, s.dstIP(c)))
	} else {
		j.s("QueryStatus", "9701")
		j.s("QueryResults", "-")
	}
	j.s("Image", p.img.Path)
	j.s("User", h.usersEsc[p.userIdx])
	j.close()
}

func (s *sampler) writeRemoteThread(j *jbuf, c *corpus, h *host, base string, ms int) {
	src := s.pickInjector(h)
	tgt := s.pickInjectTarget(h)
	j.header(h, 8, base, ms)
	j.s("SourceProcessGuid", src.guid)
	j.i("SourceProcessId", src.pid)
	j.s("SourceImage", src.img.Path)
	j.s("TargetProcessGuid", tgt.guid)
	j.i("TargetProcessId", tgt.pid)
	j.s("TargetImage", tgt.img.Path)
	j.i("NewThreadId", 1000+s.rng.IntN(60000))
	j.s("StartAddress", fmt.Sprintf("0x%012X", s.rng.Uint64()&0xFFFFFFFFFFFF))
	j.s("StartModule", "")
	j.s("StartFunction", "")
	j.close()
}

func (s *sampler) writeProcAccess(j *jbuf, c *corpus, h *host, base string, ms int) {
	src := s.pickInjector(h)
	tgt := s.pickInjectTarget(h)
	// Sysmon really does use "GUID" casing on event 10 and "Guid" on event 8. Reproduced
	// deliberately: a normalizer that only handles one form will silently drop these.
	j.header(h, 10, base, ms)
	j.s("SourceProcessGUID", src.guid)
	j.i("SourceProcessId", src.pid)
	j.i("SourceThreadId", 1000+s.rng.IntN(60000))
	j.s("SourceImage", src.img.Path)
	j.s("TargetProcessGUID", tgt.guid)
	j.i("TargetProcessId", tgt.pid)
	j.s("TargetImage", tgt.img.Path)
	j.s("GrantedAccess", []string{"0x1000", "0x1410", "0x1fffff", "0x143a", "0x40"}[s.rng.IntN(5)])
	j.s("CallTrace", s.callTrace())
	j.close()
}

var traceModules = []string{
	`C:\\Windows\\SYSTEM32\\ntdll.dll`,
	`C:\\Windows\\System32\\KERNELBASE.dll`,
	`C:\\Windows\\System32\\kernel32.dll`,
	`C:\\Windows\\System32\\rpcrt4.dll`,
	`C:\\Windows\\System32\\combase.dll`,
	`C:\\Windows\\System32\\ole32.dll`,
	`C:\\Windows\\System32\\sechost.dll`,
	`C:\\Windows\\System32\\advapi32.dll`,
}

// callTrace builds a realistic Sysmon event 10 CallTrace. These are long, and they are a
// meaningful part of why real Sysmon events average well over a kilobyte.
func (s *sampler) callTrace() string {
	n := 4 + s.rng.IntN(8)
	out := make([]byte, 0, n*48)
	for i := 0; i < n; i++ {
		if i > 0 {
			out = append(out, '|')
		}
		out = append(out, traceModules[s.rng.IntN(len(traceModules))]...)
		out = append(out, '+')
		out = strconv.AppendInt(out, int64(s.rng.IntN(0xfffff)), 16)
	}
	return string(out)
}

func portName(p int) string {
	switch p {
	case 443:
		return "https"
	case 80:
		return "http"
	case 445:
		return "microsoft-ds"
	case 139:
		return "netbios-ssn"
	case 3389:
		return "ms-wbt-server"
	case 53:
		return "domain"
	case 88:
		return "kerberos"
	case 389:
		return "ldap"
	case 636:
		return "ldaps"
	case 135:
		return "epmap"
	case 22:
		return "ssh"
	case 1433:
		return "ms-sql-s"
	}
	return ""
}
