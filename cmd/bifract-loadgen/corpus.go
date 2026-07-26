package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
)

// esc escapes s for direct interpolation between JSON string quotes. Corpus values are
// escaped once at build time so the hot path appends raw bytes instead of calling a marshaler.
func esc(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(b[1 : len(b)-1])
}

// imageEntry is one executable in the corpus. Hashes are fixed per image, not per event:
// real Sysmon reports the same hash for the same binary, and randomizing per event both
// inflates cardinality and understates the compression ratio the benchmark reports.
type imageEntry struct {
	Path     string // JSON-escaped absolute path
	Original string
	Company  string
	Product  string
	Desc     string
	Hashes   string // JSON-escaped "SHA256=...,MD5=...,IMPHASH=..."

	// Behaviour profile. Real telemetry has strong process-behaviour affinity: only mstsc
	// connects to 3389, only services.exe spawns svchost, conhost is a child of a console
	// host. Assigning activity to random processes is what makes synthetic data light up
	// "uncommon parent" and "uncommon port" detections that would never fire in production.
	Base    string   // lowercase basename, for parent matching
	Spawns  bool     // can legitimately be a parent
	DNS     bool     // issues DNS queries
	Files   bool     // writes files
	Ports   []int    // destination ports it legitimately talks to; empty means no network
	Parents []string // legitimate parent basenames; empty means any spawner
	Lolbin  bool     // only emitted under the malice roll
}

// corpus holds every immutable value pool. Built once, read concurrently by all workers.
type corpus struct {
	images    []imageEntry // Zipf-ranked, index 0 most frequent
	domains   []string     // escaped, Zipf-ranked
	extIPs    []string
	intNets   []string // "10.4.12" style /24 prefixes
	users     []string
	fileDirs  []string // escaped dir templates containing {u}
	cmdTmpl   []string // escaped command-line templates, benign baseline
	lolTmpl   []string // escaped command-line templates, malice-gated
	benignIdx []int    // indices into images, non-LOLBin
	lolbinIdx []int    // indices into images, LOLBin
	ports     []int
	tlds      []string
	words     []string
}

// sampler holds a worker's private RNG and Zipf distributions over the shared corpus.
// Zipf matters: uniform random makes every value equally rare, which is unrealistic and
// makes the rarity models produce garbage. Real telemetry is heavy-tailed.
type sampler struct {
	rng     *rand.Rand
	imageZ  *rand.Zipf
	domainZ *rand.Zipf
	ipZ     *rand.Zipf
	card    *cardinality // per-worker, records which pool entries were actually emitted
}

// pickDomain, pickExtIP and pickImage are the only paths that draw from a Zipf pool, so
// they are also where emitted cardinality is recorded.
func (s *sampler) pickDomain(c *corpus) string {
	i := s.domainZ.Uint64()
	s.card.domains.set(i)
	return c.domains[i]
}

func (s *sampler) pickExtIP(c *corpus) string {
	i := s.ipZ.Uint64()
	s.card.extIPs.set(i)
	return c.extIPs[i]
}

func (s *sampler) pickImage(c *corpus) imageEntry {
	i := c.benignIdx[s.imageZ.Uint64()]
	s.card.images.set(uint64(i))
	return c.images[i]
}

// pickLolbin draws a LOLBin. Only reached under the malice roll.
func (s *sampler) pickLolbin(c *corpus) imageEntry {
	i := c.lolbinIdx[s.rng.IntN(len(c.lolbinIdx))]
	s.card.images.set(uint64(i))
	return c.images[i]
}

func newSampler(c *corpus, seed uint64) *sampler {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	return &sampler{
		rng:     r,
		imageZ:  rand.NewZipf(r, 1.25, 1, uint64(len(c.benignIdx)-1)),
		domainZ: rand.NewZipf(r, 1.12, 1, uint64(len(c.domains)-1)),
		ipZ:     rand.NewZipf(r, 1.18, 1, uint64(len(c.extIPs)-1)),
	}
}

// systemImages is ranked most-frequent-first for the Zipf draw. Ports/DNS/Files/Parents
// encode what each binary legitimately does; see imageEntry.
var systemImages = []imageEntry{
	{Path: `C:\Windows\System32\svchost.exe`, Original: "svchost.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Host Process for Windows Services",
		Base: "svchost.exe", Spawns: true, DNS: true, Files: true, Ports: []int{443, 80, 135, 445}, Parents: []string{"services.exe"}},
	{Path: `C:\Program Files\Google\Chrome\Application\chrome.exe`, Original: "chrome.exe", Company: "Google LLC", Product: "Google Chrome", Desc: "Google Chrome",
		Base: "chrome.exe", Spawns: true, DNS: true, Files: true, Ports: []int{443, 80}, Parents: []string{"explorer.exe", "chrome.exe"}},
	{Path: `C:\Windows\explorer.exe`, Original: "EXPLORER.EXE", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Windows Explorer",
		Base: "explorer.exe", Spawns: true, DNS: true, Files: true, Ports: []int{445, 139}},
	{Path: `C:\Windows\System32\RuntimeBroker.exe`, Original: "RuntimeBroker.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Runtime Broker",
		Base: "runtimebroker.exe", Files: true, Parents: []string{"svchost.exe"}},
	{Path: `C:\Windows\System32\conhost.exe`, Original: "CONHOST.EXE", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Console Window Host",
		Base: "conhost.exe", Parents: []string{"cmd.exe", "powershell.exe", "pwsh.exe", "msbuild.exe", "git.exe"}},
	{Path: `C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe`, Original: "msedge.exe", Company: "Microsoft Corporation", Product: "Microsoft Edge", Desc: "Microsoft Edge",
		Base: "msedge.exe", Spawns: true, DNS: true, Files: true, Ports: []int{443, 80}, Parents: []string{"explorer.exe", "msedge.exe"}},
	{Path: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`, Original: "PowerShell.EXE", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Windows PowerShell",
		Base: "powershell.exe", Spawns: true, DNS: true, Files: true, Ports: []int{443, 5985}, Parents: []string{"explorer.exe", "cmd.exe", "code.exe", "svchost.exe"}},
	{Path: `C:\Windows\System32\cmd.exe`, Original: "Cmd.Exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Windows Command Processor",
		Base: "cmd.exe", Spawns: true, Files: true, Parents: []string{"explorer.exe", "code.exe", "cmd.exe", "java.exe"}},
	{Path: `C:\Windows\System32\taskhostw.exe`, Original: "taskhostw.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Host Process for Windows Tasks",
		Base: "taskhostw.exe", Files: true, Parents: []string{"svchost.exe"}},
	{Path: `C:\Program Files\Microsoft Office\root\Office16\OUTLOOK.EXE`, Original: "OUTLOOK.EXE", Company: "Microsoft Corporation", Product: "Microsoft Office", Desc: "Microsoft Outlook",
		Base: "outlook.exe", DNS: true, Files: true, Ports: []int{443}, Parents: []string{"explorer.exe"}},
	{Path: `C:\Windows\System32\services.exe`, Original: "services.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Services and Controller app",
		Base: "services.exe", Spawns: true},
	{Path: `C:\Users\{u}\AppData\Local\Microsoft\Teams\current\Teams.exe`, Original: "Teams.exe", Company: "Microsoft Corporation", Product: "Microsoft Teams", Desc: "Microsoft Teams",
		Base: "teams.exe", DNS: true, Files: true, Ports: []int{443}, Parents: []string{"explorer.exe"}},
	{Path: `C:\Windows\System32\lsass.exe`, Original: "lsass.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Local Security Authority Process",
		Base: "lsass.exe", Ports: []int{88, 389, 636}},
	{Path: `C:\Windows\System32\dllhost.exe`, Original: "dllhost.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "COM Surrogate",
		Base: "dllhost.exe", Files: true, Parents: []string{"svchost.exe", "explorer.exe"}},
	{Path: `C:\Windows\System32\SearchIndexer.exe`, Original: "SearchIndexer.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Microsoft Windows Search Indexer",
		Base: "searchindexer.exe", Files: true, Parents: []string{"services.exe"}},
	{Path: `C:\Program Files\Windows Defender\MsMpEng.exe`, Original: "MsMpEng.exe", Company: "Microsoft Corporation", Product: "Microsoft Defender Antivirus", Desc: "Antimalware Service Executable",
		Base: "msmpeng.exe", Files: true, Ports: []int{443}, Parents: []string{"services.exe"}},
	{Path: `C:\Windows\System32\wbem\WmiPrvSE.exe`, Original: "WmiPrvSE.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "WMI Provider Host",
		Base: "wmiprvse.exe", Files: true, Parents: []string{"svchost.exe"}},
	{Path: `C:\Windows\System32\backgroundTaskHost.exe`, Original: "backgroundTaskHost.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Background Task Host",
		Base: "backgroundtaskhost.exe", Files: true, Parents: []string{"svchost.exe"}},
	{Path: `C:\Windows\System32\mstsc.exe`, Original: "mstsc.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Remote Desktop Connection",
		Base: "mstsc.exe", Files: true, Ports: []int{3389}, Parents: []string{"explorer.exe"}},
	{Path: `C:\Program Files\Java\jre1.8.0_411\bin\java.exe`, Original: "java.exe", Company: "Oracle Corporation", Product: "Java Platform SE 8", Desc: "Java(TM) Platform SE binary",
		Base: "java.exe", Spawns: true, DNS: true, Files: true, Ports: []int{443, 8080, 1433}, Parents: []string{"services.exe", "cmd.exe"}},
	{Path: `C:\Program Files\Git\bin\git.exe`, Original: "git.exe", Company: "The Git Development Community", Product: "Git", Desc: "Git",
		Base: "git.exe", Spawns: true, DNS: true, Files: true, Ports: []int{443, 22}, Parents: []string{"code.exe", "cmd.exe"}},
	{Path: `C:\Program Files\Microsoft VS Code\Code.exe`, Original: "Code.exe", Company: "Microsoft Corporation", Product: "Visual Studio Code", Desc: "Visual Studio Code",
		Base: "code.exe", Spawns: true, DNS: true, Files: true, Ports: []int{443}, Parents: []string{"explorer.exe", "code.exe"}},
	{Path: `C:\Windows\System32\msiexec.exe`, Original: "msiexec.exe", Company: "Microsoft Corporation", Product: "Windows Installer - Unicode", Desc: "Windows Installer",
		Base: "msiexec.exe", Spawns: true, Files: true, Ports: []int{443, 80}, Parents: []string{"services.exe", "explorer.exe"}},
	{Path: `C:\Windows\Microsoft.NET\Framework64\v4.0.30319\MSBuild.exe`, Original: "MSBuild.exe", Company: "Microsoft Corporation", Product: "Microsoft (R) .NET Framework", Desc: "MSBuild",
		Base: "msbuild.exe", Spawns: true, Files: true, Parents: []string{"code.exe", "cmd.exe"}},
	{Path: `C:\Program Files\7-Zip\7z.exe`, Original: "7z.exe", Company: "Igor Pavlov", Product: "7-Zip", Desc: "7-Zip Console",
		Base: "7z.exe", Files: true, Parents: []string{"explorer.exe", "cmd.exe"}},
	{Path: `C:\Windows\System32\OpenSSH\ssh.exe`, Original: "ssh.exe", Company: "OpenSSH for Windows", Product: "OpenSSH for Windows", Desc: "OpenSSH SSH client",
		Base: "ssh.exe", DNS: true, Ports: []int{22}, Parents: []string{"cmd.exe", "powershell.exe", "code.exe"}},
	{Path: `C:\Program Files\PowerShell\7\pwsh.exe`, Original: "pwsh.dll", Company: "Microsoft Corporation", Product: "PowerShell", Desc: "pwsh",
		Base: "pwsh.exe", Spawns: true, DNS: true, Files: true, Ports: []int{443}, Parents: []string{"explorer.exe", "code.exe", "cmd.exe"}},
	{Path: `C:\Windows\System32\wermgr.exe`, Original: "WerMgr.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Windows Problem Reporting",
		Base: "wermgr.exe", Files: true, Ports: []int{443}, Parents: []string{"svchost.exe"}},
	{Path: `C:\Windows\System32\spoolsv.exe`, Original: "spoolsv.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Spooler SubSystem App",
		Base: "spoolsv.exe", Files: true, Parents: []string{"services.exe"}},

	// LOLBins. Real, signed, and present on every host, but their execution is exactly what
	// Sigma is written to catch. Emitted only under the malice roll (-malice), never in the
	// baseline mix, so a benchmark can run with a full rule set without 23% of events matching.
	{Path: `C:\Windows\System32\rundll32.exe`, Original: "RUNDLL32.EXE", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Windows host process (Rundll32)",
		Base: "rundll32.exe", Spawns: true, Lolbin: true, Files: true, Ports: []int{443}, Parents: []string{"explorer.exe", "cmd.exe"}},
	{Path: `C:\Windows\System32\regsvr32.exe`, Original: "REGSVR32.EXE", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Microsoft(C) Register Server",
		Base: "regsvr32.exe", Lolbin: true, DNS: true, Ports: []int{443, 80}, Parents: []string{"cmd.exe", "explorer.exe"}},
	{Path: `C:\Windows\System32\mshta.exe`, Original: "MSHTA.EXE", Company: "Microsoft Corporation", Product: "Internet Explorer", Desc: "Microsoft (R) HTML Application host",
		Base: "mshta.exe", Spawns: true, Lolbin: true, DNS: true, Ports: []int{443, 80}, Parents: []string{"explorer.exe", "cmd.exe"}},
	{Path: `C:\Windows\System32\certutil.exe`, Original: "CertUtil.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "CertUtil.exe",
		Base: "certutil.exe", Lolbin: true, DNS: true, Files: true, Ports: []int{443, 80}, Parents: []string{"cmd.exe", "powershell.exe"}},
	{Path: `C:\Windows\System32\wscript.exe`, Original: "wscript.exe", Company: "Microsoft Corporation", Product: "Microsoft (R) Windows Script Host", Desc: "Microsoft (R) Windows Based Script Host",
		Base: "wscript.exe", Spawns: true, Lolbin: true, Files: true, Parents: []string{"explorer.exe", "cmd.exe"}},
	{Path: `C:\Windows\System32\schtasks.exe`, Original: "schtasks.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Task Scheduler Configuration Tool",
		Base: "schtasks.exe", Lolbin: true, Parents: []string{"cmd.exe", "powershell.exe"}},
	{Path: `C:\Windows\System32\net.exe`, Original: "net.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Net Command",
		Base: "net.exe", Lolbin: true, Ports: []int{445}, Parents: []string{"cmd.exe", "powershell.exe"}},
	{Path: `C:\Windows\System32\whoami.exe`, Original: "whoami.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "whoami - displays logged on user information",
		Base: "whoami.exe", Lolbin: true, Parents: []string{"cmd.exe", "powershell.exe"}},
	{Path: `C:\Windows\System32\reg.exe`, Original: "reg.exe", Company: "Microsoft Corporation", Product: "Microsoft Windows Operating System", Desc: "Registry Console Tool",
		Base: "reg.exe", Lolbin: true, Parents: []string{"cmd.exe", "powershell.exe"}},
	{Path: `C:\Windows\System32\curl.exe`, Original: "curl.exe", Company: "curl, https://curl.se/", Product: "The curl executable", Desc: "The curl executable",
		Base: "curl.exe", Lolbin: true, DNS: true, Files: true, Ports: []int{443, 80}, Parents: []string{"cmd.exe", "powershell.exe"}},
}

// cmdTemplates is the benign baseline. Nothing here should match a detection rule.
var cmdTemplates = []string{
	`"{img}"`,
	`{img} -k netsvcs -p -s Schedule`,
	`{img} -k LocalServiceNetworkRestricted -p -s NlaSvc`,
	`"{img}" --type=renderer --field-trial-handle=1836,i,{n},{n},262144 --disable-features=BackForwardCache,OptimizationGuideModelDownloading --lang=en-US --device-scale-factor=1 --num-raster-threads=4 --enable-main-frame-before-activation --renderer-client-id={n} --time-ticks-at-unix-epoch=-{n} --launch-time-ticks={n} --mojo-platform-channel-handle={n} /prefetch:1`,
	`{img} /c "{dir}\scripts\build_{n}.bat"`,
	`"{img}" -jar "{dir}\lib\agent-{n}.jar" -Dconfig.file={dir}\conf\agent.conf -Xmx2048m -Dfile.encoding=UTF-8 -cp "{dir}\lib\*;{dir}\conf" com.corp.agent.Main --profile prod --node {host}`,
	`"{img}" /i "{dir}\packages\{guid}.msi" /qn REBOOT=ReallySuppress ALLUSERS=1`,
	`"{img}" "{dir}\build\project.sln" /t:Rebuild /p:Configuration=Release /p:Platform=x64 /m:8 /nologo /v:minimal /flp:LogFile={dir}\build\msbuild-{n}.log`,
	`"{img}" clone https://{dom}/corp/{word}-service.git {dir}\src\{word}-service`,
	`"{img}" -File "{dir}\scripts\Get-InventoryReport.ps1" -Scope Site -OutputPath "{dir}\reports\{guid}.xml"`,
	`{img} /v {host} /f`,
	`"{img}" a -tzip "{dir}\archive\{word}-{n}.zip" "{dir}\reports\*"`,
	`"{img}" -T corp@{dom} -i "{dir}\keys\id_rsa"`,
}

// lolbinTemplates are emitted only under the malice roll. These are deliberately the
// patterns Sigma catches, so the run produces genuine detections at a controlled rate
// instead of flooding.
var lolbinTemplates = []string{
	`{img} -urlcache -split -f https://{dom}/updates/{guid}.bin {dir}\cache\{guid}.bin`,
	`{img} /s /n /i:"{dir}\plugins\{word}.dll" scrobj.dll`,
	`"{img}" -NoProfile -EncodedCommand {b64}`,
	`{img} use \\{host}\IPC$ /user:CORP\{u} *`,
	`"{img}" /Create /SC DAILY /TN "CORP\{word}-{n}" /TR "{dir}\bin\{word}.exe --run" /ST 0{n}:00 /RU SYSTEM /F`,
	`{img} add HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Run /v {word}{n} /t REG_SZ /d "{dir}\{word}.exe" /f`,
	`"{img}" -o "{dir}\downloads\{word}-{n}.zip" https://{dom}/artifacts/{word}.zip -H "Authorization: Bearer {b64short}"`,
	`{img} javascript:"\..\mshtml,RunHTMLApplication ";document.write();new%%20ActiveXObject("WScript.Shell").Run("{dir}\{word}.exe")`,
	`{img} /c`,
}

var baseWords = []string{
	"telemetry", "sync", "vault", "ledger", "atlas", "signal", "azure", "delta", "ember", "harrier",
	"gateway", "harbor", "ionic", "jasper", "kestrel", "lumen", "meridian", "nimbus", "orbit", "pulsar",
	"quartz", "relay", "summit", "tundra", "umbra", "vertex", "willow", "xenon", "yarrow", "zephyr",
	"payroll", "invoice", "registry", "identity", "session", "catalog", "billing", "shipping", "audit", "backup",
}

var topDomains = []string{
	"microsoft.com", "windowsupdate.com", "google.com", "gstatic.com", "googleapis.com",
	"office.com", "office365.com", "outlook.com", "live.com", "msftconnecttest.com",
	"akamaiedge.net", "akamai.net", "cloudfront.net", "amazonaws.com", "azureedge.net",
	"digicert.com", "verisign.com", "entrust.net", "sectigo.com", "globalsign.com",
	"github.com", "githubusercontent.com", "slack.com", "zoom.us", "salesforce.com",
	"atlassian.net", "okta.com", "duosecurity.com", "cloudflare.com", "fastly.net",
}

var tlds = []string{"com", "net", "org", "io", "co", "cloud", "app", "dev", "info", "biz", "xyz", "top", "ru", "cn", "de", "uk"}

var fileDirTemplates = []string{
	`C:\Users\{u}\AppData\Local\Temp`,
	`C:\Users\{u}\AppData\Roaming\Microsoft\Windows\Recent`,
	`C:\Users\{u}\AppData\Local\Microsoft\Windows\INetCache\IE`,
	`C:\Users\{u}\Downloads`,
	`C:\Users\{u}\Documents`,
	`C:\Users\{u}\AppData\Local\Google\Chrome\User Data\Default\Cache\Cache_Data`,
	`C:\ProgramData\Microsoft\Windows Defender\Scans\History\Service`,
	`C:\Windows\Temp`,
	`C:\Windows\Prefetch`,
	`C:\ProgramData\Package Cache`,
	`C:\Program Files\CorpAgent\logs`,
	`C:\Windows\System32\config\systemprofile\AppData\Local`,
}

var fileExts = []string{"tmp", "log", "dat", "db", "json", "xml", "cache", "etl", "pf", "bin", "dll", "exe", "ps1", "zip", "txt"}

var userNames = []string{
	"jsmith", "mgarcia", "kobrien", "achen", "rpatel", "lnguyen", "dkowalski", "sthompson",
	"bmartinez", "hokafor", "tyamamoto", "cdubois", "gpetrov", "nsilva", "eberg", "fmoreau",
	"asvc.backup", "asvc.sql", "asvc.iis", "admin.jsmith", "admin.kobrien", "helpdesk1", "helpdesk2",
}

var commonPorts = []int{443, 443, 443, 443, 80, 80, 445, 139, 3389, 53, 88, 389, 636, 135, 5985, 22, 8080, 8443, 1433, 3128}

// buildCorpus creates every value pool. domainPool sizes the Zipf domain tail.
func buildCorpus(domainPool, extIPPool int, seed uint64) *corpus {
	r := rand.New(rand.NewPCG(seed, 0x243f6a8885a308d3))
	c := &corpus{
		users: make([]string, 0, len(userNames)),
		ports: commonPorts,
		tlds:  tlds,
		words: baseWords,
	}

	for _, im := range systemImages {
		e := im
		e.Path = esc(im.Path)
		e.Original = esc(im.Original)
		e.Company = esc(im.Company)
		e.Product = esc(im.Product)
		e.Desc = esc(im.Desc)
		e.Hashes = esc(hashesFor(r))
		c.images = append(c.images, e)
		if e.Lolbin {
			c.lolbinIdx = append(c.lolbinIdx, len(c.images)-1)
		} else {
			c.benignIdx = append(c.benignIdx, len(c.images)-1)
		}
	}

	c.domains = make([]string, 0, domainPool)
	for _, d := range topDomains {
		c.domains = append(c.domains, esc(d))
	}
	for len(c.domains) < domainPool {
		w1 := baseWords[r.IntN(len(baseWords))]
		w2 := baseWords[r.IntN(len(baseWords))]
		tld := tlds[r.IntN(len(tlds))]
		var d string
		switch r.IntN(4) {
		case 0:
			d = fmt.Sprintf("%s-%s.%s", w1, w2, tld)
		case 1:
			d = fmt.Sprintf("%s%d.%s", w1, r.IntN(9999), tld)
		case 2:
			d = fmt.Sprintf("%s.%s.%s", w1, w2, tld)
		default:
			d = fmt.Sprintf("cdn%d.%s.%s", r.IntN(64), w2, tld)
		}
		c.domains = append(c.domains, esc(d))
	}

	c.extIPs = make([]string, 0, extIPPool)
	for len(c.extIPs) < extIPPool {
		c.extIPs = append(c.extIPs, fmt.Sprintf("%d.%d.%d.%d",
			r.IntN(206)+18, r.IntN(256), r.IntN(256), r.IntN(254)+1))
	}

	for i := 0; i < 48; i++ {
		c.intNets = append(c.intNets, fmt.Sprintf("10.%d.%d", r.IntN(64), r.IntN(256)))
	}

	for _, u := range userNames {
		c.users = append(c.users, u)
	}
	for _, d := range fileDirTemplates {
		c.fileDirs = append(c.fileDirs, esc(d))
	}
	for _, t := range cmdTemplates {
		c.cmdTmpl = append(c.cmdTmpl, esc(t))
	}
	for _, t := range lolbinTemplates {
		c.lolTmpl = append(c.lolTmpl, esc(t))
	}
	return c
}

// hashesFor builds a fixed Sysmon Hashes string for one binary.
func hashesFor(r *rand.Rand) string {
	var sha [32]byte
	var md5b [16]byte
	var imp [8]byte
	binary.LittleEndian.PutUint64(sha[0:], r.Uint64())
	binary.LittleEndian.PutUint64(sha[8:], r.Uint64())
	binary.LittleEndian.PutUint64(sha[16:], r.Uint64())
	binary.LittleEndian.PutUint64(sha[24:], r.Uint64())
	binary.LittleEndian.PutUint64(md5b[0:], r.Uint64())
	binary.LittleEndian.PutUint64(md5b[8:], r.Uint64())
	binary.LittleEndian.PutUint64(imp[0:], r.Uint64())
	return "SHA256=" + hex.EncodeToString(sha[:]) +
		",MD5=" + hex.EncodeToString(md5b[:]) +
		",IMPHASH=" + hex.EncodeToString(imp[:])
}

// tailImage builds a one-off binary outside the known-good set. These drive the rare end
// of the proc_freq baseline; without them every image looks ubiquitous.
func (s *sampler) tailImage(c *corpus, user string) imageEntry {
	w := c.words[s.rng.IntN(len(c.words))]
	n := s.rng.IntN(100000)
	var p string
	switch s.rng.IntN(3) {
	case 0:
		p = fmt.Sprintf(`C:\Users\%s\AppData\Local\Temp\%s_%d\setup.exe`, user, w, n)
	case 1:
		p = fmt.Sprintf(`C:\Users\%s\Downloads\%s-installer-%d.exe`, user, w, n)
	default:
		p = fmt.Sprintf(`C:\ProgramData\%s\%s%d.exe`, w, w, n)
	}
	return imageEntry{
		Path:     esc(p),
		Original: esc(fmt.Sprintf("%s%d.exe", w, n)),
		Company:  esc(fmt.Sprintf("%s Software Ltd", w)),
		Product:  esc(w),
		Desc:     esc(fmt.Sprintf("%s installer", w)),
		Hashes:   esc(hashesFor(s.rng)),
	}
}
