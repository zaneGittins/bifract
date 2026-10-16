package alerts

import (
	"bufio"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// A log value in the alert name reaches the Subject header. A line break in it must
// not end the header and let log content write headers of its own.
func TestBuildMIMEMessageKeepsHeadersOnOneLine(t *testing.T) {
	msg := string((&EmailClient{}).buildMIMEMessage(
		"alerts@example.test",
		[]string{"soc@example.test", "oncall@example.test"},
		"Login by bob\r\nBcc: attacker@evil.test",
		"<p>body</p>",
	))

	head, body, ok := strings.Cut(msg, "\r\n\r\n")
	if !ok {
		t.Fatalf("no header/body separator in %q", msg)
	}
	if body != "<p>body</p>" {
		t.Errorf("body = %q", body)
	}
	headers := map[string]string{}
	for _, line := range strings.Split(head, "\r\n") {
		k, v, _ := strings.Cut(line, ": ")
		if _, dup := headers[k]; dup {
			t.Errorf("header %s appears twice", k)
		}
		headers[k] = v
	}
	if _, injected := headers["Bcc"]; injected {
		t.Fatal("a CRLF in the subject injected a Bcc header")
	}
	if headers["Subject"] != "Login by bob Bcc: attacker@evil.test" {
		t.Errorf("Subject = %q", headers["Subject"])
	}
	if headers["To"] != "soc@example.test, oncall@example.test" || headers["From"] != "alerts@example.test" {
		t.Errorf("addresses: %v", headers)
	}
	if headers["Content-Type"] != `text/html; charset="UTF-8"` || headers["MIME-Version"] != "1.0" {
		t.Errorf("content headers: %v", headers)
	}
	if _, err := time.Parse(time.RFC1123Z, headers["Date"]); err != nil {
		t.Errorf("Date header %q: %v", headers["Date"], err)
	}
}

func TestEmailTemplateResolution(t *testing.T) {
	ec := NewEmailClient(nil, "https://bifract.example.test/")
	alert := &Alert{
		ID: "a1", Description: "Seen <script>", Severity: "high", QueryString: `cmd="a&b"`,
		Labels: []string{"attack.t1059", "x<y"}, FractalID: "f1",
	}
	triggered := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	got := ec.resolveTemplate(
		"{{alert_name}}|{{alert_id}}|{{description}}|{{severity}}|{{query}}|{{match_count}}|{{labels}}|{{unknown}}",
		alert, "Login by <bob>", []map[string]interface{}{{}, {}}, triggered)
	want := "Login by &lt;bob&gt;|a1|Seen &lt;script&gt;|high|cmd=&#34;a&amp;b&#34;|2|attack.t1059, x&lt;y|{{unknown}}"
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}

	link := ec.resolveTemplate("{{alert_link}}", alert, "n", nil, triggered)
	if !strings.HasPrefix(link, "https://bifract.example.test/?") || !strings.Contains(link, "f=f1") {
		t.Errorf("link = %q", link)
	}

	if ec.resolveTemplate("", alert, "n", nil, triggered) != "" {
		t.Error("an empty template stays empty so the default applies")
	}
}

// Placeholders resolve in one pass: a value that itself contains a placeholder is
// text, and must not be substituted again depending on map iteration order.
func TestEmailTemplateIsSinglePass(t *testing.T) {
	ec := NewEmailClient(nil, "")
	alert := &Alert{Description: "mentions {{severity}} literally", Severity: "high"}
	for i := 0; i < 50; i++ {
		got := ec.resolveTemplate("{{description}}", alert, "n", nil, time.Time{})
		if got != "mentions {{severity}} literally" {
			t.Fatalf("iteration %d: got %q", i, got)
		}
	}
}

func TestEmailDefaultBody(t *testing.T) {
	ec := NewEmailClient(nil, "https://bifract.example.test")
	alert := &Alert{
		Description: "desc & more", Severity: "informational", QueryString: `a="<b>"`,
		Labels: []string{"l1"}, FractalID: "f1",
	}
	body := ec.defaultBody(alert, "Name <x>", []map[string]interface{}{{}, {}, {}}, time.Now())

	for _, want := range []string{
		"Name &lt;x&gt;",
		"desc &amp; more",
		"a=&#34;&lt;b&gt;&#34;",
		"<td style=\"padding:6px 12px;\">3</td>",
		"INFORMATIONAL",
		"#6b7280", // informational is grey, not the medium yellow
		`href="https://bifract.example.test/?`,
		"width:100%;",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body is missing %q", want)
		}
	}
	if strings.Contains(body, "%%") {
		t.Error("a literal %% reached the HTML: the table width is invalid CSS")
	}

	// No scope, no link; an unknown severity falls back to the medium colour; no
	// labels, no labels row.
	plain := ec.defaultBody(&Alert{Severity: "weird"}, "n", nil, time.Now())
	if strings.Contains(plain, "View in Bifract") || !strings.Contains(plain, "#eab308") || strings.Contains(plain, ">Labels<") {
		t.Errorf("unscoped body: %s", plain)
	}
}

// fakeSMTP is just enough of a server to record what a client sends.
type fakeSMTP struct {
	ln       net.Listener
	mu       sync.Mutex
	from     string
	rcpts    []string
	data     string
	authSeen bool
	rejectTo string
}

func startFakeSMTP(t *testing.T) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeSMTP{ln: ln}
	go s.serve()
	t.Cleanup(func() { ln.Close() })
	return s
}

func (s *fakeSMTP) port() int { return s.ln.Addr().(*net.TCPAddr).Port }

func (s *fakeSMTP) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *fakeSMTP) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(conn)
	reply := func(line string) { _, _ = conn.Write([]byte(line + "\r\n")) }
	reply("220 fake ESMTP")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		cmd := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			reply("250-fake")
			reply("250 AUTH PLAIN")
		case strings.HasPrefix(cmd, "AUTH"):
			s.mu.Lock()
			s.authSeen = true
			s.mu.Unlock()
			reply("235 ok")
		case strings.HasPrefix(cmd, "MAIL FROM:"):
			s.mu.Lock()
			s.from = strings.Trim(line[len("MAIL FROM:"):], "<> ")
			s.mu.Unlock()
			reply("250 ok")
		case strings.HasPrefix(cmd, "RCPT TO:"):
			addr := strings.Trim(line[len("RCPT TO:"):], "<> ")
			s.mu.Lock()
			reject := addr == s.rejectTo
			if !reject {
				s.rcpts = append(s.rcpts, addr)
			}
			s.mu.Unlock()
			if reject {
				reply("550 no such user")
			} else {
				reply("250 ok")
			}
		case cmd == "DATA":
			reply("354 go ahead")
			var b strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if l == ".\r\n" {
					break
				}
				b.WriteString(l)
			}
			s.mu.Lock()
			s.data = b.String()
			s.mu.Unlock()
			reply("250 queued")
		case cmd == "QUIT":
			reply("221 bye")
			return
		default:
			reply("250 ok")
		}
	}
}

func TestSendMailDeliversOverPlainSMTP(t *testing.T) {
	srv := startFakeSMTP(t)
	cfg := &SMTPConfig{Host: "127.0.0.1", Port: srv.port(), FromAddress: "alerts@example.test", TLSMode: "none"}

	err := (&EmailClient{}).sendMail(cfg, []string{" soc@example.test ", "oncall@example.test"}, "Subject line", "<p>hi</p>")
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.from != "alerts@example.test" {
		t.Errorf("MAIL FROM = %q", srv.from)
	}
	// Recipients are trimmed: a list typed as "a, b" must not fail on the space.
	if strings.Join(srv.rcpts, ",") != "soc@example.test,oncall@example.test" {
		t.Errorf("RCPT TO = %v", srv.rcpts)
	}
	if !strings.Contains(srv.data, "Subject: Subject line\r\n") || !strings.Contains(srv.data, "<p>hi</p>") {
		t.Errorf("DATA = %q", srv.data)
	}
	if srv.authSeen {
		t.Error("no credentials configured, so no AUTH should be attempted")
	}
}

func TestSendMailReportsARejectedRecipient(t *testing.T) {
	srv := startFakeSMTP(t)
	srv.rejectTo = "gone@example.test"
	cfg := &SMTPConfig{Host: "127.0.0.1", Port: srv.port(), FromAddress: "alerts@example.test", TLSMode: "none"}

	err := (&EmailClient{}).sendMail(cfg, []string{"gone@example.test"}, "s", "b")
	if err == nil || !strings.Contains(err.Error(), "RCPT TO <gone@example.test> failed") {
		t.Errorf("got %v", err)
	}
}

func TestSendMailReportsAnUnreachableServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	for _, mode := range []string{"none", "starttls", "implicit"} {
		cfg := &SMTPConfig{Host: "127.0.0.1", Port: port, FromAddress: "a@example.test", TLSMode: mode}
		if err := (&EmailClient{}).sendMail(cfg, []string{"b@example.test"}, "s", "b"); err == nil {
			t.Errorf("%s: a closed port must fail", mode)
		}
	}
}

// STARTTLS is required when configured: a server that does not offer it must fail
// the send rather than deliver in the clear.
func TestSendMailRefusesPlaintextWhenSTARTTLSIsConfigured(t *testing.T) {
	srv := startFakeSMTP(t)
	cfg := &SMTPConfig{Host: "127.0.0.1", Port: srv.port(), FromAddress: "a@example.test", TLSMode: "starttls"}
	err := (&EmailClient{}).sendMail(cfg, []string{"b@example.test"}, "s", "b")
	if err == nil || !strings.Contains(err.Error(), "STARTTLS") {
		t.Errorf("got %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.data != "" {
		t.Error("the message was delivered without TLS")
	}
}

func TestSendMailAuthenticatesWhenConfigured(t *testing.T) {
	srv := startFakeSMTP(t)
	cfg := &SMTPConfig{
		Host: "127.0.0.1", Port: srv.port(), FromAddress: "a@example.test", TLSMode: "none",
		Username: "user", Password: "pass",
	}
	// net/smtp allows PLAIN auth in the clear only to localhost, which this is.
	if err := (&EmailClient{}).sendMail(cfg, []string{"b@example.test"}, "s", "b"); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if !srv.authSeen {
		t.Error("configured credentials were not used")
	}
}
