package ingest

import (
	"bufio"
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
	"bifract/pkg/storage"
)

// RFC 5424: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID [SD] MSG
var rfc5424Re = regexp.MustCompile(
	`^<(\d{1,3})>(\d)\s+` + // <priority>version
		`(\S+)\s+` + // timestamp
		`(\S+)\s+` + // hostname
		`(\S+)\s+` + // app-name
		`(\S+)\s+` + // procid
		`(\S+)\s+` + // msgid
		`(.*)$`) // structured-data + message

// RFC 3164: <PRI>TIMESTAMP HOSTNAME TAG: MSG
// Timestamp format: Mmm dd hh:mm:ss
var rfc3164Re = regexp.MustCompile(
	`^<(\d{1,3})>` + // <priority>
		`([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+` + // timestamp
		`(\S+)\s+` + // hostname
		`(.*)$`) // tag + message

var facilityNames = []string{
	"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
	"uucp", "cron", "authpriv", "ftp", "ntp", "audit", "alert", "clock",
	"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
}

var severityNames = []string{
	"emergency", "alert", "critical", "error", "warning", "notice", "info", "debug",
}

func (h *IngestHandler) parseSyslogLogs(data []byte, token *ingesttokens.ValidatedToken) ([]storage.LogEntry, error) {
	var logs []storage.LogEntry

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), 10*1024*1024) // 10MB max line
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		entry := storage.LogEntry{
			RawLog: line,
			Fields: make(map[string]string),
		}

		parseSyslogLine(line, entry.Fields, token.Normalizer)

		ingestTime := time.Now()
		entry.Timestamp = h.extractTimestamp(entry.Fields, token.TimestampFields, token.Normalizer)
		if entry.Timestamp.IsZero() {
			entry.Timestamp = ingestTime
		}

		entry.IngestTimestamp = ingestTime
		entry.Fields["ingesttimestamp"] = ingestTime.Format(time.RFC3339Nano)
		entry.LogID = storage.GenerateLogID(entry.Timestamp, entry.RawLog)

		logs = append(logs, entry)
	}

	if len(logs) == 0 {
		return nil, fmt.Errorf("no valid syslog lines found")
	}
	return logs, nil
}

func parseSyslogLine(line string, fields map[string]string, norm *normalizers.CompiledNormalizer) {
	if tryParseRFC5424(line, fields, norm) {
		return
	}
	if tryParseRFC3164(line, fields, norm) {
		return
	}
	// Unparseable line: store the whole thing as the message
	fields[normalizeField("message", norm)] = line
}

func tryParseRFC5424(line string, fields map[string]string, norm *normalizers.CompiledNormalizer) bool {
	m := rfc5424Re.FindStringSubmatch(line)
	if m == nil {
		return false
	}

	pri, err := strconv.Atoi(m[1])
	if err != nil || pri > 191 {
		return false
	}

	decodePriority(pri, fields, norm)

	fields[normalizeField("version", norm)] = m[2]

	if ts := parseRFC5424Timestamp(m[3]); !ts.IsZero() {
		fields[normalizeField("timestamp", norm)] = ts.Format(time.RFC3339Nano)
	} else if m[3] != "-" {
		fields[normalizeField("timestamp", norm)] = m[3]
	}

	setIfNotNil(fields, "hostname", m[4], norm)
	setIfNotNil(fields, "appname", m[5], norm)
	setIfNotNil(fields, "procid", m[6], norm)
	setIfNotNil(fields, "msgid", m[7], norm)

	rest := m[8]
	sd, msg := extractStructuredData(rest)
	if sd != "" {
		fields[normalizeField("structured_data", norm)] = sd
	}
	if msg != "" {
		fields[normalizeField("message", norm)] = msg
	}

	return true
}

func tryParseRFC3164(line string, fields map[string]string, norm *normalizers.CompiledNormalizer) bool {
	m := rfc3164Re.FindStringSubmatch(line)
	if m == nil {
		return false
	}

	pri, err := strconv.Atoi(m[1])
	if err != nil || pri > 191 {
		return false
	}

	decodePriority(pri, fields, norm)

	if ts := parseRFC3164Timestamp(m[2]); !ts.IsZero() {
		fields[normalizeField("timestamp", norm)] = ts.Format(time.RFC3339Nano)
	}

	fields[normalizeField("hostname", norm)] = m[3]

	// Split tag: message from the remainder
	rest := m[4]
	if idx := strings.Index(rest, ": "); idx != -1 {
		tag := rest[:idx]
		msg := rest[idx+2:]
		// Tag may contain PID: "app[1234]"
		if bi := strings.Index(tag, "["); bi != -1 && strings.HasSuffix(tag, "]") {
			fields[normalizeField("appname", norm)] = tag[:bi]
			fields[normalizeField("procid", norm)] = tag[bi+1 : len(tag)-1]
		} else {
			fields[normalizeField("appname", norm)] = tag
		}
		fields[normalizeField("message", norm)] = msg
	} else if idx := strings.Index(rest, ":"); idx != -1 {
		tag := rest[:idx]
		msg := strings.TrimLeft(rest[idx+1:], " ")
		if bi := strings.Index(tag, "["); bi != -1 && strings.HasSuffix(tag, "]") {
			fields[normalizeField("appname", norm)] = tag[:bi]
			fields[normalizeField("procid", norm)] = tag[bi+1 : len(tag)-1]
		} else {
			fields[normalizeField("appname", norm)] = tag
		}
		fields[normalizeField("message", norm)] = msg
	} else {
		fields[normalizeField("message", norm)] = rest
	}

	return true
}

func decodePriority(pri int, fields map[string]string, norm *normalizers.CompiledNormalizer) {
	facility := pri / 8
	severity := pri % 8

	fields[normalizeField("priority", norm)] = strconv.Itoa(pri)
	fields[normalizeField("facility", norm)] = strconv.Itoa(facility)
	fields[normalizeField("severity", norm)] = strconv.Itoa(severity)

	if facility < len(facilityNames) {
		fields[normalizeField("facility_name", norm)] = facilityNames[facility]
	}
	if severity < len(severityNames) {
		fields[normalizeField("severity_name", norm)] = severityNames[severity]
	}
}

func setIfNotNil(fields map[string]string, key, val string, norm *normalizers.CompiledNormalizer) {
	if val != "-" && val != "" {
		fields[normalizeField(key, norm)] = val
	}
}

func extractStructuredData(s string) (sd, msg string) {
	if len(s) == 0 || s[0] != '[' {
		// No structured data, or starts with "-"
		if len(s) > 0 && s[0] == '-' {
			rest := strings.TrimLeft(s[1:], " ")
			return "", rest
		}
		return "", s
	}

	// Walk through balanced brackets
	depth := 0
	i := 0
	for i < len(s) {
		switch s[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				i++
				// There may be more SD elements
				if i < len(s) && s[i] == '[' {
					continue
				}
				sd = s[:i]
				msg = strings.TrimLeft(s[i:], " ")
				return sd, msg
			}
		case '"':
			// Skip quoted strings within SD (may contain ] )
			i++
			for i < len(s) && s[i] != '"' {
				if s[i] == '\\' {
					i++ // skip escaped char
				}
				i++
			}
		}
		i++
	}

	// Unbalanced brackets: treat entire thing as message
	return "", s
}

func parseRFC5424Timestamp(s string) time.Time {
	if s == "-" {
		return time.Time{}
	}
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999Z07:00",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func parseRFC3164Timestamp(s string) time.Time {
	// RFC 3164 timestamps lack a year, use current year
	formats := []string{
		"Jan  2 15:04:05",
		"Jan 2 15:04:05",
	}
	now := time.Now()
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			t = t.AddDate(now.Year(), 0, 0)
			// If parsed time is in the future (e.g. Dec log in Jan), use previous year
			if t.After(now.Add(24 * time.Hour)) {
				t = t.AddDate(-1, 0, 0)
			}
			return t
		}
	}
	return time.Time{}
}
