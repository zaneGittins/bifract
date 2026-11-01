package ingest

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
	"bifract/pkg/storage"
	"bifract/pkg/watermark"
)

// HECHandler accepts the Splunk HTTP Event Collector protocol, so the OTel
// splunk_hec exporter, Vector, Fluent Bit, Cribl and other HEC senders can send
// to Bifract with only a URL and token change.
type HECHandler struct {
	handler *IngestHandler
}

// HECResponse is the body Splunk HEC answers every request with.
type HECResponse struct {
	Text               string `json:"text"`
	Code               int    `json:"code"`
	InvalidEventNumber *int   `json:"invalid-event-number,omitempty"`
}

// HEC status codes, as Splunk numbers them.
const (
	hecSuccess        = 0
	hecTokenRequired  = 2
	hecInvalidAuth    = 3
	hecInvalidToken   = 4
	hecNoData         = 5
	hecInvalidFormat  = 6
	hecServerBusy     = 9
	hecEventRequired  = 12
	hecEventBlank     = 13
	hecIndexedFields  = 15
	hecHealthy        = 17
	hecQueuesAreFull  = 18
	hecSuccessMessage = "Success"
)

// hecError is a request the parser refused, with the response to send.
type hecError struct {
	status int
	resp   HECResponse
}

func newHECError(status, code int, text string, event int) *hecError {
	e := &hecError{status: status, resp: HECResponse{Text: text, Code: code}}
	if event >= 0 {
		e.resp.InvalidEventNumber = &event
	}
	return e
}

// hecMeta is the Splunk metadata stamped onto every event as fields.
type hecMeta struct {
	host, source, sourcetype, index string
}

func (m hecMeta) pairs() [4][2]string {
	return [4][2]string{{"host", m.host}, {"source", m.source}, {"sourcetype", m.sourcetype}, {"index", m.index}}
}

// set writes each non-empty value into obj, over an existing key only if overwrite.
func (m hecMeta) set(obj map[string]any, overwrite bool) {
	for _, kv := range m.pairs() {
		if kv[1] == "" {
			continue
		}
		if _, exists := obj[kv[0]]; exists && !overwrite {
			continue
		}
		obj[kv[0]] = kv[1]
	}
}

// or returns m with its empty values taken from def.
func (m hecMeta) or(def hecMeta) hecMeta {
	pick := func(v, d string) string {
		if v != "" {
			return v
		}
		return d
	}
	return hecMeta{pick(m.host, def.host), pick(m.source, def.source), pick(m.sourcetype, def.sourcetype), pick(m.index, def.index)}
}

func envelopeMeta(env map[string]any) hecMeta {
	str := func(k string) string {
		s, _ := env[k].(string)
		return s
	}
	return hecMeta{str("host"), str("source"), str("sourcetype"), str("index")}
}

// envelopeTime reads the epoch seconds HEC senders put in time, as a number in
// any notation or a string. Anything else is no time.
func envelopeTime(v any) time.Time {
	var s string
	switch t := v.(type) {
	case json.Number:
		s = string(t)
	case string:
		s = t
	default:
		return time.Time{}
	}
	ts, _ := parseEpoch(normalizers.PlainNumber(s), time.Second)
	return ts
}

// NewHECHandler creates a HEC handler sharing the standard endpoint's queue.
func NewHECHandler(handler *IngestHandler) *HECHandler {
	return &HECHandler{handler: handler}
}

// HandleEvent serves /services/collector/event: JSON envelopes, concatenated.
func (h *HECHandler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	h.handleBatch(w, r, h.parseEvents)
}

// HandleRaw serves /services/collector/raw: one event per line.
func (h *HECHandler) HandleRaw(w http.ResponseWriter, r *http.Request) {
	h.handleBatch(w, r, h.parseRaw)
}

// HandleHealth serves /services/collector/health, which load balancers and
// shippers poll unauthenticated.
func (h *HECHandler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if !h.handler.queue.Healthy() {
		respondJSON(w, http.StatusServiceUnavailable, HECResponse{Text: "HEC is unhealthy, queues are full", Code: hecQueuesAreFull})
		return
	}
	respondJSON(w, http.StatusOK, HECResponse{Text: "HEC is healthy", Code: hecHealthy})
}

type hecParser func(body []byte, meta hecMeta, token *ingesttokens.ValidatedToken) ([]storage.LogEntry, *hecError)

func (h *HECHandler) handleBatch(w http.ResponseWriter, r *http.Request, parse hecParser) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Splunk also takes the token as the Basic auth password, with any user name.
	rawToken := ingestTokenFromHeader(r)
	if rawToken == "" {
		_, rawToken, _ = r.BasicAuth()
	}
	tokenData, err := h.handler.lookupIngestToken(r.Context(), rawToken)
	if err != nil {
		status, msg := tokenErrorStatus(w, err)
		code := hecServerBusy
		switch {
		case status != http.StatusUnauthorized:
		case r.Header.Get("Authorization") == "":
			code, msg = hecTokenRequired, "Token is required"
		case rawToken == "":
			code, msg = hecInvalidAuth, "Invalid authorization"
		default:
			status, code, msg = http.StatusForbidden, hecInvalidToken, "Invalid token"
		}
		respondJSON(w, status, HECResponse{Text: msg, Code: code})
		return
	}

	if !h.handler.queue.Healthy() {
		w.Header().Set("Retry-After", "2")
		respondJSON(w, http.StatusServiceUnavailable, HECResponse{Text: "Server is busy", Code: hecServerBusy})
		return
	}

	defer r.Body.Close()
	body, err := readRequestBody(w, r, h.handler.maxBodySize)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			respondJSON(w, http.StatusRequestEntityTooLarge, HECResponse{
				Text: fmt.Sprintf("Request body exceeds %d byte limit", h.handler.maxBodySize), Code: hecInvalidFormat,
			})
			return
		}
		respondJSON(w, http.StatusBadRequest, HECResponse{Text: "Failed to read request body", Code: hecInvalidFormat})
		return
	}

	// Rows are stamped from here on, so hold the watermark until they are queued.
	stampHold := watermark.Local.Hold()
	defer watermark.Local.Done(stampHold)

	q := r.URL.Query()
	meta := hecMeta{host: q.Get("host"), source: q.Get("source"), sourcetype: q.Get("sourcetype"), index: q.Get("index")}
	logs, perr := parse(body, meta, tokenData)

	// As in Splunk, events before a bad one are kept and the error names its
	// position, and busy is a 503: older Fluent Bit drops a batch answered 429.
	if len(logs) > 0 {
		switch err := h.handler.submit(w, tokenData, logs); {
		case errors.Is(err, errQuotaExceeded):
			respondJSON(w, http.StatusServiceUnavailable, HECResponse{Text: "Server is busy: fractal disk quota exceeded", Code: hecServerBusy})
			return
		case err != nil:
			respondJSON(w, http.StatusServiceUnavailable, HECResponse{Text: "Server is busy", Code: hecServerBusy})
			return
		}
	}
	if perr != nil {
		respondJSON(w, perr.status, perr.resp)
		return
	}
	respondJSON(w, http.StatusOK, HECResponse{Text: hecSuccessMessage, Code: hecSuccess})
}

// parseEvents reads concatenated envelopes. Envelope metadata wins over the
// fields object, which wins over event keys; query string defaults fill only
// what none of them set.
func (h *HECHandler) parseEvents(body []byte, defaults hecMeta, token *ingesttokens.ValidatedToken) ([]storage.LogEntry, *hecError) {
	dec := jsontext.NewDecoder(bytes.NewReader(body),
		jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	var logs []storage.LogEntry
	for i := 0; ; i++ {
		v, err := normalizers.DecodeValue(dec)
		if err == io.EOF {
			if i == 0 {
				return nil, newHECError(http.StatusBadRequest, hecNoData, "No data", -1)
			}
			return logs, nil
		}
		env, isObj := v.(map[string]any)
		if err != nil || !isObj {
			return logs, newHECError(http.StatusBadRequest, hecInvalidFormat, "Invalid data format", i)
		}
		extra, isObj := env["fields"].(map[string]any)
		if !isObj && env["fields"] != nil {
			return logs, newHECError(http.StatusBadRequest, hecIndexedFields, "Error in handling indexed fields", i)
		}
		meta := envelopeMeta(env)
		ts := envelopeTime(env["time"])

		var entry storage.LogEntry
		switch ev := env["event"].(type) {
		case nil:
			return logs, newHECError(http.StatusBadRequest, hecEventRequired, "Event field is required", i)
		case string:
			if strings.TrimSpace(ev) == "" {
				return logs, newHECError(http.StatusBadRequest, hecEventBlank, "Event field cannot be blank", i)
			}
			entry = h.stringEntry(ev, extra, meta, defaults, ts, token)
		case map[string]any:
			raw, _ := json.Marshal(ev) // cannot fail: ev was decoded from JSON
			entry = buildHECEntry(ev, string(raw), extra, meta, defaults, ts, token)
		default:
			raw, _ := json.Marshal(ev)
			entry = buildHECEntry(map[string]any{"message": string(raw)}, string(raw), extra, meta, defaults, ts, token)
		}
		logs = append(logs, entry)
	}
}

// parseRaw reads one event per non-blank line, with metadata from the query string.
func (h *HECHandler) parseRaw(body []byte, meta hecMeta, token *ingesttokens.ValidatedToken) ([]storage.LogEntry, *hecError) {
	var logs []storage.LogEntry
	scanner := newLineScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		logs = append(logs, h.stringEntry(line, nil, hecMeta{}, meta, time.Time{}, token))
	}
	if err := lineScanErr(scanner); err != nil {
		return nil, newHECError(http.StatusRequestEntityTooLarge, hecInvalidFormat, err.Error(), -1)
	}
	if len(logs) == 0 {
		return nil, newHECError(http.StatusBadRequest, hecNoData, "No data", -1)
	}
	return logs, nil
}

// stringEntry reads a text event with the token's parser, as /api/v1/ingest would.
func (h *HECHandler) stringEntry(text string, extra map[string]any, meta, defaults hecMeta, ts time.Time, token *ingesttokens.ValidatedToken) storage.LogEntry {
	obj := map[string]any{}
	switch token.ParserType {
	case "kv", "syslog":
		fields := map[string]string{}
		if token.ParserType == "kv" {
			h.handler.parseKVLine(text, fields)
		} else {
			parseSyslogLine(text, fields)
		}
		for k, v := range fields {
			obj[k] = v
		}
	default:
		if normalizers.DecodeJSON([]byte(text), &obj) != nil || obj == nil {
			obj = map[string]any{"message": text}
		}
	}
	return buildHECEntry(obj, text, extra, meta, defaults, ts, token)
}

func buildHECEntry(obj map[string]any, raw string, extra map[string]any, meta, defaults hecMeta, ts time.Time, token *ingesttokens.ValidatedToken) storage.LogEntry {
	for k, v := range extra {
		obj[k] = v
	}
	meta.set(obj, true)
	defaults.set(obj, false)
	entry := buildLogEntry(obj, raw, ts, token.Normalizer, token.TimestampFields)

	// The raw log is only the event, so the metadata joins it in the log id;
	// otherwise the same line from two hosts would share one.
	if len(extra) > 0 || meta != (hecMeta{}) || defaults != (hecMeta{}) {
		key := []string{raw}
		for _, kv := range meta.or(defaults).pairs() {
			key = append(key, kv[1])
		}
		if len(extra) > 0 {
			b, _ := json.Marshal(extra)
			key = append(key, string(b))
		}
		entry.LogID = storage.GenerateLogID(entry.Timestamp, strings.Join(key, "\x00"))
	}
	return entry
}
