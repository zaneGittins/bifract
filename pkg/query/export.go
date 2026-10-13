package query

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"bifract/pkg/api"
	"bifract/pkg/storage"
)

// exportStallTimeout cuts off a client that stops reading, which would otherwise
// hold the ClickHouse scan open indefinitely.
const exportStallTimeout = 2 * time.Minute

const errExportNotFilter = "Only filter queries can be exported; aggregations, sorts, charts, source commands, join() and result-set bindings cannot"

// exportBaseColumns lead every line; norm_log is written last as "fields".
var exportBaseColumns = []string{"timestamp", "log_id", "fractal_id"}

var (
	errExportClientGone = errors.New("export client gone")
	errExportEncode     = errors.New("export encode")
)

// HandleExport streams every log a filter query matches as JSON Lines, newest
// first, from one unlimited low-priority ClickHouse scan.
func (h *QueryHandler) HandleExport(w http.ResponseWriter, r *http.Request) {
	prep := h.prepareQuery(w, r, purposeExport)
	if prep == nil {
		return
	}
	if !prep.emptyScope {
		if !prep.streamable || prep.translated.RowCapped {
			api.WriteError(w, http.StatusBadRequest, errExportNotFilter)
			return
		}
		if len(prep.warnings) > 0 {
			api.WriteError(w, http.StatusBadRequest, "The export would be incomplete: "+strings.Join(prep.warnings, " "))
			return
		}
	}

	tag := requestTag(r, prep.req)
	tag.Source = storage.SourceExport
	scope := "fractal=" + prep.selectedIndex
	if prep.prismID != "" {
		scope = "prism=" + prep.prismID
	}

	count := 0
	outcome := "failed"
	startWall := time.Now()
	defer func() {
		go h.logQueryAudit(prep.req.Query, "export", prep.selectedIndex, tag.User, time.Since(startWall).Milliseconds(), count,
			map[string]string{"prism_id": prep.prismID, "outcome": outcome})
	}()

	queryID := newSearchID()
	ctx, cancel := context.WithCancel(storage.TagContext(r.Context(), tag))
	defer func() {
		cancel()
		if outcome != "complete" {
			h.abandonSearch(queryID)
		}
	}()

	rc := http.NewResponseController(w)
	var out *bufio.Writer
	var gz *gzip.Writer
	started := false
	start := func() {
		started = true
		hdr := w.Header()
		hdr.Set("Content-Type", "application/x-ndjson")
		hdr.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="bifract-export-%s.jsonl"`, time.Now().UTC().Format("20060102T150405Z")))
		hdr.Set("Cache-Control", "no-store")
		hdr.Set("Vary", "Accept-Encoding")
		hdr.Set("Trailer", api.ExportRowsTrailer)
		var dst io.Writer = stallWriter{w: w, rc: rc}
		if acceptsGzip(r.Header.Values("Accept-Encoding")) {
			hdr.Set("Content-Encoding", "gzip")
			gz, _ = gzip.NewWriterLevel(dst, gzip.BestSpeed)
			dst = gz
		}
		w.WriteHeader(http.StatusOK)
		out = bufio.NewWriterSize(dst, 64<<10)
	}

	var line exportLine
	onRow := func(row map[string]interface{}) error {
		if !started {
			start()
			line.init(row, prep.fieldOrder)
		}
		if err := line.write(out, row); err != nil {
			if errors.Is(err, errExportEncode) {
				return err
			}
			return errExportClientGone
		}
		count++
		return nil
	}

	var streamErr error
	if !prep.emptyScope {
		streamErr = h.db.StreamLowPriority(ctx, queryID, prep.sql, onRow)
	}
	if streamErr == nil {
		if !started {
			start()
		}
		if err := out.Flush(); err != nil {
			streamErr = errExportClientGone
		} else if gz != nil {
			if err := gz.Close(); err != nil {
				streamErr = errExportClientGone
			}
		}
	}

	if streamErr != nil {
		if errors.Is(streamErr, errExportClientGone) || r.Context().Err() != nil {
			outcome = "cancelled"
			log.Printf("[Export] Client left after %d rows (user=%s %s)", count, tag.User, scope)
			return
		}
		log.Printf("[Export] Failed after %d rows (user=%s %s): %v", count, tag.User, scope, streamErr)
		if !started {
			msg, ok := clickhouseUserMessage(streamErr)
			if !ok {
				msg = "Export failed"
			}
			api.WriteError(w, http.StatusInternalServerError, msg)
			return
		}
		// Headers are gone, so abort the connection rather than end the body
		// cleanly; the missing trailer and gzip footer mark the file incomplete.
		panic(http.ErrAbortHandler)
	}

	outcome = "complete"
	w.Header().Set(api.ExportRowsTrailer, strconv.Itoa(count))
	log.Printf("[Export] Exported %d rows in %s (user=%s %s)", count, time.Since(startWall).Round(time.Millisecond), tag.User, scope)
}

// acceptsGzip reports whether an Accept-Encoding header admits gzip, honoring q=0.
func acceptsGzip(values []string) bool {
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			coding, params, _ := strings.Cut(part, ";")
			if !strings.EqualFold(strings.TrimSpace(coding), "gzip") {
				continue
			}
			q, ok := strings.CutPrefix(strings.ReplaceAll(params, " ", ""), "q=")
			if !ok {
				return true
			}
			f, err := strconv.ParseFloat(q, 64)
			return err == nil && f > 0
		}
	}
	return false
}

// stallWriter pushes the write deadline forward on every write, so only a
// client that stops reading entirely trips it.
type stallWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (s stallWriter) Write(p []byte) (int, error) {
	_ = s.rc.SetWriteDeadline(time.Now().Add(exportStallTimeout))
	return s.w.Write(p)
}

// exportLine renders a result row as one JSON object: the base columns, the
// columns the query selected, then the normalized log as a nested "fields"
// object copied verbatim from norm_log.
type exportLine struct {
	extras  []string
	keys    map[string][]byte // pre-encoded `"name":` per column
	hasNorm bool
	scratch bytes.Buffer
	enc     *json.Encoder
}

func (l *exportLine) init(row map[string]interface{}, fieldOrder []string) {
	l.enc = json.NewEncoder(&l.scratch)
	l.enc.SetEscapeHTML(false)
	_, l.hasNorm = row["norm_log"]
	l.keys = map[string][]byte{}
	for _, k := range exportBaseColumns {
		l.addKey(k)
	}
	for _, k := range fieldOrder {
		_, present := row[k]
		taken := slices.Contains(exportBaseColumns, k) || slices.Contains(l.extras, k) || k == "norm_log" || (l.hasNorm && k == "fields")
		if present && !taken {
			l.extras = append(l.extras, k)
			l.addKey(k)
		}
	}
}

func (l *exportLine) addKey(k string) {
	l.scratch.Reset()
	_ = l.enc.Encode(k)
	l.keys[k] = append(bytes.TrimSuffix(bytes.Clone(l.scratch.Bytes()), []byte("\n")), ':')
}

func (l *exportLine) write(out *bufio.Writer, row map[string]interface{}) error {
	if len(l.extras) > 0 {
		sanitizeFloats(row)
	}
	out.WriteByte('{')
	first := true
	sep := func() {
		if !first {
			out.WriteByte(',')
		}
		first = false
	}
	for _, k := range exportBaseColumns {
		v, ok := row[k]
		if !ok {
			continue
		}
		sep()
		out.Write(l.keys[k])
		if k == "timestamp" {
			if s, ok := v.(string); ok && writeCHTimestamp(out, s) {
				continue
			}
		}
		if err := l.value(out, v); err != nil {
			return err
		}
	}
	for _, k := range l.extras {
		sep()
		out.Write(l.keys[k])
		if err := l.value(out, row[k]); err != nil {
			return err
		}
	}
	if l.hasNorm {
		// norm_log is ClickHouse's own serialization of the fields column, so it is
		// always a JSON object; only the string bytes inside it go unvalidated.
		norm, _ := row["norm_log"].(string)
		if !utf8.ValidString(norm) {
			norm = strings.ToValidUTF8(norm, "�")
		}
		sep()
		if strings.HasPrefix(norm, "{") {
			out.WriteString(`"fields":`)
			out.WriteString(norm)
		} else {
			out.WriteString(`"norm_log":`)
			if err := l.value(out, norm); err != nil {
				return err
			}
		}
	}
	out.WriteByte('}')
	_, err := out.WriteString("\n")
	return err
}

// writeCHTimestamp rewrites ClickHouse's UTC "2006-01-02 15:04:05.000" text as
// quoted RFC3339 without parsing it, reporting false for any other shape.
func writeCHTimestamp(out *bufio.Writer, s string) bool {
	if len(s) != len("2006-01-02 15:04:05.000") || s[4] != '-' || s[10] != ' ' || s[19] != '.' {
		return false
	}
	out.WriteByte('"')
	out.WriteString(s[:10])
	out.WriteByte('T')
	out.WriteString(s[11:])
	out.WriteString(`Z"`)
	return true
}

// value encodes v without the trailing newline json.Encoder appends.
func (l *exportLine) value(out *bufio.Writer, v interface{}) error {
	l.scratch.Reset()
	if err := l.enc.Encode(v); err != nil {
		return fmt.Errorf("%w: %v", errExportEncode, err)
	}
	_, err := out.Write(bytes.TrimSuffix(l.scratch.Bytes(), []byte("\n")))
	return err
}
