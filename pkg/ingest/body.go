package ingest

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// errBodyTooLarge signals the request exceeded the configured body limit,
// either on the wire or after gzip decompression.
var errBodyTooLarge = errors.New("request body too large")

// readRequestBody reads an ingest request body, enforcing maxBodySize and
// transparently decompressing Content-Encoding: gzip. The limit is applied to
// both the compressed and decompressed streams so a compression bomb cannot
// exhaust memory.
func readRequestBody(w http.ResponseWriter, r *http.Request, maxBodySize int64) ([]byte, error) {
	var reader io.Reader = r.Body
	if maxBodySize > 0 {
		reader = http.MaxBytesReader(w, r.Body, maxBodySize)
	}

	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("invalid gzip request body: %w", err)
		}
		defer gr.Close()
		if maxBodySize > 0 {
			reader = io.LimitReader(gr, maxBodySize+1)
		} else {
			reader = gr
		}
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		if strings.Contains(err.Error(), "request body too large") {
			return nil, errBodyTooLarge
		}
		return nil, err
	}
	if maxBodySize > 0 && int64(len(body)) > maxBodySize {
		return nil, errBodyTooLarge
	}
	return body, nil
}
