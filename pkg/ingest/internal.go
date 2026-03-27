package ingest

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"

	"bifract/pkg/fractals"
	"bifract/pkg/normalizers"

	"github.com/go-chi/chi/v5"
)

// InternalIngestHandler handles log ingestion from trusted internal services
// (e.g. Caddy log shipper on the Docker network). No ingest token required;
// the fractal is identified by name in the URL path.
type InternalIngestHandler struct {
	queue             *IngestQueue
	maxBodySize       int64
	fractalManager    *fractals.Manager
	normalizerManager *normalizers.Manager
}

func NewInternalIngestHandler(queue *IngestQueue, maxBodySize int64, fm *fractals.Manager, nm *normalizers.Manager) *InternalIngestHandler {
	return &InternalIngestHandler{
		queue:             queue,
		maxBodySize:       maxBodySize,
		fractalManager:    fm,
		normalizerManager: nm,
	}
}

// HandleInternalIngest accepts logs for a named fractal without token auth.
// Must be behind InternalOnlyMiddleware.
func (h *InternalIngestHandler) HandleInternalIngest(w http.ResponseWriter, r *http.Request) {
	fractalName := chi.URLParam(r, "fractal")
	if fractalName == "" {
		respondJSON(w, http.StatusBadRequest, IngestResponse{
			Success: false,
			Error:   "Fractal name is required",
		})
		return
	}

	fractal, err := h.fractalManager.GetFractalByName(r.Context(), fractalName)
	if err != nil {
		respondJSON(w, http.StatusNotFound, IngestResponse{
			Success: false,
			Error:   fmt.Sprintf("Fractal %q not found", fractalName),
		})
		return
	}

	bodyReader := r.Body
	if h.maxBodySize > 0 {
		bodyReader = http.MaxBytesReader(w, r.Body, h.maxBodySize)
	}

	body, err := io.ReadAll(bodyReader)
	if err != nil {
		if err.Error() == "http: request body too large" {
			respondJSON(w, http.StatusRequestEntityTooLarge, IngestResponse{
				Success: false,
				Error:   fmt.Sprintf("Request body exceeds %d byte limit", h.maxBodySize),
			})
			return
		}
		respondJSON(w, http.StatusBadRequest, IngestResponse{
			Success: false,
			Error:   "Failed to read request body",
		})
		return
	}
	defer r.Body.Close()

	// Parse using the default normalizer
	var compiled *normalizers.CompiledNormalizer
	if h.normalizerManager != nil {
		defaultNorm, normErr := h.normalizerManager.GetDefault(r.Context())
		if normErr != nil {
			log.Printf("[InternalIngest] no default normalizer found, ingesting without normalization: %v", normErr)
		} else {
			compiled = defaultNorm.Compile()
		}
	}
	parser := &IngestHandler{}
	logs, err := parser.parseJSONLogsWithConfig(body, compiled, nil)
	if err != nil {
		respondJSON(w, http.StatusBadRequest, IngestResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to parse logs: %v", err),
		})
		return
	}

	if len(logs) == 0 {
		respondJSON(w, http.StatusBadRequest, IngestResponse{
			Success: false,
			Error:   "No valid logs found in request",
		})
		return
	}

	for i := range logs {
		logs[i].FractalID = fractal.ID
	}

	if !h.queue.Enqueue(logs) {
		msg := "Ingestion queue full. Retry after backoff."
		if !h.queue.Healthy() {
			msg = "Ingestion backend under pressure. Retry after backoff."
		}
		w.Header().Set("Retry-After", "2")
		respondJSON(w, http.StatusTooManyRequests, IngestResponse{
			Success: false,
			Error:   msg,
		})
		return
	}

	respondJSON(w, http.StatusOK, IngestResponse{
		Success: true,
		Count:   len(logs),
		Message: fmt.Sprintf("Accepted %d log(s) for ingestion", len(logs)),
	})
}

// InternalOnlyMiddleware rejects requests that did not originate from a
// direct connection on a private network. It checks two things:
//  1. No proxy headers (X-Forwarded-For, X-Real-IP): their presence means
//     the request was forwarded by Caddy (or another reverse proxy), so the
//     caller is external even though RemoteAddr is now a private Docker IP.
//  2. RemoteAddr belongs to a private/loopback/link-local range.
func InternalOnlyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Forwarded-For") != "" || r.Header.Get("X-Real-IP") != "" {
			respondJSON(w, http.StatusForbidden, IngestResponse{
				Success: false,
				Error:   "Internal endpoint: access denied",
			})
			return
		}
		if !isPrivateAddr(r.RemoteAddr) {
			respondJSON(w, http.StatusForbidden, IngestResponse{
				Success: false,
				Error:   "Internal endpoint: access denied",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isPrivateAddr returns true if addr (host:port or bare IP) belongs to a
// private, loopback, or link-local network.
func isPrivateAddr(addr string) bool {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if idx := strings.IndexByte(host, '%'); idx != -1 {
		host = host[:idx]
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	privateNets := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"::1/128",
		"fc00::/7",
		"fe80::/10",
	}
	for _, cidr := range privateNets {
		_, network, _ := net.ParseCIDR(cidr)
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
