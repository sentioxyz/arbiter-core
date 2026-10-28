package serve

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// maxRequestBytes bounds a request body.
const maxRequestBytes = 64 << 10

type acquireRequest struct {
	Database   string `json:"database"`
	SnapshotID string `json:"snapshot_id"`
}

type acquireResponse struct {
	LeaseID string `json:"lease_id"`
	TTLMs   int64  `json:"ttl_ms"`
}

type leaseRequest struct {
	LeaseID string `json:"lease_id"`
}

type renewResponse struct {
	TTLMs int64 `json:"ttl_ms"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// Handler is the serving endpoint: GET /v1/serving and POST
// /v1/serving/{acquire,renew,release}. Every request needs the bearer token;
// bodies are JSON and errors are {"error": "<text>"}.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/serving", s.only(http.MethodGet, s.handleServing))
	mux.HandleFunc("/v1/serving/acquire", s.only(http.MethodPost, s.handleAcquire))
	mux.HandleFunc("/v1/serving/renew", s.only(http.MethodPost, s.handleRenew))
	mux.HandleFunc("/v1/serving/release", s.only(http.MethodPost, s.handleRelease))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("member serving: no route %s", r.URL.Path))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			writeError(w, http.StatusUnauthorized, "member serving: missing or wrong bearer token")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// authorized compares the bearer token in constant time (over SHA-256 sums,
// so neither the content nor the length of the token leaks).
func (s *Server) authorized(r *http.Request) bool {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || got == "" {
		return false
	}
	a, b := sha256.Sum256([]byte(got)), sha256.Sum256([]byte(s.cfg.Token))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

func (s *Server) only(method string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			writeError(w, http.StatusMethodNotAllowed, fmt.Sprintf("member serving: %s %s is not allowed; use %s", r.Method, r.URL.Path, method))
			return
		}
		h(w, r)
	}
}

func (s *Server) handleServing(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Serving())
}

func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	var req acquireRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("member serving: %v", err))
		return
	}
	if req.Database == "" || req.SnapshotID == "" {
		writeError(w, http.StatusBadRequest, "member serving: acquire needs a database and a snapshot_id")
		return
	}
	l, err := s.acquire(req.Database, req.SnapshotID)
	var refusal *acquireRefusal
	switch {
	case errors.As(err, &refusal):
		writeError(w, http.StatusConflict, refusal.msg)
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
	default:
		writeJSON(w, http.StatusOK, acquireResponse{LeaseID: l.id, TTLMs: s.cfg.LeaseTTL.Milliseconds()})
	}
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	id, ok := s.leaseID(w, r)
	if !ok {
		return
	}
	switch err := s.renew(id); {
	case errors.Is(err, errUnknownLease):
		writeError(w, http.StatusNotFound, fmt.Sprintf("member serving: unknown lease %s", id))
	case errors.Is(err, errRevokedLease):
		writeError(w, http.StatusConflict, fmt.Sprintf("member serving: lease %s is revoked", id))
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
	default:
		writeJSON(w, http.StatusOK, renewResponse{TTLMs: s.cfg.LeaseTTL.Milliseconds()})
	}
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	id, ok := s.leaseID(w, r)
	if !ok {
		return
	}
	s.release(id)
	writeJSON(w, http.StatusOK, struct{}{})
}

func (s *Server) leaseID(w http.ResponseWriter, r *http.Request) (string, bool) {
	var req leaseRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("member serving: %v", err))
		return "", false
	}
	if req.LeaseID == "" {
		writeError(w, http.StatusBadRequest, "member serving: lease_id is required")
		return "", false
	}
	return req.LeaseID, true
}

// decodeBody reads exactly one JSON object with known fields only.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("the body holds more than one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
