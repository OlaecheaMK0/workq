package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/OlaecheaMK0/workq/internal/queue"
	"github.com/OlaecheaMK0/workq/internal/worker"
)

//go:embed index.html
var dashboard string

type Repository interface {
	Ping(context.Context) error
	Enqueue(context.Context, string, queue.Request) (queue.Job, bool, error)
	Get(context.Context, string) (queue.Job, error)
	List(context.Context, string, int) ([]queue.Job, error)
	Stats(context.Context) (map[string]int64, error)
}

type API struct {
	Store Repository
	Token string
	Log   *slog.Logger
}

func (a *API) Handler() http.Handler {
	if a.Log == nil {
		a.Log = slog.Default()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, dashboard)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := a.Store.Ping(ctx); err != nil {
			writeError(w, 503, "database unavailable")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.Handle("POST /v1/jobs", a.authorize(http.HandlerFunc(a.enqueue)))
	mux.Handle("GET /v1/jobs", a.authorize(http.HandlerFunc(a.list)))
	mux.Handle("GET /v1/jobs/{id}", a.authorize(http.HandlerFunc(a.get)))
	mux.Handle("GET /v1/stats", a.authorize(http.HandlerFunc(a.stats)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func (a *API) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.Token != "" {
			got := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
			want := sha256.Sum256([]byte(a.Token))
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				writeError(w, 401, "valid bearer token required")
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *API) enqueue(w http.ResponseWriter, r *http.Request) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		writeError(w, 415, "Content-Type must be application/json")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 128 || strings.TrimSpace(key) != key {
		writeError(w, 400, "Idempotency-Key must contain 1 to 128 non-padded bytes")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var req queue.Request
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON request")
		return
	}
	if err := d.Decode(new(any)); err != io.EOF {
		writeError(w, 400, "request must contain one JSON object")
		return
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 3
	}
	if req.MaxAttempts < 1 || req.MaxAttempts > 10 {
		writeError(w, 400, "max_attempts must be between 1 and 10")
		return
	}
	if err := worker.Validate(req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	job, created, err := a.Store.Enqueue(r.Context(), key, req)
	if errors.Is(err, queue.ErrConflict) {
		writeError(w, 409, err.Error())
		return
	}
	if err != nil {
		a.internalError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+job.ID)
	status := 200
	if created {
		status = 201
	}
	writeJSON(w, status, map[string]any{"created": created, "job": job})
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !queue.ValidID(id) {
		writeError(w, 400, "invalid job ID")
		return
	}
	job, err := a.Store.Get(r.Context(), id)
	if errors.Is(err, queue.ErrNotFound) {
		writeError(w, 404, "job not found")
		return
	}
	if err != nil {
		a.internalError(w, err)
		return
	}
	writeJSON(w, 200, job)
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	switch state {
	case "", "queued", "running", "succeeded", "dead":
	default:
		writeError(w, 400, "invalid state filter")
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		var err error
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			writeError(w, 400, "limit must be between 1 and 100")
			return
		}
	}
	jobs, err := a.Store.List(r.Context(), state, limit)
	if err != nil {
		a.internalError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"jobs": jobs})
}

func (a *API) stats(w http.ResponseWriter, r *http.Request) {
	result, err := a.Store.Stats(r.Context())
	if err != nil {
		a.internalError(w, err)
		return
	}
	writeJSON(w, 200, result)
}

func (a *API) internalError(w http.ResponseWriter, err error) {
	a.Log.Error("API database operation failed", "error", err)
	writeError(w, 503, "queue temporarily unavailable")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
