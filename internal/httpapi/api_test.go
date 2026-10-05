package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OlaecheaMK0/workq/internal/queue"
)

type fakeStore struct {
	calls   int
	err     error
	created bool
}

func (f *fakeStore) Ping(context.Context) error { return f.err }
func (f *fakeStore) Enqueue(_ context.Context, _ string, req queue.Request) (queue.Job, bool, error) {
	f.calls++
	return queue.Job{ID: "12345678-1234-1234-1234-123456789abc", Kind: req.Kind, Payload: req.Payload, MaxAttempts: req.MaxAttempts, State: "queued", LeaseToken: "must-not-leak"}, f.created, f.err
}
func (f *fakeStore) Get(context.Context, string) (queue.Job, error) { return queue.Job{}, f.err }
func (f *fakeStore) List(context.Context, string, int) ([]queue.Job, error) {
	return []queue.Job{}, f.err
}
func (f *fakeStore) Stats(context.Context) (map[string]int64, error) {
	return map[string]int64{}, f.err
}

func handler(f *fakeStore, token string) http.Handler {
	a := API{Store: f, Token: token, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return a.Handler()
}
func post(h http.Handler, body, key, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestSubmitDefaultsAndIdempotentStatus(t *testing.T) {
	f := &fakeStore{created: true}
	h := handler(f, "")
	body := `{"kind":"report","payload":{"name":"demo","items":[1,2]}}`
	response := post(h, body, "request-1", "")
	if response.Code != 201 || response.Header().Get("Location") == "" {
		t.Fatalf("response %d %s", response.Code, response.Body.String())
	}
	var data struct {
		Job queue.Job `json:"job"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if data.Job.MaxAttempts != 3 {
		t.Fatal("default attempt cap missing")
	}
	if strings.Contains(response.Body.String(), "must-not-leak") {
		t.Fatal("lease token leaked")
	}
	f.created = false
	if got := post(h, body, "request-1", "").Code; got != 200 {
		t.Fatal(got)
	}
	f.err = queue.ErrConflict
	if got := post(h, body, "request-1", "").Code; got != 409 {
		t.Fatal(got)
	}
}

func TestRejectUnsafeOrMalformedRequestsBeforeEnqueue(t *testing.T) {
	f := &fakeStore{}
	h := handler(f, "")
	cases := []struct{ body, key string }{
		{`{"kind":"report","payload":{"name":"demo","items":[1]}}`, ""},
		{`{"kind":"email","payload":{"name":"demo","items":[1]}}`, "x"},
		{`{"kind":"report","payload":{"name":"demo","items":[1]},"max_attempts":11}`, "x"},
		{`{"kind":"report","payload":{"name":"demo","items":[1],"unknown":true}}`, "x"},
		{`{"kind":"report","payload":{"name":"demo","items":[1],"delay_ms":10001}}`, "x"},
		{`{"kind":"report","payload":{"name":"demo","items":[1]}} {}`, "x"},
		{`{"kind":"report","payload":{"name":"demo","items":[1]},"typo":true}`, "x"},
		{`{"kind":"report","payload":{"name":"demo","items":[1000001]}}`, "x"},
	}
	for _, tc := range cases {
		if got := post(h, tc.body, tc.key, "").Code; got != 400 {
			t.Fatalf("body=%s status=%d", tc.body, got)
		}
	}
	if f.calls != 0 {
		t.Fatal("invalid request reached queue")
	}
}

func TestAuthenticationAndHealth(t *testing.T) {
	f := &fakeStore{created: true}
	h := handler(f, "local-test-token")
	body := `{"kind":"report","payload":{"name":"demo","items":[1]}}`
	for _, token := range []string{"", "incorrect"} {
		if got := post(h, body, "x", token).Code; got != 401 {
			t.Fatal(got)
		}
	}
	if got := post(h, body, "x", "local-test-token").Code; got != 201 {
		t.Fatal(got)
	}
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	f.err = queue.ErrNotFound
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}

func TestBadIDsAndFilters(t *testing.T) {
	h := handler(&fakeStore{}, "")
	for _, path := range []string{"/v1/jobs/not-a-uuid", "/v1/jobs?state=unknown", "/v1/jobs?limit=101", "/v1/jobs?limit=-1"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 400 {
			t.Fatalf("%s: %d", path, w.Code)
		}
	}
}
