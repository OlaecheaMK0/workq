package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OlaecheaMK0/workq/internal/queue"
	"github.com/OlaecheaMK0/workq/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T) *queue.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PostgreSQL integration test: set TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	id, err := queue.NewID()
	if err != nil {
		t.Fatal(err)
	}
	name := "workq_test_" + strings.ReplaceAll(id, "-", "")
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	cfg.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	store := &queue.Store{Pool: pool}
	t.Cleanup(func() {
		store.Close()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+ident+" CASCADE"); err != nil {
			t.Errorf("clean own test schema: %v", err)
		}
		admin.Close()
	})
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return store
}

func request(payload string, attempts int) queue.Request {
	return queue.Request{Kind: "report", Payload: json.RawMessage(payload), MaxAttempts: attempts}
}

func enqueue(t *testing.T, s *queue.Store, key string, req queue.Request) queue.Job {
	t.Helper()
	job, created, err := s.Enqueue(context.Background(), key, req)
	if err != nil || !created {
		t.Fatalf("enqueue: created=%v err=%v", created, err)
	}
	return job
}

func expire(t *testing.T, s *queue.Store, id string) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(), `UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentIdempotencyAndConflict(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	req := request(`{"name":"batch","items":[1,2]}`, 3)
	var wg sync.WaitGroup
	var created atomic.Int32
	ids := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, c, err := s.Enqueue(ctx, "same-key", req)
			if err != nil {
				t.Error(err)
				return
			}
			if c {
				created.Add(1)
			}
			ids <- j.ID
		}()
	}
	wg.Wait()
	close(ids)
	if created.Load() != 1 {
		t.Fatalf("created %d jobs", created.Load())
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("same key returned multiple IDs")
		}
	}
	_, c, err := s.Enqueue(ctx, "same-key", request(`{"items":[1,2],"name":"batch"}`, 3))
	if err != nil || c {
		t.Fatalf("equivalent payload: %v created=%v", err, c)
	}
	_, _, err = s.Enqueue(ctx, "same-key", request(`{"name":"changed","items":[1,2]}`, 3))
	if !errors.Is(err, queue.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
}

func TestConcurrentWorkersClaimEachJobOnce(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	for i := 0; i < 40; i++ {
		enqueue(t, s, fmt.Sprintf("job-%d", i), request(`{"name":"batch","items":[1]}`, 3))
	}
	var wg sync.WaitGroup
	seen := make(chan string, 40)
	for n := 0; n < 10; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, err := s.Claim(ctx, 5*time.Second)
				if errors.Is(err, queue.ErrNotFound) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				seen <- j.ID
				if err := s.Complete(ctx, j, json.RawMessage(`{"ok":true}`)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(seen)
	unique := map[string]bool{}
	for id := range seen {
		if unique[id] {
			t.Fatalf("duplicate claim for %s", id)
		}
		unique[id] = true
	}
	if len(unique) != 40 {
		t.Fatalf("claimed %d/40", len(unique))
	}
	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats["succeeded"] != 40 {
		t.Fatal(stats)
	}
}

func TestRetryVisibilityAndAttemptCap(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	original := enqueue(t, s, "poison", request(`{"name":"batch","items":[1]}`, 2))
	j, err := s.Claim(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, j, "temporary", time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, time.Second); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("delayed job became visible: %v", err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE jobs SET run_at=now() WHERE id=$1`, original.ID); err != nil {
		t.Fatal(err)
	}
	j, err = s.Claim(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if j.Attempts != 2 {
		t.Fatal(j.Attempts)
	}
	if err := s.Fail(ctx, j, "still failing", 0); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "dead" || got.Attempts != 2 || got.LastError == nil || *got.LastError != "still failing" {
		t.Fatalf("unexpected dead letter: %+v", got)
	}
	if _, err := s.Claim(ctx, time.Second); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("dead job claimed: %v", err)
	}
}

func TestCrashRecoveryFencesStaleOwner(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	enqueue(t, s, "crash", request(`{"name":"batch","items":[1]}`, 2))
	old, err := s.Claim(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	expire(t, s, old.ID)
	count, err := s.RecoverExpired(ctx)
	if err != nil || count != 1 {
		t.Fatalf("recover count=%d err=%v", count, err)
	}
	next, err := s.Claim(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if old.LeaseToken == next.LeaseToken || next.Attempts != 2 {
		t.Fatal("ownership did not change")
	}
	for _, err := range []error{s.Complete(ctx, old, json.RawMessage(`{}`)), s.Fail(ctx, old, "stale", 0), s.Heartbeat(ctx, old, time.Second)} {
		if !errors.Is(err, queue.ErrLeaseLost) {
			t.Fatalf("old owner wrote after recovery: %v", err)
		}
	}
	expire(t, s, next.ID)
	if _, err := s.RecoverExpired(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, next.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "dead" || got.Attempts != 2 {
		t.Fatalf("crash cap not enforced: %+v", got)
	}
}

func TestReportEffectSurvivesCrashBeforeAcknowledgment(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	enqueue(t, s, "effect-crash", request(`{"name":"batch","items":[10,20]}`, 3))
	old, err := s.Claim(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handler := worker.ReportHandler(s)
	first, err := handler(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate process death after the durable effect, before Complete.
	expire(t, s, old.ID)
	if _, err := s.RecoverExpired(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := s.Claim(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err := handler(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("replayed effect differs: %s / %s", first, second)
	}
	if err := s.Complete(ctx, next, second); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM report_results WHERE job_id=$1`, old.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("side effect written %d times", count)
	}
}

func TestWorkerHeartbeatAndRetryEndToEnd(t *testing.T) {
	s := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	slow := enqueue(t, s, "slow", request(`{"name":"slow","items":[1,2],"delay_ms":800}`, 3))
	retry := enqueue(t, s, "retry", request(`{"name":"retry","items":[10,20],"fail_until_attempt":2}`, 3))
	dead := enqueue(t, s, "dead", request(`{"name":"dead","items":[1],"fail_until_attempt":10}`, 3))
	w := worker.Worker{Queue: s, Handle: worker.ReportHandler(s), Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Lease: 300 * time.Millisecond, Poll: 10 * time.Millisecond, RetryBase: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond, Timeout: 3 * time.Second}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx, 4) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		a, err := s.Get(ctx, slow.ID)
		if err != nil {
			t.Fatal(err)
		}
		b, err := s.Get(ctx, retry.ID)
		if err != nil {
			t.Fatal(err)
		}
		c, err := s.Get(ctx, dead.ID)
		if err != nil {
			t.Fatal(err)
		}
		if a.State == "succeeded" && b.State == "succeeded" && c.State == "dead" {
			if a.Attempts != 1 || b.Attempts != 3 || c.Attempts != 3 {
				t.Fatalf("attempts: slow=%d retry=%d dead=%d", a.Attempts, b.Attempts, c.Attempts)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("workers did not reach terminal states")
}
